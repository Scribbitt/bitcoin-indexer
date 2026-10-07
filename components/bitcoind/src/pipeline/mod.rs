use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

use config::Config;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use reqwest::Client;

use crate::{
    block_pool::BlockPool,
    reconcile::{check_parent_walk_bounds, is_unknown_hash},
    pipeline::{
        rpc::{
            download_and_parse_block_with_retry, pipeline::start_block_download_pipeline,
            retrieve_block_hash_with_retry,
            standardize_bitcoin_block,
        },
        zmq::start_zeromq_pipeline,
    },
    try_debug, try_info,
    types::{BitcoinBlockData, BitcoinNetwork, BlockIdentifier, BlockchainEvent},
    utils::{AbstractBlock, BlockHeights, Context},
    Indexer, IndexerCommand,
};

pub mod rpc;
pub mod zmq;

/// How many canonical headers, ending at the index tip, the block pool is primed with at startup.
const PRIMED_HEADERS_COUNT: usize = 7;

/// Commands that can be sent to the block processor.
pub(crate) enum BlockProcessorCommand {
    ProcessBlocks {
        compacted_blocks: Vec<(u64, Vec<u8>)>,
        blocks: Vec<BitcoinBlockData>,
    },
    /// A block was skipped by the pipeline due to a parse/compress/standardize/download
    /// failure. The processor forwards this to the indexer which persists it to the
    /// `failed_blocks` table. See SKRYBITDEV-586.
    RecordFailed {
        block_height: u64,
        error_kind: String,
        error_message: String,
    },
    /// Barrier used by the block download pipeline to wait until every command
    /// enqueued before it has been processed. The processor acks and keeps
    /// running — it must stay alive for follow-up catch-up rounds and the ZMQ
    /// live-follow after catch-up. See INFRA-181.
    Flush {
        ack_tx: crossbeam_channel::Sender<()>,
    },
    Terminate,
}

/// Object that will receive any blocks as they come from bitcoind. These messages do not track any canonical chain alterations.
pub(crate) struct BlockProcessor {
    /// Sender for emitting block processor commands.
    pub commands_tx: crossbeam_channel::Sender<BlockProcessorCommand>,
    /// Handle for the block processor thread.
    pub thread_handle: Option<JoinHandle<()>>,
}

/// Helper function to send indexer commands with fullness logging.
fn send_indexer_command(
    tx: &crossbeam_channel::Sender<IndexerCommand>,
    mut cmd: IndexerCommand,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let mut logged = false;
    loop {
        match tx.try_send(cmd) {
            Ok(()) => break,
            Err(TrySendError::Full(returned_cmd)) => {
                cmd = returned_cmd;
                if !logged {
                    try_debug!(
                        ctx,
                        "Indexer command channel full, waiting for space (capacity: {})",
                        config.resources.indexer_channel_capacity
                    );
                    logged = true;
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Err(TrySendError::Disconnected(_)) => {
                if abort_signal.load(Ordering::SeqCst) {
                    return Ok(());
                }
                return Err("Indexer command channel disconnected".into());
            }
        }
    }
    Ok(())
}

/// Joins a thread handle and returns an error if the thread panics.
pub(crate) fn wait_for_thread_finish(
    thread_handle: &mut Option<JoinHandle<()>>,
) -> Result<(), String> {
    thread_handle
        .take()
        .unwrap()
        .join()
        .map_err(|e| format!("Failed to join thread: {:?}", e))
}

/// Moves our block pool with a newly received standardized block
async fn advance_block_pool(
    block: BitcoinBlockData,
    block_pool: &Arc<Mutex<BlockPool>>,
    block_store: &Arc<Mutex<HashMap<BlockIdentifier, BitcoinBlockData>>>,
    http_client: &Client,
    indexer_commands_tx: &Sender<IndexerCommand>,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    if abort_signal.load(Ordering::SeqCst) {
        return Ok(());
    }
    let network = BitcoinNetwork::from_network(config.bitcoind.network);
    let mut block_ids = VecDeque::new();
    block_ids.push_front(block.block_identifier.clone());
    let mut parent_walk_depth: u64 = 0;

    let block_pool_ref = block_pool.clone();
    let block_store_ref = block_store.clone();

    // Keep incoming block before sending.
    {
        let mut block_store_guard = block_store_ref.lock().unwrap();
        block_store_guard.insert(block.block_identifier.clone(), block);
    }

    while let Some(block_id) = block_ids.pop_front() {
        let (header, canonical) = {
            let mut pool_guard = block_pool_ref.lock().unwrap();
            let mut block_store_guard = block_store_ref.lock().unwrap();
            let block = block_store_guard.get(&block_id).unwrap();
            let header = block.get_header();
            if pool_guard.can_process_header(&header) {
                match pool_guard.process_header(header.clone(), ctx)? {
                    Some(event) => match event {
                        BlockchainEvent::BlockchainUpdatedWithHeaders(event) => {
                            let mut apply_blocks = vec![];
                            for header in event.new_headers.iter() {
                                apply_blocks.push(
                                    block_store_guard.remove(&header.block_identifier).unwrap(),
                                );
                            }
                            send_indexer_command(
                                indexer_commands_tx,
                                IndexerCommand::IndexBlocks {
                                    apply_blocks,
                                    rollback_block_ids: vec![],
                                },
                                abort_signal,
                                config,
                                ctx,
                            )?;
                            (header, true)
                        }
                        BlockchainEvent::BlockchainUpdatedWithReorg(event) => {
                            let mut apply_blocks = vec![];
                            for header in event.headers_to_apply.iter() {
                                apply_blocks.push(
                                    block_store_guard.remove(&header.block_identifier).unwrap(),
                                );
                            }
                            let rollback_block_ids: Vec<BlockIdentifier> = event
                                .headers_to_rollback
                                .iter()
                                .map(|h| h.block_identifier.clone())
                                .collect();
                            send_indexer_command(
                                indexer_commands_tx,
                                IndexerCommand::IndexBlocks {
                                    apply_blocks,
                                    rollback_block_ids,
                                },
                                abort_signal,
                                config,
                                ctx,
                            )?;
                            (header, true)
                        }
                    },
                    None => return Err("Unable to append block".into()),
                }
            } else {
                try_info!(
                    ctx,
                    "Received non-canonical block {}",
                    header.block_identifier
                );
                (header, false)
            }
        };
        if !canonical {
            // INFRA-389: bound the parent walk. A pool that knows no ancestor of this block can
            // never connect it, and every parent held in memory is a full standardized block.
            parent_walk_depth += 1;
            {
                let store_len = block_store_ref.lock().unwrap().len();
                check_parent_walk_bounds(parent_walk_depth, store_len, config.resources.reorg_window)?;
            }
            let parent_block = {
                // Handle a behaviour specific to ZMQ usage in bitcoind.
                // Considering a simple re-org:
                // A (1) - B1 (2) - C1 (3)
                //       \ B2 (4) - C2 (5) - D2 (6)
                // When D2 is being discovered (making A -> B2 -> C2 -> D2 the new canonical fork)
                // it looks like ZMQ is only publishing D2.
                // Without additional operation, we end up with a block that we can't append.
                let parent_block_hash = header
                    .parent_block_identifier
                    .get_hash_bytes_str()
                    .to_string();
                // try_info!(
                //     ctx,
                //     "zmq: Re-org detected, retrieving parent block {parent_block_hash}"
                // );
                let parent_block = download_and_parse_block_with_retry(
                    http_client,
                    &parent_block_hash,
                    &config.bitcoind,
                    ctx,
                )
                .await?;
                standardize_bitcoin_block(parent_block, &network, ctx).map_err(|(e, _)| e)?
            };
            // Keep parent block and repeat the cycle
            {
                let mut block_store_guard = block_store_ref.lock().unwrap();
                block_store_guard
                    .insert(parent_block.block_identifier.clone(), parent_block.clone());
            }
            block_ids.push_front(block_id);
            block_ids.push_front(parent_block.block_identifier.clone());
        }
    }
    Ok(())
}

/// Initialize our block pool with the current index's last seen block, so we can detect any re-orgs or gaps that may come our
/// way with the next blocks.
async fn initialize_block_pool(
    block_pool: &Arc<Mutex<BlockPool>>,
    index_chain_tip: &BlockIdentifier,
    http_client: &Client,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // INFRA-389: prime with the last N headers ending at the tip, not just the tip, so that an
    // ordinary short reorg found after a restart still connects to a known ancestor.
    let tip_hash = if is_unknown_hash(&index_chain_tip.hash) {
        // The blocks DB does not keep hashes: ask the node for the canonical one.
        retrieve_block_hash_with_retry(http_client, &index_chain_tip.index, &config.bitcoind, ctx)
            .await?
    } else {
        index_chain_tip.get_hash_bytes_str().to_string()
    };
    let mut headers = Vec::with_capacity(PRIMED_HEADERS_COUNT);
    let mut next_hash = tip_hash;
    for _ in 0..PRIMED_HEADERS_COUNT {
        let block = download_and_parse_block_with_retry(
            http_client,
            &next_hash,
            &config.bitcoind,
            ctx,
        )
        .await?;
        let header = block.get_block_header();
        let at_genesis = header.block_identifier.index == 0;
        next_hash = header.parent_block_identifier.get_hash_bytes_str().to_string();
        headers.push(header);
        if at_genesis {
            break;
        }
    }
    let block_pool_ref = block_pool.clone();
    let mut pool = block_pool_ref.lock().unwrap();
    // Oldest first, so each header connects to the previous one.
    for header in headers.into_iter().rev() {
        let id = header.block_identifier.clone();
        if let Err(e) = pool.process_header(header, ctx) {
            return Err(format!("Unable to load last seen block {id}: {e}"));
        }
    }
    try_debug!(
        ctx,
        "Primed fork processor with the last {PRIMED_HEADERS_COUNT} blocks up to {index_chain_tip}"
    );
    Ok(())
}

/// Runloop designed to receive Bitcoin blocks through a [BlockProcessor] and send them to a [ForkScratchPad] so it can advance
/// the canonical chain.
pub(crate) async fn block_processor_runloop(
    indexer_commands_tx: &Sender<IndexerCommand>,
    index_chain_tip: &Option<BlockIdentifier>,
    block_commands_rx: &Receiver<BlockProcessorCommand>,
    block_pool: &Arc<Mutex<BlockPool>>,
    block_store: &Arc<Mutex<HashMap<BlockIdentifier, BitcoinBlockData>>>,
    http_client: &Client,
    sequence_start_block_height: u64,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // Before starting the loop, check if the index already has progress. If so, prime the block pool with the current tip.
    if let Some(index_chain_tip) = index_chain_tip {
        if index_chain_tip.index >= sequence_start_block_height {
            initialize_block_pool(block_pool, index_chain_tip, http_client, config, ctx).await?;
        }
    }

    loop {
        if abort_signal.load(Ordering::SeqCst) {
            return Ok(());
        }
        let (compacted_blocks, blocks) = match block_commands_rx.recv() {
            Ok(BlockProcessorCommand::ProcessBlocks {
                compacted_blocks,
                blocks,
            }) => (compacted_blocks, blocks),
            Ok(BlockProcessorCommand::RecordFailed {
                block_height,
                error_kind,
                error_message,
            }) => {
                // SKRYBITDEV-586: forward to indexer which persists to `failed_blocks` table.
                send_indexer_command(
                    indexer_commands_tx,
                    IndexerCommand::RecordFailedBlock {
                        block_height,
                        error_kind,
                        error_message,
                    },
                    abort_signal,
                    config,
                    ctx,
                )?;
                continue;
            }
            Ok(BlockProcessorCommand::Flush { ack_tx }) => {
                // INFRA-181: every command enqueued before this barrier has been
                // processed. Ack and keep looping — do not exit.
                let _ = ack_tx.send(());
                continue;
            }
            Ok(BlockProcessorCommand::Terminate) => {
                try_info!(ctx, "BlockProcessor received Terminate command");
                return Ok(());
            }
            Err(e) => return Err(format!("block ingestion runloop error: {e}")),
        };

        if !compacted_blocks.is_empty() {
            send_indexer_command(
                indexer_commands_tx,
                IndexerCommand::StoreCompactedBlocks(compacted_blocks),
                abort_signal,
                config,
                ctx,
            )?;
        }
        for block in blocks.into_iter() {
            if abort_signal.load(Ordering::SeqCst) {
                return Ok(());
            }
            advance_block_pool(
                block,
                block_pool,
                block_store,
                http_client,
                indexer_commands_tx,
                abort_signal,
                config,
                ctx,
            )
            .await?;
        }
    }
}

/// First block to download. When the index is empty this is `download_start_block_height`, which
/// is deliberately separate from the sequencing start height: runes start both at their activation
/// height (SKRYBITDEV-301, no point scanning older blocks), while ordinals download from 0 and only
/// sequence from the first inscription, because satoshi numbering walks every ancestor block
/// (INFRA-385).
fn first_block_to_download(
    chain_tip: Option<&BlockIdentifier>,
    download_start_block_height: u64,
) -> u64 {
    chain_tip.map_or(download_start_block_height, |ct| ct.index + 1)
}

/// Starts a bitcoind RPC block download pipeline that will send us all historical bitcoin blocks in a parallel fashion. We will
/// then stream these blocks into our block pool so they can be fed into the configured [Indexer]. This will eventually bring the
/// index chain tip to `target_block_height`.
pub(crate) async fn download_rpc_blocks(
    indexer: &Indexer,
    block_processor: &mut BlockProcessor,
    block_pool: &Arc<Mutex<BlockPool>>,
    http_client: &Client,
    target_block_height: u64,
    download_start_block_height: u64,
    sequence_start_block_height: u64,
    compress_blocks: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let blocks = {
        let block_pool_ref = block_pool.clone();
        let pool = block_pool_ref.lock().unwrap();
        let chain_tip = pool.canonical_chain_tip().or(indexer.chain_tip.as_ref());
        let start_block = first_block_to_download(chain_tip, download_start_block_height);
        BlockHeights::BlockRange(start_block, target_block_height)
            .get_sorted_entries()
            .map_err(|_e| "Block start / end block spec invalid".to_string())?
    };
    try_debug!(
        ctx,
        "Downloading blocks from #{} to #{}",
        blocks.front().unwrap(),
        blocks.back().unwrap()
    );
    start_block_download_pipeline(
        config,
        http_client,
        blocks.into(),
        sequence_start_block_height,
        compress_blocks,
        block_processor,
        abort_signal,
        ctx,
    )
    .await
}

/// Streams all upcoming blocks from bitcoind through its ZeroMQ interface and pipes them onto the [Indexer] once processed
/// through our block pool. This process will run indefinitely and will make sure our index keeps advancing as new Bitcoin blocks
/// get mined.
pub(crate) async fn stream_zmq_blocks(
    block_processor: &mut BlockProcessor,
    sequence_start_block_height: u64,
    compress_blocks: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    start_zeromq_pipeline(
        block_processor,
        sequence_start_block_height,
        compress_blocks,
        abort_signal,
        config,
        ctx,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tip(index: u64) -> BlockIdentifier {
        BlockIdentifier {
            index,
            hash: format!("0x{:064x}", index),
        }
    }

    /// Runes pass download_start == sequence_start (SKRYBITDEV-301): an empty index still starts at
    /// the activation height rather than block 0.
    #[test]
    fn runes_empty_index_starts_at_activation_height() {
        assert_eq!(first_block_to_download(None, 840_000), 840_000);
    }

    /// Ordinals pass download_start = 0 (INFRA-385): every ancestor block must be stored, even though
    /// sequencing starts at the first inscription height.
    #[test]
    fn ordinals_empty_index_starts_at_genesis() {
        assert_eq!(first_block_to_download(None, 0), 0);
    }

    #[test]
    fn non_empty_index_resumes_after_tip() {
        assert_eq!(first_block_to_download(Some(&tip(767_429)), 0), 767_430);
        assert_eq!(first_block_to_download(Some(&tip(840_010)), 840_000), 840_011);
    }
}
