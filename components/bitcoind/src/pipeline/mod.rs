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
    pipeline::{
        rpc::{
            download_and_parse_block_with_retry, pipeline::start_block_download_pipeline,
            retrieve_best_chain_hash_with_retry, retrieve_block_hash_with_retry,
            retrieve_block_header_with_retry, standardize_bitcoin_block,
        },
        zmq::start_zeromq_pipeline,
    },
    reconcile::{
        check_parent_walk_bounds, is_off_best_chain, is_unknown_hash, needs_best_chain_check,
    },
    try_debug, try_info,
    types::{BitcoinBlockData, BitcoinNetwork, BlockBytesCursor, BlockIdentifier, BlockchainEvent},
    utils::{AbstractBlock, BlockHeights, Context},
    Indexer, IndexerCommand,
};

pub mod rpc;
pub mod zmq;

#[cfg(test)]
mod processor_tests;

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

/// A block fetched from bitcoind, standardized, with its compacted bytes when the indexer keeps a
/// blocks DB (ordinals).
pub(crate) struct FetchedBlock {
    pub block: BitcoinBlockData,
    pub compacted: Option<Vec<u8>>,
}

/// What the block processor needs from bitcoind (INFRA-685). Production talks RPC; tests use maps,
/// so they drive the same loop the indexer runs.
pub(crate) trait ChainSource {
    /// Downloads block `hash` (bare hex).
    async fn fetch_block(&self, hash: &str) -> Result<FetchedBlock, String>;
    /// Hash of bitcoind's best chain at `height`, or `None` when the height is above its tip.
    async fn best_chain_hash(&self, height: u64) -> Result<Option<String>, String>;
}

/// Where the block processor sends indexer commands.
pub(crate) trait IndexerSink {
    fn send(&self, command: IndexerCommand) -> Result<(), String>;
}

struct RpcChainSource<'a> {
    http_client: &'a Client,
    network: BitcoinNetwork,
    compress_blocks: bool,
    config: &'a Config,
    ctx: &'a Context,
}

impl ChainSource for RpcChainSource<'_> {
    async fn fetch_block(&self, hash: &str) -> Result<FetchedBlock, String> {
        let raw = download_and_parse_block_with_retry(
            self.http_client,
            hash,
            &self.config.bitcoind,
            self.ctx,
        )
        .await?;
        let compacted = if self.compress_blocks {
            Some(
                BlockBytesCursor::from_full_block(&raw)
                    .map_err(|e| format!("unable to compress block {hash}: {e}"))?,
            )
        } else {
            None
        };
        let block = standardize_bitcoin_block(raw, &self.network, self.ctx).map_err(|(e, _)| e)?;
        Ok(FetchedBlock { block, compacted })
    }

    async fn best_chain_hash(&self, height: u64) -> Result<Option<String>, String> {
        retrieve_best_chain_hash_with_retry(
            self.http_client,
            height,
            &self.config.bitcoind,
            self.ctx,
        )
        .await
    }
}

struct ChannelSink<'a> {
    tx: &'a Sender<IndexerCommand>,
    abort_signal: &'a Arc<AtomicBool>,
    config: &'a Config,
    ctx: &'a Context,
}

impl IndexerSink for ChannelSink<'_> {
    fn send(&self, command: IndexerCommand) -> Result<(), String> {
        send_indexer_command(self.tx, command, self.abort_signal, self.config, self.ctx)
    }
}

/// The block processor's state and settings, shared by every message it handles.
pub(crate) struct BlockProcessorState<'a, S: ChainSource, K: IndexerSink> {
    pub block_pool: &'a Arc<Mutex<BlockPool>>,
    pub block_store: &'a Arc<Mutex<HashMap<BlockIdentifier, BitcoinBlockData>>>,
    pub source: &'a S,
    pub sink: &'a K,
    /// The indexer keeps a blocks DB (ordinals): every applied block must end up stored in it.
    pub compress_blocks: bool,
    pub reorg_window: u64,
    pub ctx: &'a Context,
}

/// Handles one `ProcessBlocks` message: drops stale fork blocks, stores the compacted blocks that
/// remain, then advances the block pool with each standardized block.
pub(crate) async fn process_blocks_message<S: ChainSource, K: IndexerSink>(
    compacted_blocks: Vec<(u64, Vec<u8>)>,
    blocks: Vec<BitcoinBlockData>,
    state: &BlockProcessorState<'_, S, K>,
) -> Result<(), String> {
    // INFRA-685: decide about stale fork blocks BEFORE storing compacted blocks, so a dropped
    // block's bytes never overwrite the canonical block at its height in the blocks DB.
    let pool_tip = state
        .block_pool
        .lock()
        .unwrap()
        .canonical_chain_tip()
        .cloned();
    let mut dropped_heights = vec![];
    let mut kept = Vec::with_capacity(blocks.len());
    for block in blocks.into_iter() {
        let id = &block.block_identifier;
        if needs_best_chain_check(id, pool_tip.as_ref()) {
            let node_hash = state.source.best_chain_hash(id.index).await?;
            if is_off_best_chain(id, node_hash.as_deref()) {
                try_info!(
                    state.ctx,
                    "Ignoring stale fork block {} at or below the tip {}: bitcoind's best chain has {} at that height",
                    id,
                    pool_tip.as_ref().map_or_else(String::new, |t| t.to_string()),
                    node_hash.as_deref().unwrap_or("no block")
                );
                dropped_heights.push(id.index);
                continue;
            }
        }
        kept.push(block);
    }
    let compacted_blocks: Vec<(u64, Vec<u8>)> = compacted_blocks
        .into_iter()
        .filter(|(height, _)| !dropped_heights.contains(height))
        .collect();
    if !compacted_blocks.is_empty() {
        state
            .sink
            .send(IndexerCommand::StoreCompactedBlocks(compacted_blocks))?;
    }
    for block in kept.into_iter() {
        advance_block_pool(block, state).await?;
    }
    Ok(())
}

/// Moves our block pool with a newly received standardized block. Blocks the pool cannot connect
/// are walked parent by parent (bounded by INFRA-389) until they do.
pub(crate) async fn advance_block_pool<S: ChainSource, K: IndexerSink>(
    block: BitcoinBlockData,
    state: &BlockProcessorState<'_, S, K>,
) -> Result<(), String> {
    let ctx = state.ctx;
    let original = block.block_identifier.clone();
    let mut block_ids = VecDeque::new();
    block_ids.push_front(original.clone());
    let mut parent_walk_depth: u64 = 0;
    // Compacted bytes of the parents downloaded by the walk, for the blocks DB (INFRA-685).
    let mut walked_compacted: HashMap<BlockIdentifier, Vec<u8>> = HashMap::new();

    {
        let pool = state.block_pool.lock().unwrap();
        if pool.contains_header(&original) {
            try_info!(
                ctx,
                "Block {original} already processed, ignoring the repeat"
            );
            return Ok(());
        }
    }
    // Keep incoming block before sending.
    state
        .block_store
        .lock()
        .unwrap()
        .insert(original.clone(), block);

    while let Some(block_id) = block_ids.pop_front() {
        let header = {
            let store = state.block_store.lock().unwrap();
            store
                .get(&block_id)
                .ok_or_else(|| format!("block {block_id} missing from the block store"))?
                .get_header()
        };
        // `None`: the pool cannot connect the header. `Some(None)`: recorded, canonical chain
        // unchanged. `Some(Some(event))`: the canonical chain moved.
        let outcome = {
            let mut pool = state.block_pool.lock().unwrap();
            if pool.can_process_header(&header) {
                Some(pool.process_header(header.clone(), ctx)?)
            } else {
                None
            }
        };
        match outcome {
            Some(Some(event)) => {
                apply_chain_event(event, &original, &mut walked_compacted, state).await?;
            }
            Some(None) if block_id != original => {
                // INFRA-685: a parent fetched by the walk lands on a side branch that is not
                // longer yet; its child, processed next, switches the chain. This used to be a
                // fatal "Unable to append block", so every reorg deeper than one block exited.
                // The block stays in the store: the reorg that follows applies it from there.
                try_debug!(
                    ctx,
                    "Block {} recorded on a side branch, canonical chain unchanged",
                    header.block_identifier
                );
            }
            Some(None) => {
                // The announced block itself did not move the chain although bitcoind follows
                // it (a stale block was dropped before getting here): bitcoind switched to a
                // branch the pool does not rank above ours, e.g. a shorter one with more work.
                // Exit so the restart reconciles the index against bitcoind (INFRA-389).
                state.block_store.lock().unwrap().remove(&block_id);
                return Err(format!(
                    "Unable to append block {block_id}: bitcoind follows it but the block pool does not rank its branch above the current tip"
                ));
            }
            None => {
                try_info!(
                    ctx,
                    "Received non-canonical block {}",
                    header.block_identifier
                );
                // INFRA-389: bound the parent walk. A pool that knows no ancestor of this block
                // can never connect it, and every parent held in memory is a full block.
                parent_walk_depth += 1;
                {
                    let store_len = state.block_store.lock().unwrap().len();
                    check_parent_walk_bounds(parent_walk_depth, store_len, state.reorg_window)?;
                }
                // ZMQ announces only the new tip of a reorg:
                // A (1) - B1 (2) - C1 (3)
                //       \ B2 (4) - C2 (5) - D2 (6)
                // D2 arrives alone; its parents are fetched until one connects.
                let parent_hash = header
                    .parent_block_identifier
                    .get_hash_bytes_str()
                    .to_string();
                let fetched = state.source.fetch_block(&parent_hash).await?;
                let parent_id = fetched.block.block_identifier.clone();
                if let Some(bytes) = fetched.compacted {
                    walked_compacted.insert(parent_id.clone(), bytes);
                }
                state
                    .block_store
                    .lock()
                    .unwrap()
                    .insert(parent_id.clone(), fetched.block);
                block_ids.push_front(block_id);
                block_ids.push_front(parent_id);
            }
        }
    }
    Ok(())
}

/// Takes the blocks of `ids` out of the block store, downloading any that is missing (a block
/// applied, rolled back and applied again, A->B->A, was already removed: INFRA-685), together
/// with their compacted bytes when `with_compacted` holds (from the walk, or downloaded).
async fn collect_blocks<S: ChainSource, K: IndexerSink>(
    ids: &[BlockIdentifier],
    with_compacted: impl Fn(&BlockIdentifier) -> bool,
    walked_compacted: &mut HashMap<BlockIdentifier, Vec<u8>>,
    state: &BlockProcessorState<'_, S, K>,
) -> Result<(Vec<BitcoinBlockData>, Vec<(u64, Vec<u8>)>), String> {
    let mut blocks = Vec::with_capacity(ids.len());
    let mut compacted = vec![];
    for id in ids.iter() {
        let stored = state.block_store.lock().unwrap().remove(id);
        let needs_bytes = with_compacted(id);
        let walked_bytes = if needs_bytes {
            walked_compacted.remove(id)
        } else {
            None
        };
        let (block, bytes) = match (stored, walked_bytes) {
            (Some(block), Some(bytes)) => (block, Some(bytes)),
            (Some(block), None) if !needs_bytes => (block, None),
            (stored, _) => {
                let fetched = state.source.fetch_block(id.get_hash_bytes_str()).await?;
                if fetched.block.block_identifier != *id {
                    return Err(format!(
                        "bitcoind returned {} for {}",
                        fetched.block.block_identifier, id
                    ));
                }
                (stored.unwrap_or(fetched.block), fetched.compacted)
            }
        };
        if needs_bytes {
            let bytes = bytes.ok_or_else(|| format!("no compacted bytes for block {id}"))?;
            compacted.push((id.index, bytes));
        }
        blocks.push(block);
    }
    Ok((blocks, compacted))
}

/// Sends the indexer what a chain event asks for.
///
/// INFRA-685, for indexers with a blocks DB (ordinals): every applied block must be in the blocks
/// DB before it is indexed. The announced block's bytes were stored by `process_blocks_message`,
/// but parents fetched by the walk were not, and a reorg's rollback deletes the blocks DB entry at
/// each rolled-back height, which is the same height as an applied block. So a reorg goes out as:
/// rollback only, then the compacted bytes of every applied block, then the apply. Without that,
/// heights stayed missing and a later sat traversal failed with "block #N not in database".
async fn apply_chain_event<S: ChainSource, K: IndexerSink>(
    event: BlockchainEvent,
    original: &BlockIdentifier,
    walked_compacted: &mut HashMap<BlockIdentifier, Vec<u8>>,
    state: &BlockProcessorState<'_, S, K>,
) -> Result<(), String> {
    let compress = state.compress_blocks;
    match event {
        BlockchainEvent::BlockchainUpdatedWithHeaders(event) => {
            let ids: Vec<BlockIdentifier> = event
                .new_headers
                .iter()
                .map(|h| h.block_identifier.clone())
                .collect();
            // The announced block's bytes are already stored; any other applied block came from
            // the walk (or an earlier side branch).
            let (apply_blocks, compacted) = collect_blocks(
                &ids,
                |id| compress && id != original,
                walked_compacted,
                state,
            )
            .await?;
            if !compacted.is_empty() {
                state
                    .sink
                    .send(IndexerCommand::StoreCompactedBlocks(compacted))?;
            }
            state.sink.send(IndexerCommand::IndexBlocks {
                apply_blocks,
                rollback_block_ids: vec![],
            })
        }
        BlockchainEvent::BlockchainUpdatedWithReorg(event) => {
            let ids: Vec<BlockIdentifier> = event
                .headers_to_apply
                .iter()
                .map(|h| h.block_identifier.clone())
                .collect();
            let rollback_block_ids: Vec<BlockIdentifier> = event
                .headers_to_rollback
                .iter()
                .map(|h| h.block_identifier.clone())
                .collect();
            let (apply_blocks, compacted) =
                collect_blocks(&ids, |_| compress, walked_compacted, state).await?;
            if !compress {
                return state.sink.send(IndexerCommand::IndexBlocks {
                    apply_blocks,
                    rollback_block_ids,
                });
            }
            state.sink.send(IndexerCommand::IndexBlocks {
                apply_blocks: vec![],
                rollback_block_ids,
            })?;
            state
                .sink
                .send(IndexerCommand::StoreCompactedBlocks(compacted))?;
            state.sink.send(IndexerCommand::IndexBlocks {
                apply_blocks,
                rollback_block_ids: vec![],
            })
        }
    }
}

/// Initialize our block pool with the current index's last seen blocks, so we can detect any
/// re-orgs or gaps that may come our way with the next blocks.
async fn initialize_block_pool(
    block_pool: &Arc<Mutex<BlockPool>>,
    index_chain_tip: &BlockIdentifier,
    http_client: &Client,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // INFRA-389: prime with the last headers ending at the tip, not just the tip, so that a reorg
    // found after a restart still connects to a known ancestor. INFRA-685: as many as the pool
    // keeps (the reorg window), fetched as headers only.
    let primed_count = block_pool.lock().unwrap().retained_headers();
    let tip_hash = if is_unknown_hash(&index_chain_tip.hash) {
        // The blocks DB does not keep hashes: ask the node for the canonical one.
        retrieve_block_hash_with_retry(http_client, &index_chain_tip.index, &config.bitcoind, ctx)
            .await?
    } else {
        index_chain_tip.get_hash_bytes_str().to_string()
    };
    let mut headers = Vec::with_capacity(primed_count);
    let mut next_hash = tip_hash;
    for _ in 0..primed_count {
        let header =
            retrieve_block_header_with_retry(http_client, &next_hash, &config.bitcoind, ctx)
                .await?;
        let at_genesis = header.block_identifier.index == 0;
        next_hash = header
            .parent_block_identifier
            .get_hash_bytes_str()
            .to_string();
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
        "Primed fork processor with the last {primed_count} blocks up to {index_chain_tip}"
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
    compress_blocks: bool,
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
    let source = RpcChainSource {
        http_client,
        network: BitcoinNetwork::from_network(config.bitcoind.network),
        compress_blocks,
        config,
        ctx,
    };
    let sink = ChannelSink {
        tx: indexer_commands_tx,
        abort_signal,
        config,
        ctx,
    };
    let state = BlockProcessorState {
        block_pool,
        block_store,
        source: &source,
        sink: &sink,
        compress_blocks,
        reorg_window: config.resources.reorg_window,
        ctx,
    };

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
                sink.send(IndexerCommand::RecordFailedBlock {
                    block_height,
                    error_kind,
                    error_message,
                })?;
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
        if abort_signal.load(Ordering::SeqCst) {
            return Ok(());
        }
        process_blocks_message(compacted_blocks, blocks, &state).await?;
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
        assert_eq!(
            first_block_to_download(Some(&tip(840_010)), 840_000),
            840_011
        );
    }
}
