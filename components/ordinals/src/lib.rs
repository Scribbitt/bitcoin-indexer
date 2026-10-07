use core::{
    first_inscription_height,
    meta_protocols::brc20::cache::brc20_new_cache,
    new_traversals_lazy_cache,
    pipeline::processors::{
        block_archiving::store_compacted_blocks,
        inscription_indexing::{process_blocks, rollback_block},
    },
    protocol::sequence_cursor::SequenceCursor,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use bitcoind::{
    reconcile::check_persisted_tip, start_bitcoin_indexer, try_debug, try_error, try_info, try_warn,
    types::BlockIdentifier,
    utils::{future_block_on, Context},
    Indexer, IndexerCommand,
};
use config::Config;
use db::{
    blocks::{self, find_last_block_inserted, open_blocks_db_with_retry},
    migrate_dbs, ordinals_pg,
};
use deadpool_postgres::Pool;
use postgres::{pg_pool, pg_pool_client};
use utils::monitoring::{start_serving_prometheus_metrics, PrometheusMonitoring};

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate lazy_static;

extern crate serde;

pub mod core;
pub mod db;
pub mod utils;

#[derive(Debug, Clone)]
pub struct PgConnectionPools {
    pub ordinals: Pool,
    pub brc20: Option<Pool>,
}

fn pg_pools(config: &Config) -> PgConnectionPools {
    PgConnectionPools {
        ordinals: pg_pool(&config.ordinals.as_ref().unwrap().db).unwrap(),
        brc20: config
            .ordinals_brc20_config()
            .map(|brc20| pg_pool(&brc20.db).unwrap()),
    }
}

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// Lowest height in `0..=min(last_insert, complete_below)` that has no entry in the blocks DB, or
/// `None` if that whole range is present. Heights above `complete_below` are not required to be
/// contiguous: only the ancestors of the first inscription must exist.
fn lowest_missing_block_height(
    blocks_db: &rocksdb::DB,
    last_insert: u64,
    complete_below: u64,
) -> Option<u64> {
    // An empty DB (no last_insert) is handled by the caller as "start from zero".
    if last_insert == 0 {
        return None;
    }
    let upper = last_insert.min(complete_below);
    (0..=upper).find(|height| {
        !matches!(
            blocks_db.get_pinned((*height as u32).to_be_bytes()),
            Ok(Some(_))
        )
    })
}

async fn new_ordinals_indexer_runloop(
    prometheus: &PrometheusMonitoring,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<Indexer, String> {
    // Initialize AMQP connection for block event publishing
    if let Some(ref amqp) = config.amqp {
        if amqp.enabled {
            match core::pipeline::amqp::init(&amqp.url, &amqp.exchange).await {
                Ok(()) => try_info!(ctx, "AMQP connected to {}", amqp.url),
                Err(e) => try_warn!(ctx, "AMQP init failed (non-fatal): {e}"),
            }
        }
    }

    let (commands_tx, commands_rx) =
        crossbeam_channel::bounded(config.resources.indexer_channel_capacity);
    let pg_pools = pg_pools(config);

    // INFRA-389: reconcile the persisted tip against bitcoind before the indexer thread starts. A
    // tip persisted on an orphaned branch is rolled back by a fixed window (Postgres keeps only the
    // tip hash), and the canonical blocks inside the window are simply re-indexed.
    {
        let pg_tip = {
            let ord_client = pg_pool_client(&pg_pools.ordinals).await?;
            db::ordinals_pg::get_chain_tip(&ord_client).await?
        };
        if let Some(tip) = pg_tip {
            if let Some(plan) =
                check_persisted_tip(&tip, config.resources.reorg_window, &config.bitcoind, ctx)?
            {
                let blocks_db_rw = open_blocks_db_with_retry(true, config, ctx);
                blocks::delete_blocks_in_block_range(
                    plan.rollback_start as u32,
                    plan.rollback_end as u32,
                    &blocks_db_rw,
                    ctx,
                );
                // Newest first, as a reorg rollback would.
                for block_height in (plan.rollback_start..=plan.rollback_end).rev() {
                    rollback_block(block_height, config, &pg_pools, ctx).await?;
                }
                blocks_db_rw
                    .flush()
                    .map_err(|e| format!("error dropping rollback blocks from rocksdb: {e}"))?;
            }
        }
    }

    let config_moved = config.clone();
    let ctx_moved = ctx.clone();
    let pg_pools_moved = pg_pools.clone();
    let prometheus_moved = prometheus.clone();
    let abort_signal_moved = abort_signal.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("ordinals_indexer")
        .spawn(move || {
            future_block_on(&ctx_moved.clone(), async move {
                let cache_l2 = Arc::new(new_traversals_lazy_cache(2048));
                let garbage_collect_every_n_blocks = 100;
                let mut garbage_collect_nth_block = 0;

                let mut sequence_cursor = SequenceCursor::new();
                let mut brc20_cache: Option<core::meta_protocols::brc20::cache::Brc20MemoryCache> =
                    brc20_new_cache(&config_moved);
                loop {
                    if abort_signal_moved.load(Ordering::SeqCst) {
                        break;
                    }
                    match commands_rx.recv() {
                        Ok(command) => match command {
                            IndexerCommand::StoreCompactedBlocks(blocks) => {
                                let blocks_db_rw =
                                    open_blocks_db_with_retry(true, &config_moved, &ctx_moved);
                                store_compacted_blocks(blocks, true, &blocks_db_rw, &ctx_moved);
                            }
                            IndexerCommand::IndexBlocks {
                                mut apply_blocks,
                                rollback_block_ids,
                            } => {
                                if !rollback_block_ids.is_empty() {
                                    let blocks_db_rw =
                                        open_blocks_db_with_retry(true, &config_moved, &ctx_moved);
                                    for block_id in rollback_block_ids.iter() {
                                        blocks::delete_blocks_in_block_range(
                                            block_id.index as u32,
                                            block_id.index as u32,
                                            &blocks_db_rw,
                                            &ctx_moved,
                                        );
                                        rollback_block(
                                            block_id.index,
                                            &config_moved,
                                            &pg_pools_moved,
                                            &ctx_moved,
                                        )
                                        .await?;
                                    }
                                    blocks_db_rw.flush().map_err(|e| {
                                        format!("error dropping rollback blocks from rocksdb: {e}")
                                    })?;
                                }

                                let blocks = match process_blocks(
                                    &mut apply_blocks,
                                    &mut sequence_cursor,
                                    &cache_l2,
                                    &mut brc20_cache,
                                    &prometheus_moved,
                                    &config_moved,
                                    &pg_pools_moved,
                                    &ctx_moved,
                                    &abort_signal_moved,
                                )
                                .await
                                {
                                    Ok(blocks) => blocks,
                                    Err(e) => return Err(format!("error indexing blocks: {e}")),
                                };

                                garbage_collect_nth_block += blocks.len();
                                if garbage_collect_nth_block > garbage_collect_every_n_blocks {
                                    try_debug!(
                                        ctx_moved,
                                        "Clearing cache L2 ({} entries)",
                                        cache_l2.len()
                                    );
                                    cache_l2.clear();
                                    garbage_collect_nth_block = 0;
                                }
                            }
                            IndexerCommand::RecordFailedBlock {
                                block_height,
                                error_kind,
                                error_message,
                            } => {
                                // SKRYBITDEV-586: persist the skipped block to the
                                // `failed_blocks` table so it can be retried manually
                                // later via `bitcoin-indexer ordinals retry-failed`.
                                let ord_client = pg_pool_client(&pg_pools_moved.ordinals).await?;
                                if let Err(e) = db::ordinals_pg::record_failed_block(
                                    &ord_client,
                                    block_height,
                                    &error_kind,
                                    &error_message,
                                )
                                .await
                                {
                                    try_error!(
                                        ctx_moved,
                                        "Unable to persist failed block #{block_height}: {e}"
                                    );
                                }

                                // SKRYBITDEV-586: bump per-kind error counter.
                                match error_kind.as_str() {
                                    "parse" => prometheus_moved.block_parse_errors_total.inc(),
                                    "compress" => {
                                        prometheus_moved.block_compress_errors_total.inc()
                                    }
                                    "standardize" => {
                                        prometheus_moved.block_standardize_errors_total.inc()
                                    }
                                    "download" => {
                                        prometheus_moved.block_download_errors_total.inc()
                                    }
                                    _ => {}
                                }

                                // SKRYBITDEV-586: refresh pending count gauge.
                                if let Ok(count) =
                                    db::ordinals_pg::count_unresolved_failed_blocks(&ord_client)
                                        .await
                                {
                                    prometheus_moved.failed_blocks_pending.set(count as u64);
                                }
                            }
                            IndexerCommand::Terminate => {
                                break;
                            }
                        },
                        Err(e) => return Err(format!("ordinals indexer channel error: {e}")),
                    }
                }
                try_info!(ctx_moved, "OrdinalsIndexer thread complete");
                Ok(())
            });
        })
        .expect("unable to spawn thread");

    let pg_chain_tip = {
        let ord_client = pg_pool_client(&pg_pools.ordinals).await?;
        db::ordinals_pg::get_chain_tip(&ord_client).await?
    };
    let blocks_chain_tip = {
        // Use readwrite mode to create the rocksdb if it doesn't exist (first run).
        let blocks_db = open_blocks_db_with_retry(true, config, ctx);
        let height = find_last_block_inserted(&blocks_db);
        // INFRA-385: `last_insert` is not a contiguous tip. Satoshi numbering walks every ancestor
        // block, so the blocks DB must hold every height from 0 up to the first inscription. If
        // anything below is missing, resume the download from the lowest missing height (block 0
        // for a DB that was only ever filled from the first inscription onward).
        let complete_below = first_inscription_height(config).saturating_sub(1);
        match lowest_missing_block_height(&blocks_db, height as u64, complete_below) {
            Some(missing) => {
                try_info!(
                    ctx,
                    "blocks DB incomplete (last insert #{height}), will backfill from #{missing}"
                );
                if missing == 0 {
                    None
                } else {
                    Some(BlockIdentifier {
                        index: missing - 1,
                        hash: ZERO_HASH.into(),
                    })
                }
            }
            // Blocks DB does not have the hash available.
            None if height > 0 => Some(BlockIdentifier {
                index: height as u64,
                hash: ZERO_HASH.into(),
            }),
            None => None,
        }
    };
    let chain_tip = match (pg_chain_tip, blocks_chain_tip) {
        // Index chain tip is the minimum of postgres DB tip vs blocks DB tip.
        (Some(x), Some(y)) => Some(if x.index <= y.index { x } else { y }),
        // No blocks DB means start from zero so we can pull them.
        (Some(_), None) => None,
        // No postgres DB means we might be using an archived blocks DB, make sure we index from the first inscription chain tip.
        (None, Some(y)) => {
            let x = BlockIdentifier {
                index: first_inscription_height(config) - 1,
                hash: "0x0000000000000000000000000000000000000000000000000000000000000000".into(),
            };
            Some(if x.index <= y.index { x } else { y })
        }
        // Start from zero.
        (None, None) => None,
    };
    Ok(Indexer {
        commands_tx,
        chain_tip,
        thread_handle: Some(handle),
    })
}

pub async fn get_chain_tip(config: &Config) -> Result<BlockIdentifier, String> {
    let pool = pg_pool(&config.ordinals.as_ref().unwrap().db)?;
    let ord_client = pg_pool_client(&pool).await?;
    // Empty DB on first boot returns None — fall back to a zero-tip so
    // start_ordinals_indexer can initialise from scratch instead of panicking.
    Ok(db::ordinals_pg::get_chain_tip(&ord_client)
        .await?
        .unwrap_or_else(|| BlockIdentifier {
            index: 0,
            hash: "0x0000000000000000000000000000000000000000000000000000000000000000".into(),
        }))
}

/// SKRYBITDEV-586: Public wrapper around
/// [`db::ordinals_pg::list_unresolved_failed_blocks`] for CLI consumers.
pub async fn list_unresolved_failed_blocks(
    config: &Config,
) -> Result<Vec<db::ordinals_pg::FailedBlock>, String> {
    let pools = pg_pools(config);
    let client = pg_pool_client(&pools.ordinals).await?;
    db::ordinals_pg::list_unresolved_failed_blocks(&client).await
}

pub async fn rollback_block_range(
    start_block: u64,
    end_block: u64,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let blocks_db_rw = open_blocks_db_with_retry(true, config, ctx);
    let pg_pools = pg_pools(config);
    blocks::delete_blocks_in_block_range(start_block as u32, end_block as u32, &blocks_db_rw, ctx);
    for block in start_block..=end_block {
        rollback_block(block, config, &pg_pools, ctx).await?;
    }
    blocks_db_rw
        .flush()
        .map_err(|e| format!("error dropping rollback blocks from rocksdb: {e}"))
}

/// Starts the ordinals indexing process. Will block the main thread indefinitely until explicitly stopped or it reaches chain tip
/// and `stream_blocks_at_chain_tip` is set to false.
pub async fn start_ordinals_indexer(
    stream_blocks_at_chain_tip: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    migrate_dbs(config, ctx).await?;
    let prometheus = PrometheusMonitoring::new();
    let pg_pools = pg_pools(config);

    // Initialize metrics with current state
    let max_inscription_number = {
        let ord_client = pg_pool_client(&pg_pools.ordinals).await?;
        ordinals_pg::get_highest_inscription_number(&ord_client)
            .await?
            .unwrap_or(0) as u64
    };
    let chain_tip = get_chain_tip(config).await?;
    prometheus
        .initialize(max_inscription_number, chain_tip.index, &pg_pools)
        .await?;

    let mut indexer = new_ordinals_indexer_runloop(&prometheus, abort_signal, config, ctx).await?;

    if let Some(metrics) = &config.metrics {
        if metrics.enabled {
            let registry_moved = prometheus.registry.clone();
            let ctx_cloned = ctx.clone();
            let port = metrics.prometheus_port;
            let abort_signal_cloned = abort_signal.clone();
            let _ = std::thread::spawn(move || {
                hiro_system_kit::nestable_block_on(start_serving_prometheus_metrics(
                    port,
                    registry_moved,
                    ctx_cloned,
                    abort_signal_cloned,
                ));
            });
        }
    }

    // Download from block 0 but sequence from the first inscription: satoshi numbering walks every
    // ancestor block, so they must all be in the blocks DB (INFRA-385).
    start_bitcoin_indexer(
        &mut indexer,
        0,
        first_inscription_height(config),
        stream_blocks_at_chain_tip,
        true,
        abort_signal,
        config,
        ctx,
    )
    .await
}
