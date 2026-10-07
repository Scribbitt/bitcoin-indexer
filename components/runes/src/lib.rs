use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use bitcoind::{
    reconcile::check_persisted_tip, start_bitcoin_indexer, try_error, try_info, try_warn,
    types::BlockIdentifier,
    utils::{future_block_on, Context},
    Indexer, IndexerCommand,
};
use config::Config;
use db::{
    cache::index_cache::IndexCache,
    index::{get_rune_genesis_block_height, index_block, roll_back_block},
};
use deadpool_postgres::Pool;
use postgres::{pg_pool, pg_pool_client};
use utils::monitoring::{start_serving_prometheus_metrics, PrometheusMonitoring};

extern crate serde;

pub mod db;
pub mod utils;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

async fn new_runes_indexer_runloop(
    pg_pool: &Pool,
    prometheus: &PrometheusMonitoring,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<Indexer, String> {
    // INFRA-389: reconcile the persisted tip against bitcoind before the indexer thread (and its
    // caches) start, so a tip left on an orphaned branch is rolled back instead of walked from.
    {
        let mut pg_client = pg_pool_client(pg_pool).await?;
        if let Some(tip) = db::get_chain_tip(&pg_client).await {
            if let Some(plan) =
                check_persisted_tip(&tip, config.resources.reorg_window, &config.bitcoind, ctx)?
            {
                for block_height in (plan.rollback_start..=plan.rollback_end).rev() {
                    roll_back_block(&mut pg_client, block_height, ctx).await;
                }
            }
        }
    }

    let (commands_tx, commands_rx) =
        crossbeam_channel::bounded(config.resources.indexer_channel_capacity);

    let config_moved = config.clone();
    let ctx_moved = ctx.clone();
    let prometheus_moved = prometheus.clone();
    let abort_signal_moved = abort_signal.clone();
    let pg_pool_moved = pg_pool.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("RunesIndexer")
        .spawn(move || {
            future_block_on(&ctx_moved.clone(), async move {
                #[cfg(feature = "dhat-heap")]
                let _profiler = dhat::Profiler::new_heap();

                let mut index_cache =
                    IndexCache::new(&config_moved, &pg_pool_moved, &ctx_moved).await;
                loop {
                    if abort_signal_moved.load(Ordering::SeqCst) {
                        break;
                    }
                    match commands_rx.recv() {
                        Ok(command) => match command {
                            IndexerCommand::StoreCompactedBlocks(_) => {
                                // No-op. The Runes indexer has no need for compacted blocks.
                                try_warn!(
                                    ctx_moved,
                                    "Runes indexer received unexpected StoreCompactedBlocks command"
                                );
                            }
                            IndexerCommand::IndexBlocks {
                                mut apply_blocks,
                                rollback_block_ids,
                            } => {
                                let mut pg_client = pg_pool_client(&pg_pool_moved).await?;
                                for block_id in rollback_block_ids.iter() {
                                    roll_back_block(&mut pg_client, block_id.index, &ctx_moved)
                                        .await;
                                }
                                for block in apply_blocks.iter_mut() {
                                    if abort_signal_moved.load(Ordering::SeqCst) {
                                        break;
                                    }
                                    index_block(
                                        &mut pg_client,
                                        &mut index_cache,
                                        block,
                                        &prometheus_moved,
                                        &ctx_moved,
                                    )
                                    .await?;
                                }
                            }
                            IndexerCommand::RecordFailedBlock {
                                block_height,
                                error_kind,
                                error_message,
                            } => {
                                // SKRYBITDEV-586: persist to `failed_blocks` so the block
                                // can be retried manually via `bitcoin-indexer runes retry-failed`.
                                let pg_client = pg_pool_client(&pg_pool_moved).await?;
                                if let Err(e) = db::record_failed_block(
                                    &pg_client,
                                    block_height,
                                    &error_kind,
                                    &error_message,
                                )
                                .await
                                {
                                    try_error!(
                                        ctx_moved,
                                        "Unable to persist failed rune block #{block_height}: {e}"
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
                                    db::count_unresolved_failed_blocks(&pg_client).await
                                {
                                    prometheus_moved.failed_blocks_pending.set(count as u64);
                                }
                            }
                            IndexerCommand::Terminate => {
                                break;
                            }
                        },
                        Err(error) => {
                            try_error!(
                                ctx_moved,
                                "Runes indexer received invalid command: {}",
                                error
                            );
                            return Err(error.to_string());
                        }
                    }
                }
                try_info!(ctx_moved, "RunesIndexer thread complete");
                Ok(())
            });
        })
        .expect("unable to spawn thread");

    let pg_client = pg_pool_client(pg_pool).await?;
    let chain_tip = db::get_chain_tip(&pg_client).await;
    Ok(Indexer {
        commands_tx,
        chain_tip,
        thread_handle: Some(handle),
    })
}

pub async fn get_chain_tip(config: &Config) -> Result<BlockIdentifier, String> {
    let pool = pg_pool(&config.runes.as_ref().unwrap().db)?;
    let pg_client = pg_pool_client(&pool).await?;
    Ok(db::get_chain_tip(&pg_client).await.unwrap())
}

pub async fn rollback_block_range(
    start_block: u64,
    end_block: u64,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let pool = pg_pool(&config.runes.as_ref().unwrap().db)?;
    let mut pg_client = pg_pool_client(&pool).await?;
    for block_id in start_block..=end_block {
        roll_back_block(&mut pg_client, block_id, ctx).await;
    }
    Ok(())
}

/// SKRYBITDEV-586: Public wrapper around [`db::list_unresolved_failed_blocks`]
/// for CLI consumers.
pub async fn list_unresolved_failed_blocks(
    config: &Config,
) -> Result<Vec<db::FailedBlock>, String> {
    let pool = pg_pool(&config.runes.as_ref().unwrap().db)?;
    let pg_client = pg_pool_client(&pool).await?;
    db::list_unresolved_failed_blocks(&pg_client).await
}

/// Starts the runes indexing process. Will block the main thread indefinitely until explicitly stopped or it reaches chain tip
/// and `stream_blocks_at_chain_tip` is set to false.
pub async fn start_runes_indexer(
    stream_blocks_at_chain_tip: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let pool = pg_pool(&config.runes.as_ref().unwrap().db)?;
    {
        let mut pg_client = pg_pool_client(&pool).await?;
        db::migrate(&mut pg_client, ctx).await;
    }

    let prometheus = PrometheusMonitoring::new();
    let mut indexer =
        new_runes_indexer_runloop(&pool, &prometheus, abort_signal, config, ctx).await?;

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
    // Initialize metrics with current state
    {
        let pg_client = pg_pool_client(&pool).await?;
        let max_rune_number = db::pg_get_max_rune_number(&pg_client).await;
        let genesis = get_rune_genesis_block_height(config.bitcoind.network);
        let chain_tip = db::get_chain_tip(&pg_client)
            .await
            .unwrap_or(BlockIdentifier {
                index: genesis.saturating_sub(1),
                hash: "0x0000000000000000000000000000000000000000000000000000000000000000".into(),
            });
        prometheus
            .initialize(max_rune_number as u64, chain_tip.index)
            .await?;
    }

    // Runes download and sequence from the same height (SKRYBITDEV-301).
    let rune_genesis = get_rune_genesis_block_height(config.bitcoind.network);
    start_bitcoin_indexer(
        &mut indexer,
        rune_genesis,
        rune_genesis,
        stream_blocks_at_chain_tip,
        false,
        abort_signal,
        config,
        ctx,
    )
    .await
}
