//! Startup reconciliation of a persisted index chain tip against bitcoind's canonical chain
//! (INFRA-389).
//!
//! Postgres keeps only the tip hash, so when the tip was persisted on a branch that has since been
//! orphaned the block pool cannot find a common ancestor. Instead of walking towards genesis, the
//! indexer rolls back a fixed window of blocks and re-indexes the canonical ones.

use config::BitcoindConfig;

use crate::{
    try_warn,
    types::BlockIdentifier,
    utils::{bitcoind::bitcoind_get_block_hash, Context},
};

/// What to do with a persisted tip that is not canonical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TipReconciliation {
    /// The persisted (non-canonical) tip.
    pub old_tip: BlockIdentifier,
    /// The canonical block at the old tip's height.
    pub canonical_at_old_tip: BlockIdentifier,
    /// First height to roll back (inclusive).
    pub rollback_start: u64,
    /// Last height to roll back (inclusive), i.e. the old tip's height.
    pub rollback_end: u64,
    /// Canonical block the index tip becomes once the rollback is done.
    pub new_tip: BlockIdentifier,
}

fn strip_0x(hash: &str) -> &str {
    hash.strip_prefix("0x").unwrap_or(hash)
}

fn with_0x(hash: &str) -> String {
    format!("0x{}", strip_0x(hash))
}

/// True if `hash` is the all-zero placeholder used where the real hash is unknown.
pub fn is_unknown_hash(hash: &str) -> bool {
    strip_0x(hash).chars().all(|c| c == '0')
}

/// Decides whether `tip` is still canonical. `canonical_hash` returns the node's block hash at a
/// height (with or without `0x`). Returns `None` when the tip is canonical, otherwise the rollback
/// to apply: everything in `(tip.index - window, tip.index]`.
pub fn plan_tip_reconciliation<F>(
    tip: &BlockIdentifier,
    reorg_window: u64,
    canonical_hash: F,
) -> Result<Option<TipReconciliation>, String>
where
    F: Fn(u64) -> Result<String, String>,
{
    if is_unknown_hash(&tip.hash) {
        // Nothing to compare against (e.g. a tip derived from the blocks DB).
        return Ok(None);
    }
    let canonical_at_tip = canonical_hash(tip.index)?;
    if strip_0x(&canonical_at_tip).eq_ignore_ascii_case(strip_0x(&tip.hash)) {
        return Ok(None);
    }
    let rollback_to = tip.index.saturating_sub(reorg_window.max(1));
    let new_tip_hash = canonical_hash(rollback_to)?;
    Ok(Some(TipReconciliation {
        old_tip: tip.clone(),
        canonical_at_old_tip: BlockIdentifier {
            index: tip.index,
            hash: with_0x(&canonical_at_tip),
        },
        rollback_start: rollback_to + 1,
        rollback_end: tip.index,
        new_tip: BlockIdentifier {
            index: rollback_to,
            hash: with_0x(&new_tip_hash),
        },
    }))
}

/// Checks the persisted tip against bitcoind and, if it is not canonical, logs one WARN and returns
/// the rollback the caller must apply to its own stores.
pub fn check_persisted_tip(
    tip: &BlockIdentifier,
    reorg_window: u64,
    config: &BitcoindConfig,
    ctx: &Context,
) -> Result<Option<TipReconciliation>, String> {
    let plan = plan_tip_reconciliation(tip, reorg_window, |height| {
        bitcoind_get_block_hash(config, ctx, height)
    })?;
    if let Some(plan) = &plan {
        try_warn!(
            ctx,
            "persisted tip {} not canonical (canonical {}); rolling back to {}",
            plan.old_tip,
            plan.canonical_at_old_tip,
            plan.new_tip.index
        );
    }
    Ok(plan)
}

/// Maximum number of unapplied blocks the parent walk may keep in memory, as a multiple of the reorg
/// window.
const BLOCK_STORE_WINDOW_FACTOR: usize = 2;

/// Guards the parent walk in `advance_block_pool`. `depth` is the number of parents downloaded so
/// far for the current block, `store_len` the size of the in-memory block store.
pub fn check_parent_walk_bounds(
    depth: u64,
    store_len: usize,
    reorg_window: u64,
) -> Result<(), String> {
    if depth > reorg_window {
        return Err(format!(
            "reorg deeper than {reorg_window} blocks / no common ancestor"
        ));
    }
    let cap = (reorg_window as usize).saturating_mul(BLOCK_STORE_WINDOW_FACTOR);
    if store_len > cap {
        return Err(format!(
            "block store holds {store_len} unapplied blocks (cap {cap}): reorg deeper than {reorg_window} blocks / no common ancestor"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        block_pool::BlockPool,
        types::{BlockHeader, BlockchainEvent},
        utils::Context,
    };

    fn hash(branch: u8, height: u64) -> String {
        format!("{:02x}{:062x}", branch, height)
    }

    fn id(branch: u8, height: u64) -> BlockIdentifier {
        BlockIdentifier {
            index: height,
            hash: format!("0x{}", hash(branch, height)),
        }
    }

    fn header(branch: u8, height: u64) -> BlockHeader {
        BlockHeader {
            block_identifier: id(branch, height),
            parent_block_identifier: id(branch, height - 1),
        }
    }

    /// Mirrors INFRA-389: tip persisted at 154545 on a stale branch of length 4 (154545..154548),
    /// canonical chain continues on branch 0.
    #[test]
    fn stale_tip_rolls_back_a_window_then_connects_with_bounded_memory() {
        let mut canonical: HashMap<u64, String> = HashMap::new();
        for h in 154_400..=154_560u64 {
            canonical.insert(h, hash(0, h));
        }
        let stale_tip = id(1, 154_545);
        let plan = plan_tip_reconciliation(&stale_tip, 100, |h| {
            canonical.get(&h).cloned().ok_or("missing".to_string())
        })
        .unwrap()
        .expect("stale tip must be reconciled");
        assert_eq!(plan.rollback_start, 154_446);
        assert_eq!(plan.rollback_end, 154_545);
        assert_eq!(plan.new_tip, id(0, 154_445));
        assert_eq!(plan.canonical_at_old_tip, id(0, 154_545));

        // Prime the pool with the last 7 canonical headers ending at the new tip, then connect.
        let ctx = Context::empty();
        let mut pool = BlockPool::new();
        for h in (plan.new_tip.index - 6)..=plan.new_tip.index {
            pool.process_header(header(0, h), &ctx).unwrap();
        }
        assert_eq!(pool.canonical_chain_tip(), Some(&plan.new_tip));
        let next = header(0, 154_446);
        assert!(pool.can_process_header(&next));
        match pool.process_header(next, &ctx).unwrap() {
            Some(BlockchainEvent::BlockchainUpdatedWithHeaders(e)) => {
                assert_eq!(e.new_headers.len(), 1);
                assert_eq!(e.new_headers[0].block_identifier, id(0, 154_446));
            }
            other => panic!("expected the canonical block to connect, got {other:?}"),
        }

        // Without the reconciliation the pool only knows the stale tip: a canonical block does not
        // connect and the parent walk must stop at the window instead of running to genesis.
        let mut stale_pool = BlockPool::new();
        stale_pool.process_header(header(1, 154_545), &ctx).unwrap();
        assert!(!stale_pool.can_process_header(&header(0, 154_546)));
        let mut store_len = 1usize;
        let mut depth = 0u64;
        let err = loop {
            depth += 1;
            store_len += 1;
            if let Err(e) = check_parent_walk_bounds(depth, store_len, 100) {
                break e;
            }
        };
        assert!(err.contains("no common ancestor"));
        assert!(depth <= 101 && store_len <= 102, "memory stays bounded");
    }

    #[test]
    fn canonical_tip_is_left_alone() {
        let tip = id(0, 1000);
        let plan = plan_tip_reconciliation(&tip, 100, |h| Ok(hash(0, h))).unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn unknown_hash_tip_is_not_reconciled() {
        let tip = BlockIdentifier {
            index: 5,
            hash: format!("0x{}", "0".repeat(64)),
        };
        assert!(plan_tip_reconciliation(&tip, 100, |_| Err("no".into()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn window_larger_than_height_rolls_back_to_genesis() {
        let tip = id(1, 50);
        let plan = plan_tip_reconciliation(&tip, 100, |h| Ok(hash(0, h)))
            .unwrap()
            .unwrap();
        assert_eq!(plan.rollback_start, 1);
        assert_eq!(plan.new_tip.index, 0);
    }
}
