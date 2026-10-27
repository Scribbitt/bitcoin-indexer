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

/// INFRA-685: a block handed to the block processor at or below the pool tip may be a stale fork
/// behind us (testnet4 produces many 1-2 block ones). When its parents are older than the headers
/// the pool keeps, walking them can never connect and ends in the INFRA-389 bound; when its parent
/// is still known it becomes an equal-length fork the pool may switch to. Such a block is only
/// worth processing if bitcoind's best chain contains it: ask the node first.
///
/// A block above the pool tip is never checked: that is how a genuine reorg to a longer chain
/// arrives (ZMQ announces only the new tip), and it must still walk to the common ancestor.
pub fn needs_best_chain_check(block: &BlockIdentifier, pool_tip: Option<&BlockIdentifier>) -> bool {
    pool_tip.is_some_and(|tip| block.index <= tip.index)
}

/// INFRA-685: true when bitcoind's best chain holds a different block at `block`'s height, i.e. the
/// node itself does not follow `block`. `node_hash_at_height` may carry a `0x` prefix or not.
pub fn is_off_best_chain(block: &BlockIdentifier, node_hash_at_height: &str) -> bool {
    !strip_0x(node_hash_at_height).eq_ignore_ascii_case(strip_0x(&block.hash))
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

    /// Header on `branch` at `height` whose parent is on branch 0 when `height - 1 <= fork_at`,
    /// i.e. branch `branch` forks off branch 0 right after `fork_at`.
    fn fork_header(branch: u8, height: u64, fork_at: u64) -> BlockHeader {
        let parent_branch = if height - 1 <= fork_at { 0 } else { branch };
        BlockHeader {
            block_identifier: id(branch, height),
            parent_block_identifier: id(parent_branch, height - 1),
        }
    }

    /// Pool primed the way `initialize_block_pool` does: the last 7 canonical headers up to `tip`.
    fn primed_pool(tip: u64, ctx: &Context) -> BlockPool {
        let mut pool = BlockPool::new();
        for h in (tip - 6)..=tip {
            pool.process_header(header(0, h), ctx).unwrap();
        }
        pool
    }

    #[derive(Debug)]
    enum Advance {
        Ignored,
        Applied(Vec<BlockchainEvent>),
    }

    /// Mirrors `advance_block_pool`'s loop without the network: `parents` stands in for
    /// `getblock` on a parent hash, `node_best` for `getblockhash` on bitcoind's best chain, and
    /// `store` for the in-memory block store. `stale_check` toggles the INFRA-685 guard so a test
    /// can show the behaviour before it.
    fn advance(
        pool: &mut BlockPool,
        incoming: BlockHeader,
        parents: &HashMap<BlockIdentifier, BlockHeader>,
        node_best: &HashMap<u64, String>,
        reorg_window: u64,
        stale_check: bool,
        ctx: &Context,
    ) -> Result<Advance, String> {
        let mut store: HashMap<BlockIdentifier, BlockHeader> = HashMap::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_front(incoming.block_identifier.clone());
        store.insert(incoming.block_identifier.clone(), incoming);
        let mut depth = 0u64;
        let mut events = vec![];
        while let Some(block_id) = queue.pop_front() {
            if stale_check && depth == 0 {
                let pool_tip = pool.canonical_chain_tip().cloned();
                if needs_best_chain_check(&block_id, pool_tip.as_ref()) {
                    let node_hash = node_best.get(&block_id.index).cloned().ok_or("missing")?;
                    if is_off_best_chain(&block_id, &node_hash) {
                        store.remove(&block_id);
                        assert!(
                            store.is_empty(),
                            "nothing else is held for an ignored block"
                        );
                        return Ok(Advance::Ignored);
                    }
                }
            }
            let header = store.get(&block_id).unwrap().clone();
            if pool.can_process_header(&header) {
                match pool.process_header(header.clone(), ctx)? {
                    Some(event) => {
                        match &event {
                            BlockchainEvent::BlockchainUpdatedWithHeaders(e) => {
                                for h in &e.new_headers {
                                    store.remove(&h.block_identifier).unwrap();
                                }
                            }
                            BlockchainEvent::BlockchainUpdatedWithReorg(e) => {
                                for h in &e.headers_to_apply {
                                    store.remove(&h.block_identifier).unwrap();
                                }
                            }
                        }
                        events.push(event);
                    }
                    // Same as `advance_block_pool`: recorded, canonical chain unchanged.
                    None => {}
                }
                continue;
            }
            depth += 1;
            check_parent_walk_bounds(depth, store.len(), reorg_window)?;
            let parent = parents
                .get(&header.parent_block_identifier)
                .cloned()
                .ok_or("unknown parent")?;
            store.insert(parent.block_identifier.clone(), parent.clone());
            queue.push_front(block_id);
            queue.push_front(parent.block_identifier.clone());
        }
        Ok(Advance::Applied(events))
    }

    /// INFRA-685, the nj guest 203 crash loop: at tip 157450 the indexer is handed a stale testnet4
    /// fork block at 157376. The node's best chain has another block there, so it is dropped: no
    /// error, no parent walk, pool unchanged. Without the guard the same input exits the process.
    #[test]
    fn stale_fork_below_tip_is_ignored_without_error() {
        let ctx = Context::empty();
        let tip = 157_450u64;
        let fork_at = 157_375u64;
        let mut parents = HashMap::new();
        let mut node_best = HashMap::new();
        for h in 157_200..=tip {
            parents.insert(id(0, h), header(0, h));
            node_best.insert(h, hash(0, h));
        }
        let stale = fork_header(1, 157_376, fork_at);
        parents.insert(stale.block_identifier.clone(), stale.clone());

        let mut pool = primed_pool(tip, &ctx);
        let outcome = advance(
            &mut pool,
            stale.clone(),
            &parents,
            &node_best,
            100,
            true,
            &ctx,
        )
        .expect("a stale fork below the tip must not be an error");
        assert!(matches!(outcome, Advance::Ignored), "got {outcome:?}");
        assert_eq!(pool.canonical_chain_tip(), Some(&id(0, tip)));

        // The pool still follows the canonical chain afterwards.
        match advance(
            &mut pool,
            header(0, tip + 1),
            &parents,
            &node_best,
            100,
            true,
            &ctx,
        ) {
            Ok(Advance::Applied(events)) => assert_eq!(events.len(), 1),
            other => panic!("expected the next canonical block to apply, got {other:?}"),
        }

        // Before INFRA-685: the walk descends from 157375 and never meets the 7 primed headers.
        let mut old_pool = primed_pool(tip, &ctx);
        let err = advance(&mut old_pool, stale, &parents, &node_best, 100, false, &ctx)
            .expect_err("the old behaviour exits");
        assert!(err.contains("no common ancestor"), "{err}");
    }

    /// A stale block one below the tip still connects to a primed header. Without the guard it
    /// forms an equal-length fork; with it the block is dropped and the pool tip stays put.
    #[test]
    fn stale_fork_with_known_parent_is_ignored() {
        let ctx = Context::empty();
        let tip = 1_000u64;
        let mut node_best = HashMap::new();
        for h in 900..=tip {
            node_best.insert(h, hash(0, h));
        }
        let parents = HashMap::new();
        let mut pool = primed_pool(tip, &ctx);
        let stale = fork_header(1, tip - 1, tip - 2);
        assert!(
            pool.can_process_header(&stale),
            "its parent is a primed header"
        );
        let outcome = advance(&mut pool, stale, &parents, &node_best, 100, true, &ctx).unwrap();
        assert!(matches!(outcome, Advance::Ignored), "got {outcome:?}");
        assert_eq!(pool.canonical_chain_tip(), Some(&id(0, tip)));
    }

    /// The guard asks the node, it does not assume: a block at or below our tip that bitcoind's best
    /// chain does contain (a same-height reorg with more work) is still walked and applied.
    #[test]
    fn block_below_tip_on_the_node_best_chain_is_still_walked() {
        let ctx = Context::empty();
        let tip = 1_000u64;
        let fork_at = 998u64;
        let mut parents = HashMap::new();
        let mut node_best = HashMap::new();
        for h in 900..=tip {
            parents.insert(id(0, h), header(0, h));
            node_best.insert(h, hash(0, h));
        }
        for h in (fork_at + 1)..=tip {
            let b = fork_header(1, h, fork_at);
            parents.insert(b.block_identifier.clone(), b);
            node_best.insert(h, hash(1, h));
        }
        let mut pool = primed_pool(tip, &ctx);
        let outcome = advance(
            &mut pool,
            fork_header(1, tip, fork_at),
            &parents,
            &node_best,
            100,
            true,
            &ctx,
        )
        .unwrap();
        assert!(matches!(outcome, Advance::Applied(_)), "got {outcome:?}");
    }

    /// A genuine reorg deeper than the window still fails loudly (INFRA-389 is kept): the new tip is
    /// above ours, so the INFRA-685 guard does not apply, and the walk stops at the bound.
    #[test]
    fn genuine_reorg_deeper_than_window_still_errors() {
        let ctx = Context::empty();
        let tip = 1_000u64;
        let fork_at = 850u64;
        let mut parents = HashMap::new();
        let mut node_best = HashMap::new();
        for h in 700..=tip {
            parents.insert(id(0, h), header(0, h));
        }
        for h in 700..=fork_at {
            node_best.insert(h, hash(0, h));
        }
        for h in (fork_at + 1)..=(tip + 1) {
            let b = fork_header(1, h, fork_at);
            parents.insert(b.block_identifier.clone(), b);
            node_best.insert(h, hash(1, h));
        }
        let mut pool = primed_pool(tip, &ctx);
        let err = advance(
            &mut pool,
            fork_header(1, tip + 1, fork_at),
            &parents,
            &node_best,
            100,
            true,
            &ctx,
        )
        .expect_err("a 150-block reorg exceeds the 100-block window");
        assert!(err.contains("reorg deeper than 100 blocks"), "{err}");
        assert_eq!(pool.canonical_chain_tip(), Some(&id(0, tip)));
    }

    /// An ordinary 2-block reorg to a longer chain: ZMQ announces only the new tip (above ours), the
    /// walk fetches its two parents, finds the common ancestor in the primed headers and reorgs.
    #[test]
    fn short_reorg_to_longer_chain_still_applies() {
        let ctx = Context::empty();
        let tip = 1_000u64;
        let fork_at = 998u64;
        let mut parents = HashMap::new();
        let mut node_best = HashMap::new();
        for h in 900..=tip {
            parents.insert(id(0, h), header(0, h));
        }
        for h in 900..=fork_at {
            node_best.insert(h, hash(0, h));
        }
        for h in (fork_at + 1)..=(tip + 1) {
            let b = fork_header(1, h, fork_at);
            parents.insert(b.block_identifier.clone(), b);
            node_best.insert(h, hash(1, h));
        }
        let mut pool = primed_pool(tip, &ctx);
        let events = match advance(
            &mut pool,
            fork_header(1, tip + 1, fork_at),
            &parents,
            &node_best,
            100,
            true,
            &ctx,
        ) {
            Ok(Advance::Applied(events)) => events,
            other => panic!("expected the reorg to apply, got {other:?}"),
        };
        assert_eq!(pool.canonical_chain_tip(), Some(&id(1, tip + 1)));
        let rolled_back: Vec<BlockIdentifier> = events
            .iter()
            .filter_map(|e| match e {
                BlockchainEvent::BlockchainUpdatedWithReorg(r) => Some(
                    r.headers_to_rollback
                        .iter()
                        .map(|h| h.block_identifier.clone())
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .flatten()
            .collect();
        assert!(rolled_back.contains(&id(0, 999)), "{rolled_back:?}");
        assert!(rolled_back.contains(&id(0, 1_000)), "{rolled_back:?}");
    }

    #[test]
    fn best_chain_check_only_at_or_below_the_tip() {
        let tip = id(0, 100);
        assert!(needs_best_chain_check(&id(1, 100), Some(&tip)));
        assert!(needs_best_chain_check(&id(1, 40), Some(&tip)));
        assert!(!needs_best_chain_check(&id(1, 101), Some(&tip)));
        assert!(!needs_best_chain_check(&id(1, 40), None));
        assert!(!is_off_best_chain(&id(0, 40), &hash(0, 40)));
        assert!(!is_off_best_chain(
            &id(0, 40),
            &format!("0x{}", hash(0, 40).to_uppercase())
        ));
        assert!(is_off_best_chain(&id(1, 40), &hash(0, 40)));
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
