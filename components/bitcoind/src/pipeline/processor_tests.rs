//! INFRA-685: the block processor (`process_blocks_message` -> `advance_block_pool` ->
//! `apply_chain_event`) driven through a fake bitcoind and a recording indexer. These are the
//! functions the indexer runs; only the network and the indexer channel are replaced.
//!
//! The sink also keeps a model of the ordinals blocks DB (compacted block per height): stores
//! insert, a rollback deletes the rolled-back heights, as `ordinals::start_ordinals_indexer` does.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use super::{process_blocks_message, BlockProcessorState, ChainSource, FetchedBlock, IndexerSink};
use crate::{
    block_pool::BlockPool,
    types::{BitcoinBlockData, BitcoinBlockMetadata, BitcoinNetwork, BlockHeader, BlockIdentifier},
    utils::Context,
    IndexerCommand,
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

fn bytes_of(id: &BlockIdentifier) -> Vec<u8> {
    id.hash.as_bytes().to_vec()
}

fn block(branch: u8, height: u64, parent_branch: u8) -> BitcoinBlockData {
    BitcoinBlockData {
        block_identifier: id(branch, height),
        parent_block_identifier: id(parent_branch, height - 1),
        timestamp: 0,
        transactions: vec![],
        metadata: BitcoinBlockMetadata {
            network: BitcoinNetwork::Testnet,
        },
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Cmd {
    Store(Vec<u64>),
    Index {
        apply: Vec<BlockIdentifier>,
        rollback: Vec<BlockIdentifier>,
    },
}

#[derive(Default)]
struct FakeChain {
    /// Every block bitcoind knows, by bare hash.
    blocks: HashMap<String, BitcoinBlockData>,
    /// Its best chain: height -> bare hash.
    best: RefCell<BTreeMap<u64, String>>,
    best_chain_fails: Cell<bool>,
    fetched: RefCell<Vec<BlockIdentifier>>,
}

impl FakeChain {
    /// Adds blocks `lo..=hi` on `branch`; block `lo`'s parent is on `parent_branch`.
    fn add(&mut self, branch: u8, lo: u64, hi: u64, parent_branch: u8) {
        for h in lo..=hi {
            let parent = if h == lo { parent_branch } else { branch };
            let b = block(branch, h, parent);
            self.blocks.insert(hash(branch, h), b);
        }
    }

    /// Makes `branch` bitcoind's best chain for `lo..=hi`.
    fn follow(&self, branch: u8, lo: u64, hi: u64) {
        let mut best = self.best.borrow_mut();
        for h in lo..=hi {
            best.insert(h, hash(branch, h));
        }
    }

    /// bitcoind's best chain now ends at `height` (RPC -8 above it).
    fn truncate_best(&self, height: u64) {
        self.best.borrow_mut().retain(|h, _| *h <= height);
    }
}

impl ChainSource for FakeChain {
    async fn fetch_block(&self, hash: &str) -> Result<FetchedBlock, String> {
        let block = self
            .blocks
            .get(hash)
            .cloned()
            .ok_or_else(|| format!("unknown block {hash}"))?;
        self.fetched
            .borrow_mut()
            .push(block.block_identifier.clone());
        let compacted = Some(bytes_of(&block.block_identifier));
        Ok(FetchedBlock { block, compacted })
    }

    async fn best_chain_hash(&self, height: u64) -> Result<Option<String>, String> {
        if self.best_chain_fails.get() {
            return Err("unable to retrieve block hash: connection refused".into());
        }
        Ok(self.best.borrow().get(&height).cloned())
    }
}

#[derive(Default)]
struct RecordingSink {
    commands: RefCell<Vec<Cmd>>,
    /// Model of the ordinals blocks DB: height -> compacted bytes.
    blocks_db: RefCell<BTreeMap<u64, Vec<u8>>>,
    /// The indexer keeps a blocks DB (ordinals), so applied blocks must be in it.
    models_blocks_db: bool,
    /// Model of `metadata::last_insert`: the last height stored, or the height below the last
    /// rolled-back one. The ordinals restart resumes from the lower of this and the Postgres tip.
    last_insert: Cell<u64>,
}

impl IndexerSink for RecordingSink {
    fn send(&self, command: IndexerCommand) -> Result<(), String> {
        match command {
            IndexerCommand::StoreCompactedBlocks(blocks) => {
                let mut db = self.blocks_db.borrow_mut();
                let heights = blocks.iter().map(|(h, _)| *h).collect();
                let mut sorted_blocks = blocks;
                sorted_blocks.sort_by_key(|(h, _)| *h);
                for (h, bytes) in sorted_blocks {
                    self.last_insert.set(h);
                    db.insert(h, bytes);
                }
                self.commands.borrow_mut().push(Cmd::Store(heights));
            }
            IndexerCommand::IndexBlocks {
                apply_blocks,
                rollback_block_ids,
            } => {
                let mut db = self.blocks_db.borrow_mut();
                for id in rollback_block_ids.iter() {
                    db.remove(&id.index);
                    self.last_insert.set(id.index - 1);
                }
                // The indexer reads each applied block from the blocks DB (sat traversal).
                for b in apply_blocks.iter() {
                    assert!(
                        !self.models_blocks_db
                            || db.get(&b.block_identifier.index)
                                == Some(&bytes_of(&b.block_identifier)),
                        "block {} applied without its compacted bytes in the blocks DB",
                        b.block_identifier
                    );
                }
                self.commands.borrow_mut().push(Cmd::Index {
                    apply: apply_blocks
                        .into_iter()
                        .map(|b| b.block_identifier)
                        .collect(),
                    rollback: rollback_block_ids,
                });
            }
            _ => {}
        }
        Ok(())
    }
}

struct Env {
    pool: Arc<Mutex<BlockPool>>,
    store: Arc<Mutex<HashMap<BlockIdentifier, BitcoinBlockData>>>,
    chain: FakeChain,
    sink: RecordingSink,
    compress: bool,
    window: u64,
    ctx: Context,
}

impl Env {
    /// A processor at `tip` on branch 0, its pool primed the way `initialize_block_pool` does
    /// (as many headers as the pool keeps). With `compress`, the blocks DB holds branch 0.
    fn new(pool: BlockPool, tip: u64, window: u64, compress: bool) -> Env {
        let ctx = Context::empty();
        let mut chain = FakeChain::default();
        let low = tip.saturating_sub(window + 60).max(1);
        chain.add(0, low, tip, 0);
        chain.follow(0, low, tip);
        let mut pool = pool;
        let primed = pool.retained_headers() as u64;
        for h in (tip + 1 - primed)..=tip {
            pool.process_header(
                BlockHeader {
                    block_identifier: id(0, h),
                    parent_block_identifier: id(0, h - 1),
                },
                &ctx,
            )
            .unwrap();
        }
        let sink = RecordingSink {
            models_blocks_db: compress,
            ..Default::default()
        };
        if compress {
            let mut db = sink.blocks_db.borrow_mut();
            for h in low..=tip {
                db.insert(h, bytes_of(&id(0, h)));
            }
            sink.last_insert.set(tip);
        }
        Env {
            pool: Arc::new(Mutex::new(pool)),
            store: Arc::new(Mutex::new(HashMap::new())),
            chain,
            sink,
            compress,
            window,
            ctx,
        }
    }

    /// What the ZMQ pipeline sends for a `hashblock` announcement.
    fn announce(&self, b: BitcoinBlockData) -> Result<(), String> {
        let compacted = if self.compress {
            vec![(b.block_identifier.index, bytes_of(&b.block_identifier))]
        } else {
            vec![]
        };
        let state = BlockProcessorState {
            block_pool: &self.pool,
            block_store: &self.store,
            source: &self.chain,
            sink: &self.sink,
            compress_blocks: self.compress,
            reorg_window: self.window,
            ctx: &self.ctx,
        };
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(process_blocks_message(compacted, vec![b], &state))
    }

    fn announce_tip(&self, branch: u8, height: u64) -> Result<(), String> {
        let b = self
            .chain
            .blocks
            .get(&hash(branch, height))
            .cloned()
            .expect("block added to the fake chain");
        self.announce(b)
    }

    fn tip(&self) -> BlockIdentifier {
        self.pool
            .lock()
            .unwrap()
            .canonical_chain_tip()
            .cloned()
            .unwrap()
    }

    fn commands(&self) -> Vec<Cmd> {
        self.sink.commands.borrow().clone()
    }

    fn store_len(&self) -> usize {
        self.store.lock().unwrap().len()
    }

    /// The blocks DB tip is the highest block stored (a restart must not resume below the Postgres
    /// tip, which would index a block twice).
    fn assert_last_insert(&self, height: u64) {
        assert_eq!(self.sink.last_insert.get(), height, "blocks DB last_insert");
    }

    /// The blocks DB holds exactly `branch_of(h)`'s block at every height in `lo..=hi`.
    fn assert_blocks_db(&self, lo: u64, hi: u64, branch_of: impl Fn(u64) -> u8) {
        let db = self.sink.blocks_db.borrow();
        for h in lo..=hi {
            assert_eq!(
                db.get(&h),
                Some(&bytes_of(&id(branch_of(h), h))),
                "blocks DB at #{h}"
            );
        }
    }
}

fn ids(branch: u8, lo: u64, hi: u64) -> Vec<BlockIdentifier> {
    (lo..=hi).map(|h| id(branch, h)).collect()
}

fn sorted(mut v: Vec<BlockIdentifier>) -> Vec<BlockIdentifier> {
    v.sort_by_key(|b| b.index);
    v
}

/// Everything applied, in order, and everything rolled back, sorted, over all commands.
fn applied_and_rolled_back(cmds: &[Cmd]) -> (Vec<BlockIdentifier>, Vec<BlockIdentifier>) {
    let mut applied = vec![];
    let mut rolled_back = vec![];
    for c in cmds {
        if let Cmd::Index { apply, rollback } = c {
            applied.extend(apply.iter().cloned());
            rolled_back.extend(rollback.iter().cloned());
        }
    }
    (applied, sorted(rolled_back))
}

/// The nj guest 203 crash (INFRA-685 root cause): testnet4 reorgs deeper than the seven headers
/// the pool used to keep. Tip 157798, bitcoind switches to a branch forking after 157780 and ZMQ
/// announces its tip 157799. The pool now keeps the reorg window, so the walk connects.
#[test]
fn reorg_deeper_than_seven_blocks_is_followed() {
    let env = Env::new(BlockPool::for_reorg_window(100), 157_798, 100, true);
    let mut env = env;
    env.chain.add(1, 157_781, 157_799, 0);
    env.chain.follow(1, 157_781, 157_799);
    env.announce_tip(1, 157_799)
        .expect("an 18-block reorg is within the window");

    assert_eq!(env.tip(), id(1, 157_799));
    let cmds = env.commands();
    let (applied, rolled_back) = applied_and_rolled_back(&cmds);
    assert_eq!(applied, ids(1, 157_781, 157_799));
    assert_eq!(rolled_back, ids(0, 157_781, 157_798));
    // With a blocks DB the rollback goes out alone, before the new branch's compacted blocks.
    let first_rollback = cmds
        .iter()
        .position(|c| matches!(c, Cmd::Index { rollback, .. } if !rollback.is_empty()))
        .unwrap();
    assert!(
        matches!(&cmds[first_rollback], Cmd::Index { apply, .. } if apply.is_empty()),
        "{cmds:?}"
    );
    assert!(
        matches!(&cmds[first_rollback + 1], Cmd::Store(h) if h[0] == 157_781),
        "{cmds:?}"
    );
    // No hole in the blocks DB (review blocker 1).
    env.assert_blocks_db(157_700, 157_799, |h| if h > 157_780 { 1 } else { 0 });
    env.assert_last_insert(157_799);
    assert_eq!(env.store_len(), 0, "nothing left in the block store");
}

/// The same reorg against the old seven-header pool: the walk never meets a known header and
/// exits with the INFRA-389 error, which is what crash-looped the runes indexer.
#[test]
fn seven_header_pool_cannot_follow_the_same_reorg() {
    let mut env = Env::new(BlockPool::new(), 157_798, 100, false);
    env.chain.add(1, 157_781, 157_799, 0);
    env.chain.follow(1, 157_781, 157_799);
    let err = env.announce_tip(1, 157_799).unwrap_err();
    assert!(err.contains("reorg deeper than 100 blocks"), "{err}");
}

/// INFRA-389 is kept: a reorg deeper than the window still fails loudly and changes nothing.
#[test]
fn reorg_deeper_than_the_window_still_errors() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, false);
    env.chain.add(1, 851, 1_001, 0);
    env.chain.follow(1, 851, 1_001);
    let err = env.announce_tip(1, 1_001).unwrap_err();
    assert!(err.contains("reorg deeper than 100 blocks"), "{err}");
    assert_eq!(env.tip(), id(0, 1_000));
    assert!(env.commands().is_empty());
}

/// A 2-block reorg to a longer chain (runes, no blocks DB): rollback and apply in one command.
/// Before INFRA-685 the walked parent 999' made `process_header` return None, which was fatal.
#[test]
fn two_block_reorg_applies_without_blocks_db() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, false);
    env.chain.add(1, 999, 1_001, 0);
    env.chain.follow(1, 999, 1_001);
    env.announce_tip(1, 1_001).unwrap();
    assert_eq!(env.tip(), id(1, 1_001));
    let cmds = env.commands();
    assert!(!cmds.iter().any(|c| matches!(c, Cmd::Store(_))), "{cmds:?}");
    let (applied, rolled_back) = applied_and_rolled_back(&cmds);
    assert_eq!(applied, ids(1, 999, 1_001));
    assert_eq!(rolled_back, ids(0, 999, 1_000));
    // Without a blocks DB, rollback and apply travel together as before.
    assert!(
        matches!(&cmds[0], Cmd::Index { apply, rollback } if !apply.is_empty() && !rollback.is_empty()),
        "{cmds:?}"
    );
    assert_eq!(env.store_len(), 0);
}

/// A stale fork block far below the tip that bitcoind does not follow is dropped before its
/// bytes reach the blocks DB: no store, no index, the pool and the DB unchanged.
#[test]
fn stale_fork_below_tip_is_dropped_before_its_bytes_are_stored() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 157_450, 100, true);
    env.chain.add(1, 157_376, 157_376, 0);
    env.announce_tip(1, 157_376).unwrap();
    assert!(env.commands().is_empty(), "{:?}", env.commands());
    assert_eq!(env.tip(), id(0, 157_450));
    env.assert_blocks_db(157_370, 157_450, |_| 0);
    assert_eq!(env.store_len(), 0);

    // The next canonical block still applies.
    env.chain.add(0, 157_451, 157_451, 0);
    env.chain.follow(0, 157_451, 157_451);
    env.announce_tip(0, 157_451).unwrap();
    assert_eq!(env.tip(), id(0, 157_451));
}

/// A stale block one below the tip whose parent the pool knows: dropped, so it never becomes an
/// equal-length fork the pool could switch to.
#[test]
fn stale_fork_with_known_parent_is_dropped() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, true);
    env.chain.add(1, 999, 999, 0);
    env.announce_tip(1, 999).unwrap();
    assert!(env.commands().is_empty());
    assert_eq!(env.tip(), id(0, 1_000));
    env.assert_blocks_db(990, 1_000, |_| 0);
}

/// bitcoind's best chain ends below the block (getblockhash returns -8): not on its best chain,
/// dropped, instead of retrying and exiting (review finding 4).
#[test]
fn block_above_the_node_tip_is_dropped() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, false);
    env.chain.add(1, 1_000, 1_000, 0);
    env.chain.truncate_best(998);
    env.announce_tip(1, 1_000).unwrap();
    assert!(env.commands().is_empty());
    assert_eq!(env.tip(), id(0, 1_000));
}

/// A failing getblockhash is an error, not a silent drop.
#[test]
fn best_chain_lookup_error_is_returned() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, false);
    env.chain.add(1, 999, 999, 0);
    env.chain.best_chain_fails.set(true);
    let err = env.announce_tip(1, 999).unwrap_err();
    assert!(err.contains("connection refused"), "{err}");
}

/// bitcoind follows a block the pool does not rank above its tip (a shorter chain with more
/// work): the announced block's `None` stays an error so the restart reconciles the index
/// (review finding 2), and nothing is left in the block store.
#[test]
fn announced_block_that_does_not_move_the_chain_is_an_error() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, false);
    env.chain.add(1, 999, 999, 0);
    env.chain.truncate_best(998);
    env.chain.follow(1, 999, 999);
    let err = env.announce_tip(1, 999).unwrap_err();
    assert!(err.contains("does not rank its branch above"), "{err}");
    assert_eq!(env.tip(), id(0, 1_000));
    assert_eq!(env.store_len(), 0);
}

/// The same block announced twice: ignored, nothing sent, nothing kept.
#[test]
fn repeated_announcement_is_ignored() {
    let env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, false);
    env.announce_tip(0, 1_000).unwrap();
    assert!(env.commands().is_empty());
    assert_eq!(env.store_len(), 0);
}

/// ZMQ missed block 1001 and announces 1002: the walk fetches 1001, and with a blocks DB its
/// bytes are stored before it is indexed (review blocker 1, no reorg involved).
#[test]
fn walked_parent_is_stored_in_the_blocks_db() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, true);
    env.chain.add(0, 1_001, 1_002, 0);
    env.chain.follow(0, 1_001, 1_002);
    env.announce_tip(0, 1_002).unwrap();
    assert_eq!(env.tip(), id(0, 1_002));
    assert_eq!(
        env.commands(),
        vec![
            Cmd::Store(vec![1_002]),
            // The announced block is stored again after its parent: the blocks DB tip must end
            // at the highest height, not at the parent's.
            Cmd::Store(vec![1_001, 1_002]),
            Cmd::Index {
                apply: vec![id(0, 1_001)],
                rollback: vec![]
            },
            Cmd::Index {
                apply: vec![id(0, 1_002)],
                rollback: vec![]
            },
        ]
    );
    env.assert_blocks_db(990, 1_002, |_| 0);
    env.assert_last_insert(1_002);
}

/// A -> B -> A: a block applied, rolled back and applied again is no longer in the block store.
/// It used to be `remove(..).unwrap()` (a panic); it is now downloaded again (review note 8).
#[test]
fn flip_flop_back_to_the_first_branch_refetches_its_blocks() {
    let mut env = Env::new(BlockPool::for_reorg_window(100), 1_000, 100, true);
    // B forks after 999 and wins with 1001'.
    env.chain.add(1, 1_000, 1_001, 0);
    env.chain.follow(1, 1_000, 1_001);
    env.announce_tip(1, 1_001).unwrap();
    assert_eq!(env.tip(), id(1, 1_001));
    // A comes back with 1001 and 1002.
    env.chain.add(0, 1_001, 1_002, 0);
    env.chain.follow(0, 1_000, 1_002);
    env.announce_tip(0, 1_002).unwrap();
    assert_eq!(env.tip(), id(0, 1_002));
    assert!(
        env.chain.fetched.borrow().contains(&id(0, 1_000)),
        "A's 1000 was applied before, so it had to be downloaded again"
    );
    env.assert_blocks_db(990, 1_002, |_| 0);
    assert_eq!(env.store_len(), 0);
}
