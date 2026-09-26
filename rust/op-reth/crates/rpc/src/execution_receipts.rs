//! IPC receipt feed. Each subscription starts at a canonical anchor, then delivers a continuous
//! execution prefix. A reset is terminal: clients must reload state and subscribe again.

use crate::{OpEthApi, OpEthApiError};
use alloy_consensus::{BlockHeader, TxReceipt};
use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use jsonrpsee::{RpcModule, server::SubscriptionMessage};
use reth_chain_state::CanonStateSubscriptions;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_optimism_flashblocks::PendingFlashBlock;
use reth_optimism_primitives::OpPrimitives;
use reth_primitives_traits::{InMemorySize, Recovered, TransactionMeta};
use reth_rpc_eth_api::{EthApiTypes, RpcConvert, RpcNodeCore, transaction::ConvertReceiptInput};
use reth_rpc_eth_types::block::BlockAndReceipts;
use reth_storage_api::{
    BlockHashReader, BlockReader, BlockReaderIdExt, ReceiptProvider, TransactionVariant,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;

const MAX_BLOCKS: usize = 64;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECOVERY_BLOCKS: u64 = 1024;
const SEND_TIMEOUT: Duration = Duration::from_secs(2);
static SESSIONS: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Seal {
    block_number: u64,
    block_hash: B256,
    parent_hash: B256,
    transaction_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event<T> {
    Apply(T),
    Seal(Seal),
}

#[derive(Clone, Debug)]
struct Snapshot {
    data: BlockAndReceipts<OpPrimitives>,
    canonical: bool,
    // Only a locally calculated root can certify a speculative parent's full header hash.
    computed_hash: bool,
    publication_id: u64,
}

impl Snapshot {
    fn pending(p: &PendingFlashBlock<OpPrimitives>) -> Self {
        Self {
            data: p.to_block_and_receipts(),
            canonical: false,
            computed_hash: p.has_computed_state_root,
            publication_id: p.publication_id,
        }
    }

    fn number(&self) -> u64 {
        self.data.block.number()
    }
    fn hash(&self) -> B256 {
        self.data.block.hash()
    }
    fn parent(&self) -> B256 {
        self.data.block.parent_hash()
    }
    fn len(&self) -> usize {
        self.data.receipts.len()
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.data.block.body().transactions.len() != self.len() {
            return Err("receipt_count_mismatch");
        }
        if self.bytes() > MAX_BYTES {
            return Err("buffer_limit");
        }
        Ok(())
    }

    fn bytes(&self) -> usize {
        self.data.block.size() + self.data.receipts.iter().map(InMemorySize::size).sum::<usize>()
    }

    fn extends(&self, previous: &Self) -> bool {
        self.parent() == previous.parent() &&
            self.len() >= previous.len() &&
            self.data.receipts[..previous.len()] == previous.data.receipts[..] &&
            self.data
                .block
                .body()
                .transactions
                .iter()
                .zip(previous.data.block.body().transactions())
                .all(|(a, b)| a.tx_hash() == b.tx_hash())
    }
}

#[derive(Debug)]
struct Delta {
    snapshot: Snapshot,
    start: usize,
}

/// Pure ordering/reconciliation logic. It never does provider I/O or writes to a client socket.
#[derive(Debug)]
struct ReceiptSequence {
    anchor: BlockNumHash,
    emitted: BTreeMap<u64, Snapshot>,
    waiting: BTreeMap<u64, Snapshot>,
}

impl ReceiptSequence {
    const fn new(anchor: BlockNumHash) -> Self {
        Self { anchor, emitted: BTreeMap::new(), waiting: BTreeMap::new() }
    }

    fn needs_recovery(
        &self,
        next_canonical: Option<&Snapshot>,
        pending_target: Option<u64>,
    ) -> Result<bool, &'static str> {
        let target = next_canonical
            .map_or(pending_target, |snapshot| snapshot.number().checked_sub(1))
            .unwrap_or(self.anchor.number);
        let missing = target.saturating_sub(self.anchor.number);

        if missing > MAX_RECOVERY_BLOCKS {
            return Err("canonical_recovery_limit");
        }
        Ok(missing != 0)
    }

    fn pending(&mut self, snapshot: Snapshot) -> Result<Vec<Event<Delta>>, &'static str> {
        snapshot.validate()?;
        let number = snapshot.number();
        if number <= self.anchor.number {
            return Ok(Vec::new());
        }
        // The initial latest-state snapshot may overtake publications already in the queue.
        if self
            .waiting
            .get(&number)
            .or_else(|| self.emitted.get(&number))
            .is_some_and(|old| old.publication_id >= snapshot.publication_id)
        {
            return Ok(Vec::new());
        }
        self.waiting.insert(number, snapshot);
        self.drain()
    }

    fn canonical(&mut self, snapshot: Snapshot) -> Result<Vec<Event<Delta>>, &'static str> {
        snapshot.validate()?;
        let number = snapshot.number();
        if number <= self.anchor.number {
            if number == self.anchor.number && snapshot.hash() != self.anchor.hash {
                return Err("canonical_reorg");
            }
            return Ok(Vec::new());
        }
        if number != self.anchor.number + 1 {
            return Err("canonical_gap");
        }
        if snapshot.parent() != self.anchor.hash {
            return Err("canonical_parent_mismatch");
        }
        let mut deltas = Vec::new();
        let mut start = 0;
        if let Some(previous) = self.emitted.remove(&number) {
            if !snapshot.extends(&previous) {
                return Err("execution_mismatch");
            }
            start = previous.len();
            // A child may only have been emitted once its entire parent was certified.
            if let Some((_, child)) = self.emitted.first_key_value() &&
                (start != snapshot.len() || child.parent() != snapshot.hash())
            {
                return Err("published_parent_changed");
            }
        }
        self.waiting.remove(&number);
        self.anchor = BlockNumHash::new(number, snapshot.hash());
        let seal = Seal {
            block_number: number,
            block_hash: snapshot.hash(),
            parent_hash: snapshot.parent(),
            transaction_count: snapshot.len(),
        };
        if start < snapshot.len() {
            deltas.push(Event::Apply(Delta { snapshot, start }));
        }
        // Repair the parent tail, seal it, then release any waiting child.
        deltas.push(Event::Seal(seal));
        deltas.extend(self.drain()?);
        Ok(deltas)
    }

    fn drain(&mut self) -> Result<Vec<Event<Delta>>, &'static str> {
        let mut deltas = Vec::new();
        while let Some((&number, snapshot)) = self.waiting.first_key_value() {
            let mut start = 0;
            if let Some(previous) = self.emitted.get(&number) {
                if !snapshot.extends(previous) {
                    return Err("pending_prefix_changed");
                }
                start = previous.len();
                if self.emitted.last_key_value().is_some_and(|(&last, _)| last > number) &&
                    (snapshot.len() != previous.len() || snapshot.hash() != previous.hash())
                {
                    return Err("published_parent_changed");
                }
            } else if let Some((&parent_number, parent)) = self.emitted.last_key_value() {
                if number != parent_number + 1 || !parent.computed_hash {
                    break;
                }
                if snapshot.parent() != parent.hash() {
                    break;
                }
            } else {
                if number != self.anchor.number + 1 {
                    break;
                }
                if snapshot.parent() != self.anchor.hash {
                    return Err("pending_parent_changed");
                }
            }
            let snapshot = self.waiting.pop_first().expect("checked nonempty").1;
            let first = self.emitted.insert(number, snapshot.clone()).is_none();
            // Emit the first empty pending snapshot so the client has its block context.
            if first || start < snapshot.len() {
                deltas.push(Event::Apply(Delta { snapshot, start }));
            }
        }
        self.check_bound()?;
        Ok(deltas)
    }

    fn check_bound(&self) -> Result<(), &'static str> {
        if self.emitted.len() + self.waiting.len() > MAX_BLOCKS ||
            self.emitted
                .values()
                .chain(self.waiting.values())
                .map(Snapshot::bytes)
                .sum::<usize>() >
                MAX_BYTES
        {
            return Err("buffer_limit");
        }
        Ok(())
    }
}

/// Read one canonical successor without retaining the entire recovery range.
/// Receipt counts and memory bounds are checked when the sequence accepts the snapshot.
fn read_canonical_successor(
    anchor: BlockNumHash,
    mut read_hash: impl FnMut(u64) -> eyre::Result<Option<B256>>,
    read_snapshot: impl FnOnce(B256) -> eyre::Result<Snapshot>,
) -> eyre::Result<Option<Snapshot>> {
    let hash =
        read_hash(anchor.number)?.ok_or_else(|| eyre::eyre!("canonical_anchor_unavailable"))?;
    if hash != anchor.hash {
        tracing::warn!(target: "rpc::execution_receipts", block = anchor.number,
            expected_hash = %anchor.hash, actual_hash = %hash, "Canonical recovery anchor changed");
        eyre::bail!("canonical_anchor_changed");
    }

    let Some(number) = anchor.number.checked_add(1) else {
        eyre::bail!("canonical_height_overflow");
    };
    let Some(hash) = read_hash(number)? else { return Ok(None) };
    let snapshot = read_snapshot(hash)?;

    // Hash-based reads keep the block and its receipts together. Recheck canonical membership
    // after the reads in case the provider changed branches while recovery was in progress.
    let current_anchor = read_hash(anchor.number)?;
    if current_anchor != Some(anchor.hash) {
        tracing::warn!(target: "rpc::execution_receipts", block = anchor.number,
            expected_hash = %anchor.hash, actual_hash = ?current_anchor, "Canonical recovery anchor changed during read");
        eyre::bail!("canonical_anchor_changed");
    }
    let current_hash = read_hash(number)?;
    if current_hash != Some(hash) {
        tracing::warn!(target: "rpc::execution_receipts", block = number,
            expected_hash = %hash, actual_hash = ?current_hash, "Canonical recovery successor changed during read");
        eyre::bail!("canonical_successor_changed");
    }
    if snapshot.number() != number || snapshot.hash() != hash || snapshot.parent() != anchor.hash {
        tracing::warn!(target: "rpc::execution_receipts", expected_number = number, expected_hash = %hash,
            expected_parent = %anchor.hash, actual_number = snapshot.number(), actual_hash = %snapshot.hash(),
            actual_parent = %snapshot.parent(), "Canonical recovery successor does not extend anchor");
        eyre::bail!("canonical_parent_mismatch");
    }
    Ok(Some(snapshot))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Update<R> {
    block_number: u64,
    parent_hash: B256,
    block_hash: B256,
    source: &'static str,
    from_transaction_index: usize,
    receipts: Vec<R>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename = "batch", rename_all = "camelCase")]
struct ReceiptNotification<'a, R> {
    session_id: &'a str,
    sequence: u64,
    events: &'a [Event<Update<R>>],
}

fn convert_delta<Rpc: RpcConvert<Primitives = OpPrimitives>>(
    delta: Delta,
    converter: &Rpc,
) -> Result<Update<<Rpc::Network as reth_rpc_eth_api::RpcTypes>::Receipt>, String> {
    let Snapshot { data, canonical, .. } = delta.snapshot;
    let mut cumulative = 0;
    let mut next_log_index = 0;
    let mut inputs = Vec::with_capacity(data.receipts.len() - delta.start);
    for (index, ((sender, tx), receipt)) in
        data.block.transactions_with_sender().zip(data.receipts.iter()).enumerate()
    {
        let gas_used = receipt
            .cumulative_gas_used()
            .checked_sub(cumulative)
            .ok_or("invalid_cumulative_gas")?;
        if index >= delta.start {
            inputs.push(ConvertReceiptInput {
                tx: Recovered::new_unchecked(tx, *sender),
                receipt: receipt.clone(),
                gas_used,
                next_log_index,
                meta: TransactionMeta {
                    tx_hash: tx.tx_hash(),
                    index: index as u64,
                    block_hash: data.block.hash(),
                    block_number: data.block.number(),
                    base_fee: data.block.base_fee_per_gas(),
                    timestamp: data.block.timestamp(),
                    excess_blob_gas: data.block.excess_blob_gas(),
                },
            });
        }
        cumulative = receipt.cumulative_gas_used();
        next_log_index += receipt.logs().len();
    }
    let receipts = converter
        .convert_receipts_with_block(inputs, data.block.sealed_block())
        .map_err(|e| e.to_string())?;
    Ok(Update {
        block_number: data.block.number(),
        parent_hash: data.block.parent_hash(),
        block_hash: data.block.hash(),
        source: if canonical { "canonical" } else { "pending" },
        from_transaction_index: delta.start,
        receipts,
    })
}

/// Registers IPC-only apply/seal events. No replay or resume parameters. `ready` is sequence 0;
/// every update/reset increments the per-subscription sequence.
pub fn execution_receipts_rpc<N, Rpc>(eth: OpEthApi<N, Rpc>) -> eyre::Result<RpcModule<()>>
where
    N: RpcNodeCore<Primitives = OpPrimitives>,
    Rpc: RpcConvert<Primitives = OpPrimitives, Error = OpEthApiError>,
{
    let mut module = RpcModule::new(());
    module.register_subscription(
        "eth_subscribeExecutionReceipts", "eth_executionReceipts", "eth_unsubscribeExecutionReceipts",
        move |params, pending_sink, _, _| {
            let eth = eth.clone();
            async move {
                if let Err(error) = params.parse::<Option<[Value; 0]>>() {
                    pending_sink.reject(error).await;
                    return Ok::<(), jsonrpsee::core::SubscriptionError>(());
                }
                let Some(mut publications) = eth.subscribe_published_blocks() else {
                    pending_sink.reject(jsonrpsee_types::ErrorObjectOwned::owned(-32000, "Flashblocks is disabled", None::<()>)).await;
                    return Ok(());
                };
                // Subscribe before reading the anchor. Canonical events racing with initialization
                // are retained; duplicate commits below the anchor are ignored, reorgs never are.
                let mut canonical = eth.provider().subscribe_to_canonical_state();
                let provider = eth.provider().clone();
                let anchor = tokio::task::spawn_blocking(move || provider.latest_header()).await;
                let anchor = match anchor {
                    Ok(Ok(Some(header))) => BlockNumHash::new(header.number(), header.hash()),
                    _ => {
                        pending_sink.reject(jsonrpsee_types::ErrorObjectOwned::owned(-32000, "Canonical anchor unavailable", None::<()>)).await;
                        return Ok(());
                    }
                };
                let mut first = eth.pending_block_rx().and_then(|rx| rx.borrow().as_ref().map(Snapshot::pending));
                let sink = pending_sink.accept().await?;
                let session = format!("{}-{}-{}", std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos(), SESSIONS.fetch_add(1, Ordering::Relaxed));
                let mut sequence = 0_u64;
                let mut state = ReceiptSequence::new(anchor);
                let mut canonical_blocks = VecDeque::<Snapshot>::new();
                let mut repair_to = None;
                let mut canonical_input = None;
                let spec = eth.provider().chain_spec();
                let ready = json!({
                    "type":"ready", "sessionId":session, "sequence":sequence,
                    "chainId":spec.chain_id(), "genesisHash":spec.genesis_hash(),
                    "anchor":{"blockNumber":anchor.number,"blockHash":anchor.hash}});
                let ready = SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &ready)?;
                if sink.send_timeout(ready, SEND_TIMEOUT).await.is_err() { return Ok(()); }
                let result: Result<(), String> = async {
                    loop {
                        let deltas;
                        // Keep a jumped notification queued until its predecessors have been
                        // applied and sent. Each iteration reads at most one missing block.
                        if let Some(snapshot) = canonical_blocks.front() {
                            canonical_input = Some((snapshot.number(), snapshot.hash(), snapshot.parent()));
                        }
                        if state.needs_recovery(canonical_blocks.front(), repair_to)? {
                            let required = !canonical_blocks.is_empty();
                            let provider = eth.provider().clone();
                            let anchor = state.anchor;
                            let snapshot = tokio::task::spawn_blocking(move || {
                                // Pending publications may precede execution of their parent.
                                if !required && provider.latest_header()?.is_none_or(|head| head.number() <= anchor.number) {
                                    return Ok(None);
                                }
                                read_canonical_successor(anchor, |number| Ok(provider.block_hash(number)?), |hash| {
                                    let block = provider.recovered_block(hash.into(), TransactionVariant::WithHash)?
                                        .ok_or_else(|| eyre::eyre!("canonical_block_unavailable"))?;
                                    let receipts = provider.receipts_by_block(hash.into())?
                                        .ok_or_else(|| eyre::eyre!("canonical_receipts_unavailable"))?;
                                    Ok(Snapshot { data: BlockAndReceipts::new(Arc::new(block), Arc::new(receipts)),
                                        canonical: true, computed_hash: true, publication_id: 0 })
                                })
                            }).await.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
                            let Some(snapshot) = snapshot else {
                                if required { return Err("canonical_block_unavailable".into()); }
                                // A pending child can precede its canonical parent. Wait for a
                                // new input rather than polling the database in a tight loop.
                                repair_to = None;
                                continue;
                            };
                            canonical_input = Some((snapshot.number(), snapshot.hash(), snapshot.parent()));
                            deltas = state.canonical(snapshot)?;
                        } else if let Some(snapshot) = canonical_blocks.pop_front() {
                            deltas = state.canonical(snapshot)?;
                        } else {
                            let pending = if let Some(pending) = first.take() {
                                pending
                            } else { tokio::select! {
                                _ = sink.closed() => return Ok(()),
                                event = publications.recv() => {
                                    let pending = event.map_err(|e| match e {
                                        broadcast::error::RecvError::Lagged(_) => "publication_gap",
                                        broadcast::error::RecvError::Closed => "publication_stream_closed",
                                    })?;
                                    Snapshot::pending(&pending)
                                }
                                event = canonical.recv() => {
                                    let event = event.map_err(|_| "canonical_stream_gap")?;
                                    if event.reverted().is_some() { return Err("canonical_reorg".into()); }
                                    let mut bytes = 0;
                                    for (block, receipts) in event.committed().blocks_and_receipts() {
                                        let snapshot = Snapshot { data: BlockAndReceipts::new(block.clone(), Arc::new(receipts.clone())),
                                            canonical: true, computed_hash: true, publication_id: 0 };
                                        bytes += snapshot.bytes();
                                        if canonical_blocks.len() >= MAX_RECOVERY_BLOCKS as usize || bytes > MAX_BYTES {
                                            return Err("buffer_limit".into());
                                        }
                                        canonical_blocks.push_back(snapshot);
                                    }
                                    continue;
                                }
                            }};
                            deltas = state.pending(pending)?;
                            // Readable canonical parents need not wait for their notification.
                            repair_to = state.waiting.last_key_value()
                                .map(|(&number, _)| number.saturating_sub(1));
                        }
                        if deltas.is_empty() { continue; }
                        let converter_eth = eth.clone();
                        let next_sequence = sequence + 1;
                        let session_copy = session.clone();
                        let method = sink.method_name().to_owned();
                        let subscription_id = sink.subscription_id();
                        // Conversion and serialization run off the async executor as well as off
                        // the Flashblocks service. Only the bounded socket enqueue is awaited here.
                        let message = tokio::task::spawn_blocking(move || -> Result<_, String> {
                            let events = deltas.into_iter().map(|event| match event {
                                Event::Apply(delta) => convert_delta(delta, converter_eth.converter()).map(Event::Apply),
                                Event::Seal(seal) => Ok(Event::Seal(seal)),
                            }).collect::<Result<Vec<_>, String>>()?;
                            let response = ReceiptNotification { session_id: &session_copy, sequence: next_sequence, events: &events };
                            let message = SubscriptionMessage::new(&method, subscription_id, &response).map_err(|e| e.to_string())?;
                            Ok(message)
                        }).await.map_err(|e| e.to_string())??;
                        tokio::time::timeout(SEND_TIMEOUT, sink.send(message)).await
                            .map_err(|_| "client_too_slow")?.map_err(|_| "client_disconnected")?;
                        sequence = next_sequence;
                    }
                }.await;
                if let Err(reason) = result {
                    sequence += 1;
                    let context = json!({
                        "anchor":{"blockNumber":state.anchor.number,"blockHash":state.anchor.hash},
                        "lastCanonicalInput":canonical_input.map(|(number, hash, parent)| json!({
                            "blockNumber":number,"blockHash":hash,"parentHash":parent})),
                        "queuedCanonical":canonical_blocks.front().map(|snapshot| json!({
                            "blockNumber":snapshot.number(),"blockHash":snapshot.hash(),"parentHash":snapshot.parent()})),
                        "repairTarget":canonical_blocks.front()
                            .and_then(|snapshot| snapshot.number().checked_sub(1))
                            .or(repair_to).filter(|&to| to > state.anchor.number),
                    });
                    tracing::warn!(target: "rpc::execution_receipts", %session, sequence, %reason, %context, "Receipt subscription reset");
                    // Release snapshots and input queues before waiting for a slow reader. A
                    // terminal reset must not be silently discarded while IPC remains connected.
                    drop(state);
                    drop(publications);
                    drop(canonical);
                    drop(canonical_blocks);
                    let reset = json!({"type":"reset", "sessionId":session, "sequence":sequence, "reason":reason, "context":context});
                    if let Ok(message) = SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &reset) {
                        tokio::select! {
                            _ = sink.closed() => {},
                            _ = sink.send(message) => {},
                        }
                    }
                }
                Ok(())
            }
        },
    )?;
    Ok(module)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Block, BlockBody as Body, Header, Receipt, TxLegacy};
    use alloy_primitives::{Address, Signature, U256};
    use op_alloy_consensus::{OpReceipt, OpTypedTransaction};
    use reth_optimism_primitives::OpTransactionSigned;
    use reth_primitives_traits::RecoveredBlock;

    fn snapshot(
        number: u64,
        parent: B256,
        ids: &[u64],
        publication_id: u64,
        canonical: bool,
    ) -> Snapshot {
        let txs = ids
            .iter()
            .map(|&nonce| {
                OpTransactionSigned::new_unhashed(
                    OpTypedTransaction::Legacy(TxLegacy {
                        nonce,
                        gas_limit: 21_000,
                        ..Default::default()
                    }),
                    Signature::new(U256::from(1), U256::from(2), false),
                )
            })
            .collect();
        let block = Block {
            header: Header { number, parent_hash: parent, ..Default::default() },
            body: Body { transactions: txs, ..Default::default() },
        };
        let receipts = ids
            .iter()
            .enumerate()
            .map(|(i, _)| {
                OpReceipt::Legacy(Receipt {
                    status: true.into(),
                    cumulative_gas_used: (i as u64 + 1) * 21_000,
                    logs: Vec::new(),
                })
            })
            .collect();
        Snapshot {
            data: BlockAndReceipts::new(
                Arc::new(RecoveredBlock::new_unhashed(block, vec![Address::ZERO; ids.len()])),
                Arc::new(receipts),
            ),
            canonical,
            computed_hash: canonical,
            publication_id,
        }
    }

    fn sequence() -> ReceiptSequence {
        ReceiptSequence::new(BlockNumHash::new(99, B256::ZERO))
    }

    #[test]
    fn cumulative_pending_only_emits_new_receipts() {
        let mut s = sequence();
        assert_eq!(
            event_positions(&s.pending(snapshot(100, B256::ZERO, &[1, 2], 1, false)).unwrap()),
            vec![("apply", 100, 0, 2)]
        );
        assert_eq!(
            event_positions(
                &s.pending(snapshot(100, B256::ZERO, &[1, 2, 3, 4], 2, false)).unwrap()
            ),
            vec![("apply", 100, 2, 4)]
        );
    }

    #[test]
    fn equal_transaction_hashes_do_not_hide_execution_mismatch() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1], 1, false)).unwrap();
        let mut full = snapshot(100, B256::ZERO, &[1], 0, true);
        Arc::make_mut(&mut full.data.receipts)[0] = OpReceipt::Legacy(Receipt {
            status: false.into(),
            cumulative_gas_used: 21_000,
            logs: Vec::new(),
        });
        assert_eq!(s.canonical(full).unwrap_err(), "execution_mismatch");
    }

    #[test]
    fn reordered_transactions_reset_even_when_receipts_match() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1, 2], 1, false)).unwrap();
        assert_eq!(
            s.canonical(snapshot(100, B256::ZERO, &[2, 1], 0, true)).unwrap_err(),
            "execution_mismatch"
        );
    }

    #[test]
    fn pending_prefix_changes_reset() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1, 2], 1, false)).unwrap();
        assert_eq!(
            s.pending(snapshot(100, B256::ZERO, &[1, 3], 2, false)).unwrap_err(),
            "pending_prefix_changed"
        );
    }

    #[test]
    fn uncomputed_parent_needs_canonical_confirmation() {
        let mut s = sequence();
        let parent = snapshot(100, B256::ZERO, &[1], 1, false);
        s.pending(parent.clone()).unwrap();
        assert!(s.pending(snapshot(101, parent.hash(), &[2], 2, false)).unwrap().is_empty());
        let mut canonical = parent;
        canonical.canonical = true;
        assert_eq!(
            event_positions(&s.canonical(canonical).unwrap()),
            vec![("seal", 100, 1, 1), ("apply", 101, 0, 1)]
        );
    }

    #[test]
    fn mismatching_child_parent_never_bypasses_ordering() {
        let mut s = sequence();
        let mut parent = snapshot(100, B256::ZERO, &[1], 1, false);
        parent.computed_hash = true;
        s.pending(parent.clone()).unwrap();
        assert!(s.pending(snapshot(101, B256::repeat_byte(7), &[2], 2, false)).unwrap().is_empty());
        parent.canonical = true;
        assert_eq!(s.canonical(parent).unwrap_err(), "pending_parent_changed");
    }

    #[test]
    fn extending_parent_after_child_was_published_resets() {
        let mut s = sequence();
        let mut parent = snapshot(100, B256::ZERO, &[1], 1, false);
        parent.computed_hash = true;
        s.pending(parent.clone()).unwrap();
        s.pending(snapshot(101, parent.hash(), &[2], 2, false)).unwrap();
        assert_eq!(
            s.pending(snapshot(100, B256::ZERO, &[1, 3], 3, false)).unwrap_err(),
            "published_parent_changed"
        );
    }

    #[test]
    fn latest_snapshot_can_overtake_queued_older_publications() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1, 2, 3], 3, false)).unwrap();
        assert!(s.pending(snapshot(100, B256::ZERO, &[1], 1, false)).unwrap().is_empty());
        assert!(s.pending(snapshot(100, B256::ZERO, &[1, 2], 2, false)).unwrap().is_empty());
        assert_eq!(s.emitted[&100].len(), 3);
    }

    #[test]
    fn receipt_count_mismatch_is_terminal() {
        let mut s = sequence();
        let mut p = snapshot(100, B256::ZERO, &[1], 1, false);
        p.data.receipts = Arc::new(Vec::new());
        assert_eq!(s.pending(p).unwrap_err(), "receipt_count_mismatch");
    }

    #[test]
    fn empty_canonical_blocks_advance_the_anchor() {
        let mut s = sequence();
        let empty = snapshot(100, B256::ZERO, &[], 0, true);
        assert_eq!(
            event_positions(&s.canonical(empty.clone()).unwrap()),
            vec![("seal", 100, 0, 0)]
        );
        assert_eq!(
            event_positions(&s.pending(snapshot(101, empty.hash(), &[1], 1, false)).unwrap()),
            vec![("apply", 101, 0, 1)]
        );
    }

    #[test]
    fn changed_canonical_anchor_resets() {
        let mut s = sequence();
        assert_eq!(
            s.canonical(snapshot(99, B256::ZERO, &[], 0, true)).unwrap_err(),
            "canonical_reorg"
        );
        assert_eq!(
            s.canonical(snapshot(100, B256::repeat_byte(1), &[], 0, true)).unwrap_err(),
            "canonical_parent_mismatch"
        );
    }

    fn read_from_blocks(
        blocks: &[Snapshot],
        anchor: BlockNumHash,
    ) -> eyre::Result<Option<Snapshot>> {
        read_canonical_successor(
            anchor,
            |number| Ok(blocks.iter().find(|b| b.number() == number).map(Snapshot::hash)),
            |hash| {
                blocks
                    .iter()
                    .find(|b| b.hash() == hash)
                    .cloned()
                    .ok_or_else(|| eyre::eyre!("canonical_block_unavailable"))
            },
        )
    }

    #[test]
    fn backfill_gap_larger_than_pending_buffer_preserves_receipt_order() {
        let anchor = snapshot(99, B256::ZERO, &[], 0, true);
        let mut state = ReceiptSequence::new(BlockNumHash::new(99, anchor.hash()));
        let mut parent = anchor.hash();
        let mut blocks = Vec::new();
        for number in 100..=168 {
            let block = snapshot(number, parent, &[number, number + 1], 0, true);
            parent = block.hash();
            blocks.push(block);
        }

        // One transaction of the first block and a pending child are already known.
        let mut events = state.pending(snapshot(100, anchor.hash(), &[100], 1, false)).unwrap();
        assert!(
            state.pending(snapshot(101, blocks[0].hash(), &[101], 2, false)).unwrap().is_empty()
        );
        let notification = blocks.last().unwrap().clone();
        let stored = std::iter::once(anchor).chain(blocks.iter().cloned()).collect::<Vec<_>>();
        assert_eq!(state.canonical(notification.clone()).unwrap_err(), "canonical_gap");

        // Canonical input skips 68 predecessors, as happens when backfill writes straight to DB.
        while state.needs_recovery(Some(&notification), None).unwrap() {
            let next = read_from_blocks(&stored, state.anchor).unwrap().unwrap();
            assert_eq!(next.number(), state.anchor.number + 1);
            events.extend(state.canonical(next).unwrap());
        }
        events.extend(state.canonical(notification).unwrap());

        let mut receipts = Vec::new();
        let mut seals = Vec::new();
        for event in &events {
            match event {
                Event::Apply(delta) => receipts.extend(
                    (delta.start..delta.snapshot.len()).map(|i| (delta.snapshot.number(), i)),
                ),
                Event::Seal(seal) => {
                    assert_eq!(receipts.iter().filter(|(n, _)| *n == seal.block_number).count(), 2);
                    seals.push(seal.block_number);
                }
            }
        }
        assert_eq!(receipts, (100..=168).flat_map(|n| [(n, 0), (n, 1)]).collect::<Vec<_>>());
        assert_eq!(seals, (100..=168).collect::<Vec<_>>());
        // Delayed canonical notifications and stale pending data cannot replay anything.
        for block in blocks {
            assert!(state.canonical(block).unwrap().is_empty());
        }
        assert!(state.pending(snapshot(101, B256::ZERO, &[999], 3, false)).unwrap().is_empty());
    }

    #[test]
    fn recovery_rejects_changed_anchor_and_disconnected_successor() {
        let anchor = snapshot(99, B256::ZERO, &[], 0, true);
        assert_eq!(
            read_from_blocks(std::slice::from_ref(&anchor), BlockNumHash::new(99, B256::ZERO))
                .unwrap_err()
                .to_string(),
            "canonical_anchor_changed"
        );

        let position = BlockNumHash::new(99, anchor.hash());
        assert!(read_from_blocks(std::slice::from_ref(&anchor), position).unwrap().is_none());
        let next = snapshot(100, B256::repeat_byte(1), &[], 0, true);
        assert_eq!(
            read_from_blocks(&[anchor, next], position).unwrap_err().to_string(),
            "canonical_parent_mismatch"
        );
    }

    #[test]
    fn recovery_requires_block_and_receipts_and_checks_pending_execution() {
        let anchor = snapshot(99, B256::ZERO, &[], 0, true);
        let position = BlockNumHash::new(99, anchor.hash());
        let next = snapshot(100, anchor.hash(), &[1, 2], 0, true);
        let hashes = |n| Ok(Some(if n == 99 { anchor.hash() } else { next.hash() }));
        for reason in ["canonical_block_unavailable", "canonical_receipts_unavailable"] {
            assert_eq!(
                read_canonical_successor(position, hashes, |_| eyre::bail!(reason))
                    .unwrap_err()
                    .to_string(),
                reason
            );
        }
        let mut incomplete = next.clone();
        incomplete.data.receipts = Arc::new(vec![]);
        let recovered =
            read_canonical_successor(position, hashes, |_| Ok(incomplete)).unwrap().unwrap();
        let mut state = ReceiptSequence::new(position);
        assert_eq!(state.canonical(recovered).unwrap_err(), "receipt_count_mismatch");
        assert_eq!(state.anchor, position);
        state.pending(snapshot(100, anchor.hash(), &[3], 1, false)).unwrap();
        let recovered = read_from_blocks(&[anchor, next], position).unwrap().unwrap();
        assert_eq!(state.canonical(recovered).unwrap_err(), "execution_mismatch");
        assert_eq!(state.anchor, position);
    }

    #[test]
    fn recovery_detects_a_branch_change_during_reads() {
        let anchor = snapshot(99, B256::ZERO, &[], 0, true);
        let next = snapshot(100, anchor.hash(), &[], 0, true);
        let position = BlockNumHash::new(99, anchor.hash());
        for changed_height in [99, 100] {
            let mut reads = 0;
            let result = read_canonical_successor(
                position,
                |n| {
                    reads += 1;
                    Ok(Some(if reads > 2 && n == changed_height {
                        B256::repeat_byte(7)
                    } else if n == 99 {
                        anchor.hash()
                    } else {
                        next.hash()
                    }))
                },
                |_| Ok(next.clone()),
            );
            assert_eq!(
                result.unwrap_err().to_string(),
                if changed_height == 99 {
                    "canonical_anchor_changed"
                } else {
                    "canonical_successor_changed"
                }
            );
        }
    }

    #[test]
    fn recovery_is_bounded_and_only_fills_predecessors() {
        let state = sequence();
        let next = snapshot(100, B256::ZERO, &[], 0, true);
        // Process a contiguous notification before a pending child's later recovery target.
        assert_eq!(state.needs_recovery(Some(&next), Some(200)), Ok(false));
        let boundary = state.anchor.number + MAX_RECOVERY_BLOCKS;
        assert_eq!(state.needs_recovery(None, Some(boundary)), Ok(true));
        assert_eq!(state.needs_recovery(None, Some(boundary + 1)), Err("canonical_recovery_limit"));
        let next = snapshot(boundary + 1, B256::ZERO, &[], 0, true);
        assert_eq!(state.needs_recovery(Some(&next), None), Ok(true));
        let too_far = snapshot(boundary + 2, B256::ZERO, &[], 0, true);
        assert_eq!(state.needs_recovery(Some(&too_far), None), Err("canonical_recovery_limit"));
        assert_eq!(state.needs_recovery(None, Some(99)), Ok(false));
    }

    #[test]
    fn recovery_preserves_empty_block_seals() {
        let anchor = snapshot(99, B256::ZERO, &[], 0, true);
        let first = snapshot(100, anchor.hash(), &[], 0, true);
        let second = snapshot(101, first.hash(), &[], 0, true);
        let mut state = ReceiptSequence::new(BlockNumHash::new(99, anchor.hash()));
        let recovered = read_from_blocks(&[anchor, first], state.anchor).unwrap().unwrap();
        assert_eq!(
            event_positions(&state.canonical(recovered).unwrap()),
            vec![("seal", 100, 0, 0)]
        );
        assert_eq!(event_positions(&state.canonical(second).unwrap()), vec![("seal", 101, 0, 0)]);
    }

    #[test]
    fn waiting_blocks_have_a_hard_bound() {
        let mut s = sequence();
        for i in 0..MAX_BLOCKS {
            assert!(
                s.pending(snapshot(200 + i as u64, B256::ZERO, &[], i as u64 + 1, false))
                    .unwrap()
                    .is_empty()
            );
        }
        assert_eq!(
            s.pending(snapshot(999, B256::ZERO, &[], 100, false)).unwrap_err(),
            "buffer_limit"
        );
    }
    #[test]
    fn replacing_a_snapshot_at_capacity_does_not_take_another_slot() {
        let mut s = sequence();
        let mut parent = B256::ZERO;
        for i in 0..MAX_BLOCKS {
            let mut p = snapshot(100 + i as u64, parent, &[i as u64], i as u64 + 1, false);
            p.computed_hash = true;
            parent = p.hash();
            s.pending(p).unwrap();
        }
        let mut replacement = s.emitted.last_key_value().unwrap().1.clone();
        replacement.publication_id += 1;
        assert!(s.pending(replacement).unwrap().is_empty());
        assert_eq!(s.emitted.len(), MAX_BLOCKS);
        assert_eq!(
            s.pending(snapshot(100 + MAX_BLOCKS as u64, parent, &[99], 100, false)).unwrap_err(),
            "buffer_limit"
        );
    }

    #[test]
    fn retained_receipts_still_obey_the_total_byte_budget() {
        let mut s = sequence();
        for i in 0..2 {
            let mut p = snapshot(200 + i, B256::ZERO, &[i], i + 1, false);
            let OpReceipt::Legacy(receipt) = &mut Arc::make_mut(&mut p.data.receipts)[0] else {
                unreachable!()
            };
            receipt.logs.push(alloy_primitives::Log::new_unchecked(
                Address::ZERO,
                Vec::new(),
                vec![0_u8; MAX_BYTES / 2].into(),
            ));
            assert!(p.bytes() < MAX_BYTES);
            if i == 0 {
                assert!(s.pending(p).unwrap().is_empty());
            } else {
                assert_eq!(s.pending(p).unwrap_err(), "buffer_limit");
            }
        }
    }

    fn event_positions(events: &[Event<Delta>]) -> Vec<(&'static str, u64, usize, usize)> {
        events
            .iter()
            .map(|event| match event {
                Event::Apply(d) => ("apply", d.snapshot.number(), d.start, d.snapshot.len()),
                Event::Seal(s) => {
                    ("seal", s.block_number, s.transaction_count, s.transaction_count)
                }
            })
            .collect()
    }

    #[test]
    fn subscription_accepts_no_options() {
        for raw in [None, Some("[]")] {
            assert!(jsonrpsee_types::Params::new(raw).parse::<Option<[Value; 0]>>().is_ok());
        }
        for raw in [r#"[{"version":2}]"#, r#"[{"resume":10}]"#, "[{}]", "[null]", "{}", "[1,2]"] {
            assert_eq!(
                jsonrpsee_types::Params::new(Some(raw))
                    .parse::<Option<[Value; 0]>>()
                    .unwrap_err()
                    .code(),
                -32602,
                "{raw}"
            );
        }
    }

    #[test]
    fn repairs_then_seals_before_releasing_child() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1], 1, false)).unwrap();
        let parent = snapshot(100, B256::ZERO, &[1, 2], 0, true);
        let parent_hash = parent.hash();
        assert!(s.pending(snapshot(101, parent_hash, &[3], 2, false)).unwrap().is_empty());

        let events = s.canonical(parent.clone()).unwrap();
        assert_eq!(
            event_positions(&events),
            vec![("apply", 100, 1, 2), ("seal", 100, 2, 2), ("apply", 101, 0, 1)]
        );
        let Event::Seal(seal) = &events[1] else { panic!("missing seal") };
        assert_eq!(seal.block_hash, parent_hash);
        assert_eq!(seal.parent_hash, B256::ZERO);
        assert!(s.canonical(parent).unwrap().is_empty());
    }

    #[test]
    fn full_pending_only_needs_one_seal() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1, 2], 1, false)).unwrap();
        let parent = snapshot(100, B256::ZERO, &[1, 2], 0, true);
        assert_eq!(
            event_positions(&s.canonical(parent.clone()).unwrap()),
            vec![("seal", 100, 2, 2)]
        );
        assert!(s.canonical(parent).unwrap().is_empty());
    }

    #[test]
    fn empty_canonical_blocks_are_explicit_and_contiguous() {
        let mut s = sequence();
        let empty = snapshot(100, B256::ZERO, &[], 0, true);
        let empty_hash = empty.hash();
        assert_eq!(event_positions(&s.canonical(empty).unwrap()), vec![("seal", 100, 0, 0)]);
        let next = snapshot(101, empty_hash, &[], 0, true);
        let events = s.canonical(next).unwrap();
        assert_eq!(event_positions(&events), vec![("seal", 101, 0, 0)]);
        let Event::Seal(seal) = &events[0] else { panic!("missing seal") };
        assert_eq!(seal.parent_hash, empty_hash);
    }

    #[test]
    fn seal_after_speculative_child_does_not_replay_child() {
        let mut s = sequence();
        let mut parent = snapshot(100, B256::ZERO, &[1], 1, false);
        parent.computed_hash = true;
        s.pending(parent.clone()).unwrap();
        assert_eq!(
            event_positions(&s.pending(snapshot(101, parent.hash(), &[2], 2, false)).unwrap()),
            vec![("apply", 101, 0, 1)]
        );
        parent.canonical = true;
        assert_eq!(event_positions(&s.canonical(parent).unwrap()), vec![("seal", 100, 1, 1)]);
        assert_eq!(s.emitted[&101].len(), 1);
    }

    #[test]
    fn zero_transaction_pending_has_context_without_repeated_empty_updates() {
        let mut s = sequence();
        let mut parent = snapshot(100, B256::ZERO, &[], 1, false);
        parent.computed_hash = true;
        assert_eq!(
            event_positions(&s.pending(parent.clone()).unwrap()),
            vec![("apply", 100, 0, 0)]
        );
        parent.publication_id = 2;
        assert!(s.pending(parent.clone()).unwrap().is_empty());
        assert_eq!(
            event_positions(&s.pending(snapshot(101, parent.hash(), &[1], 3, false)).unwrap()),
            vec![("apply", 101, 0, 1)]
        );
        parent.canonical = true;
        assert_eq!(event_positions(&s.canonical(parent).unwrap()), vec![("seal", 100, 0, 0)]);
    }

    #[test]
    fn missing_block_is_applied_and_sealed_before_child() {
        let mut s = sequence();
        let parent = snapshot(100, B256::ZERO, &[1, 2], 0, true);
        assert!(s.pending(snapshot(101, parent.hash(), &[3], 1, false)).unwrap().is_empty());
        assert_eq!(
            event_positions(&s.canonical(parent).unwrap()),
            vec![("apply", 100, 0, 2), ("seal", 100, 2, 2), ("apply", 101, 0, 1)]
        );
    }

    #[test]
    fn conflict_cannot_confirm_or_release_child() {
        let mut s = sequence();
        s.pending(snapshot(100, B256::ZERO, &[1], 1, false)).unwrap();
        let mut parent = snapshot(100, B256::ZERO, &[1], 0, true);
        Arc::make_mut(&mut parent.data.receipts)[0] = OpReceipt::Legacy(Receipt {
            status: false.into(),
            cumulative_gas_used: 21_000,
            logs: Vec::new(),
        });
        s.pending(snapshot(101, parent.hash(), &[2], 2, false)).unwrap();
        assert_eq!(s.canonical(parent).unwrap_err(), "execution_mismatch");
        assert_eq!(s.anchor.number, 99);
        assert!(!s.emitted.contains_key(&101));
    }

    #[test]
    fn wire_events_keep_order_and_do_not_nest_apply_fields() {
        let events = vec![
            Event::Apply(Update {
                block_number: 100,
                parent_hash: B256::ZERO,
                block_hash: B256::repeat_byte(1),
                source: "canonical",
                from_transaction_index: 2,
                receipts: vec![json!({"transactionIndex":"0x2","logs":[]})],
            }),
            Event::Seal(Seal {
                block_number: 100,
                block_hash: B256::repeat_byte(1),
                parent_hash: B256::ZERO,
                transaction_count: 3,
            }),
        ];
        let response = ReceiptNotification { session_id: "session", sequence: 1, events: &events };
        let wire = serde_json::to_value(&response).unwrap();
        assert_eq!(
            wire,
            json!({"type":"batch","sessionId":"session","sequence":1,"events":[
                {"type":"apply","blockNumber":100,"parentHash":B256::ZERO,"blockHash":B256::repeat_byte(1),
                 "source":"canonical","fromTransactionIndex":2,"receipts":[{"transactionIndex":"0x2","logs":[]}]},
                {"type":"seal","blockNumber":100,"blockHash":B256::repeat_byte(1),"parentHash":B256::ZERO,"transactionCount":3}
            ]})
        );
    }

    #[test]
    fn every_pending_prefix_reconciles_without_replaying_receipts() {
        for parent_len in 0..=3 {
            for child_len in 0..=3 {
                for publish_parent in [false, true] {
                    let mut state = sequence();
                    let parent = snapshot(100, B256::ZERO, &[1, 2, 3], 0, true);
                    let child = snapshot(101, parent.hash(), &[4, 5, 6], 0, true);
                    let mut events = Vec::new();
                    if publish_parent {
                        let p = snapshot(100, B256::ZERO, &[1, 2, 3][..parent_len], 1, false);
                        events.extend(state.pending(p.clone()).unwrap());
                        assert!(state.pending(p).unwrap().is_empty());
                    }
                    let p = snapshot(101, parent.hash(), &[4, 5, 6][..child_len], 2, false);
                    events.extend(state.pending(p.clone()).unwrap());
                    assert!(state.pending(p).unwrap().is_empty());
                    events.extend(state.canonical(parent.clone()).unwrap());
                    events.extend(state.canonical(child.clone()).unwrap());
                    assert!(state.canonical(parent).unwrap().is_empty());
                    assert!(state.canonical(child).unwrap().is_empty());
                    assert!(
                        state.pending(snapshot(101, B256::ZERO, &[], 3, false)).unwrap().is_empty()
                    );
                    let mut positions = Vec::new();
                    let mut seals = Vec::new();
                    for event in events {
                        match event {
                            Event::Apply(delta) => positions.extend(
                                (delta.start..delta.snapshot.len())
                                    .map(|i| (delta.snapshot.number(), i)),
                            ),
                            Event::Seal(seal) => {
                                assert_eq!(
                                    positions
                                        .iter()
                                        .filter(|(n, _)| *n == seal.block_number)
                                        .count(),
                                    seal.transaction_count
                                );
                                seals.push(seal.block_number);
                            }
                        }
                    }
                    assert_eq!(
                        positions,
                        vec![(100, 0), (100, 1), (100, 2), (101, 0), (101, 1), (101, 2)]
                    );
                    assert_eq!(seals, vec![100, 101]);
                }
            }
        }
    }
}
