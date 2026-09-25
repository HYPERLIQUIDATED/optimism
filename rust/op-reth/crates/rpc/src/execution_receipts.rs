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
use reth_storage_api::{BlockReader, BlockReaderIdExt, ReceiptProvider, TransactionVariant};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;

const MAX_BLOCKS: usize = 64;
const MAX_BYTES: usize = 64 * 1024 * 1024;
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
        if number != self.anchor.number + 1 || snapshot.parent() != self.anchor.hash {
            return Err("canonical_gap_or_reorg");
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
                let spec = eth.provider().chain_spec();
                let ready = json!({
                    "type":"ready", "sessionId":session, "sequence":sequence,
                    "chainId":spec.chain_id(), "genesisHash":spec.genesis_hash(),
                    "anchor":{"blockNumber":anchor.number,"blockHash":anchor.hash}});
                let ready = SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &ready)?;
                if sink.send_timeout(ready, SEND_TIMEOUT).await.is_err() { return Ok(()); }
                let result: Result<(), String> = async {
                    loop {
                        let mut deltas;
                        if let Some(pending) = first.take() {
                            deltas = state.pending(pending)?;
                        } else {
                            tokio::select! {
                                _ = sink.closed() => return Ok(()),
                                event = publications.recv() => {
                                    let pending = event.map_err(|e| match e {
                                        broadcast::error::RecvError::Lagged(_) => "publication_gap",
                                        broadcast::error::RecvError::Closed => "publication_stream_closed",
                                    })?;
                                    deltas = state.pending(Snapshot::pending(&pending))?;
                                }
                                event = canonical.recv() => {
                                    let event = event.map_err(|_| "canonical_stream_gap")?;
                                    if event.reverted().is_some() { return Err("canonical_reorg".into()); }
                                    deltas = Vec::new();
                                    for (block, receipts) in event.committed().blocks_and_receipts() {
                                        let snapshot = Snapshot { data: BlockAndReceipts::new(block.clone(), Arc::new(receipts.clone())),
                                            canonical: true, computed_hash: true, publication_id: 0 };
                                        deltas.extend(state.canonical(snapshot)?);
                                    }
                                }
                            }
                        }
                        // A canonical parent can already be readable before its notification is
                        // delivered. Repair now instead of waiting for another subscription event.
                        if let Some((&waiting, _)) = state.waiting.last_key_value() {
                            let from = state.anchor.number + 1;
                            let to = waiting.saturating_sub(1);
                            if to >= from {
                                if to - from >= MAX_BLOCKS as u64 { return Err("buffer_limit".into()); }
                                let provider = eth.provider().clone();
                                let blocks = tokio::task::spawn_blocking(move || -> eyre::Result<Vec<Snapshot>> {
                                    let Some(head) = provider.latest_header()? else { return Ok(Vec::new()); };
                                    let mut blocks = Vec::new();
                                    for number in from..=to.min(head.number()) {
                                        let block = provider.recovered_block(number.into(), TransactionVariant::WithHash)?
                                            .ok_or_else(|| eyre::eyre!("canonical block unavailable"))?;
                                        let receipts = provider.receipts_by_block(block.hash().into())?
                                            .ok_or_else(|| eyre::eyre!("canonical receipts unavailable"))?;
                                        blocks.push(Snapshot { data: BlockAndReceipts::new(Arc::new(block), Arc::new(receipts)),
                                            canonical: true, computed_hash: true, publication_id: 0 });
                                    }
                                    Ok(blocks)
                                }).await.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
                                for block in blocks { deltas.extend(state.canonical(block)?); }
                            }
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
                    // Release snapshots and input queues before waiting for a slow reader. A
                    // terminal reset must not be silently discarded while IPC remains connected.
                    drop(state);
                    drop(publications);
                    drop(canonical);
                    let reset = json!({"type":"reset", "sessionId":session, "sequence":sequence, "reason":reason});
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
            "canonical_gap_or_reorg"
        );
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
