# IPC execution receipts

This subscription delivers executed pending receipts and canonical repairs in transaction order.
It is registered only on IPC when Flashblocks is enabled. Connect a Unix stream socket to the
node's configured IPC path (on GIWA, `/data/giwa/data/execution/reth.ipc`).

## Subscribe and initialize

Subscribe with no options:

```json
{"jsonrpc":"2.0","id":1,"method":"eth_subscribeExecutionReceipts","params":[]}
```

Nonempty or named parameters are rejected with invalid params (`-32602`). The response contains a
subscription ID. Notifications use `eth_executionReceipts`; their `params` contain `subscription`
and `result`. Newline-delimited requests work, and responses/notifications end with a newline.
Buffer reads until a complete line arrives: a read may contain a partial message or several messages.

The first notification result is:

```json
{
  "type":"ready",
  "sessionId":"...",
  "sequence":0,
  "chainId":91342,
  "genesisHash":"0x...",
  "anchor":{"blockNumber":100,"blockHash":"0x..."}
}
```

Chain and block values above are illustrative. Check `chainId` and `genesisHash` before applying
receipts. Initialize state at the exact anchor block hash while buffering subsequent notifications.
An existing snapshot must belong to this chain and have a canonical block hash; replay historical
blocks up to the anchor before applying the buffered stream. Do not initialize against a moving
`latest` tag. No receipts at or below the anchor are delivered by the subscription.

The anchor is locally canonical, not necessarily safe or finalized. The node subscribes to input
events before reading the anchor so initialization does not leave a gap. Sequence zero is `ready`;
later `batch` and `reset` notifications increment it by one within this subscription. Sequence is
not a resume cursor or a simulation-state identifier.

## Apply receipts and confirm blocks

Each batch contains an ordered array of events:

```json
{
  "type":"batch",
  "sessionId":"...",
  "sequence":1,
  "events":[
    {
      "type":"apply",
      "blockNumber":101,
      "parentHash":"0x...",
      "blockHash":"0x...",
      "source":"canonical",
      "fromTransactionIndex":20,
      "receipts":["full OP receipt objects; placeholder here"]
    },
    {
      "type":"seal",
      "blockNumber":101,
      "blockHash":"0x...",
      "parentHash":"0x...",
      "transactionCount":30
    }
  ]
}
```

`apply` adds receipts starting at `fromTransactionIndex`. Process events in array order and receipts
in transaction order. Receipts include failed transactions and transactions without logs; do not
renumber after filtering. Metadata numbers are JSON integers; standard receipt quantities use hex.
`source` is `pending` or `canonical`. Pending block hashes are provisional.

Only the newly executed suffix of a cumulative pending build is emitted. A build combining several
Flashblocks emits their receipts together; upstream index boundaries are not preserved. The first
zero-transaction pending snapshot emits an empty apply with block context. Repeated empty snapshots
do not repeat that event. There is no deliberate delay to accumulate a larger batch.

`seal` confirms that a block is locally canonical and all previously emitted execution effects match
it. It carries the final block hash and total transaction count. The client must have applied exactly
that many receipts for this block. Seals follow canonical block order, including empty blocks and
blocks already fully delivered through pending. Duplicate canonical input does not repeat a seal or
previously emitted receipts. A seal is not safe/finalized finality and does not prevent a later reorg.

Missing parent receipts are emitted before the parent's seal, then any waiting child is released in
the same batch. A child can be emitted earlier only if the delivered parent's locally computed full
header hash matches the child's parent hash. Its later parent seal does not replay or rewind the
child. A consecutive set of Flashblocks indices alone does not certify a complete parent.
Readable canonical parents are fetched directly instead of waiting for their event notification.

Track canonical confirmation separately from the latest applied pending position. If later pending
effects are already in memory, a seal does not make that entire memory state a snapshot of the sealed
block. Do not overwrite a valid block snapshot with state containing later pending effects.

The node updates RPC pending before broadcasting its publication. A simulation using `pending` may
observe a newer revision than the notification being processed. This subscription does not pin
simulation state; canonical repairs do not restore an old pending revision.

## Errors and recovery

A terminal notification looks like:

```json
{"type":"reset","sessionId":"...","sequence":2,"reason":"execution_mismatch"}
```

A reset ends the subscription. Reasons include conflicting execution, changed parent/prefix,
canonical reorg, input-stream lag or closure, and buffer limits. Stop using affected state for
trading. Delivery of a reset is best-effort if the connection is unwritable or already disconnected.
An unexpected sequence, changed session, or subscription termination also requires reconciliation.

The server retains no session replay and accepts no resume cursor. On reconnect, subscribe again
and buffer the new stream in a bounded queue. Its anchor may have advanced past previously delivered
pending receipts; fetch complete historical receipts to bridge that gap.

A client that retains memory through a transport interruption can compare its applied ordered
transaction/receipt prefix against fresh cumulative or canonical data, then apply only the missing
suffix. Compare execution effects, not provisional block hashes; transaction hashes alone are
insufficient. Only advance the processed sequence after consuming the complete notification.

If execution conflicts or the canonical branch changes, discard the affected state. A client can
exit and restart from a valid canonical block snapshot instead of implementing online rollback.
Verify the snapshot block hash on restart; an orphaned snapshot requires an older valid checkpoint
or rebuilding. After a process restart that loses memory, an old sequence number cannot restore it.

## Resource limits and unsubscribe

Publication input uses a bounded 64-event broadcast queue; overflow is detected instead of silently
coalescing messages. Each subscription retains at most 64 pending block snapshots with a 64 MiB
block-and-receipt memory budget, excluding transport and serialization buffers. Socket enqueue has
a 2-second timeout. Slow consumers do not block Flashblocks execution. IPC message-size and connection
limits also apply. Use a private, permission-controlled IPC socket.

Unsubscribe normally:

```json
{"jsonrpc":"2.0","id":2,"method":"eth_unsubscribeExecutionReceipts","params":["SUBSCRIPTION_ID"]}
```
