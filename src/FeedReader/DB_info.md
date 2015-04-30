Invariants
----------

- All locks last at most **O(log n)**.
This includes "big" procedures, like GC, that never "stop the world".
- All meta data (incl. indexes) reside in memory in a `MasterState` structure.
- All meta data is built incrementally, in **O(log n)** steps,
from the transaction log (no backtracking).
- No meta data resides in the data file, just serialized user "records".
- No serialized user data is saved in the transaction log file, just keys, addresses and sizes.
- All primitive transactions in the user facing API are **O(log n)**.
- Index management is fully automatic, online (*O(log n)* with no rebuild steps), and hidden.

Memory Data Structures
----------------------

The following "Haskell" is just pseudocode.
Instead of functions and lists we will have `Data.IntMap`, `Data.IntSet`, etc.

A `newtype` based `Handle` wraps a `DBState` ADT that contains `master :: MVar MasterState` and `dataHandle`.
The handle is created by an `open` IO action, used for running transaction monad actions, and closed by a `close` IO action.

### Master State

`intIdx` indexes columns that hold Int-convertible values, like Date/Time.
It is used for range queries.

`refIdx` indexes document reference columns.
`[DID]` is sorted on the `PropID` in the pair.
There can be multiple copies for the same main `PropID`, but sorted on different columns.
Used for filter+range queries (filter of first prop, then range on second).

The flag `keepTrans` is set during GC, so as the Update Manager will no longer clear `logComp`.
`logPend` and `logComp` contain pending and completed transactions.

```haskell
logHandle :: Handle
keepTrans :: Bool
logPend   :: TID -> [(DID, Addr, Size, Del, [(PropID, [DID])])]
logComp   :: TID -> [(DID, Addr, Size, Del, [(PropID, [DID])])]
gaps      :: Size -> [Addr]
mainIdx   :: DID -> [(TID, Addr, Size, [(PropID, [DID])])]
intIdx    :: PropID -> IntVal -> [DID]
refIdx    :: PropID -> [(PropID, DID -> [DID])]
```

### Data State

```haskell
dataHandle :: MVar Handle
```

Data Files Format
-----------------

`TID`, `DID`, `Addr`, `PropID`, `Del` and `Size` are all aliases of `DBWord`, a newtype wrapper around `Word32`.
`DBWord` has a custom serializer that enforces Big Endianess.

The pseudo-Haskell represents just a byte sequence in lexical order.

### Transaction Log File

The transaction log is just a sequence of `DBWord`s, which makes it portable.
The tags for `Pending` and `Completed` in the sum type are also `DBWord`.
Any ordered subset of a valid log is a valid log.

```haskell
logPos :: Addr
recs   :: [TRec]

TRec = Pending TID DID Addr Size Del ValCount [(PropID, IntVal)] RefCount [(PropID, DID)]
     | Completed TID
```

### Data File

The data file is a sequence of `ByteString`s produced by the `Serialize` instance of user data types interspersed with gaps (old records orphaned by the GC).

```haskell
recs :: [(ByteString, Gap)]
```

Initialization
--------------

All meta data resides in `master`, which is built incrementally from the transaction log.
Each `DBWord` in the log leads to an *O(log(n))* `master` update operation.
No "wrapping up" needs to be performed at the end.
`master` remains consistent at every step.

Transaction Monad
-----------------

A State monad over IO that holds inside a `Handle`, a `TID`, a read list, and an update list.

Reads are executed live, while updates are just accumulated in the list.
The transaction is written in the log at the end, and contains the updated DID list.
Only this step is under lock.

While the record data is asynchronously written by the Update Manager,
other transactions can check the DID list of pending & completed transactions
and abort themselves in case of conflict.

Read queries target a specific version, `TID <= tid`, and are not blocked by writes.
Nevertheless, actual file access is under lock.

See: https://en.wikipedia.org/wiki/Multiversion_concurrency_control

### Primitive ops

```haskell
lookup :: DID a -> Trans (Maybe a)
insert :: a -> Trans (DID a)
update :: DID a -> a -> Trans ()
delete :: DID a -> Trans ()
```

Also range/page queries, and queries on the `intIdx`.

### Running Transactions

```haskell
runTrans :: Handle -> Trans a -> IO (Maybe a)

ReadList   = [DID]
UpdateList = [(DocRecord, ByteString)]

```
```
begin:
  with master lock:
    tid <- generate TID
  init update list
  init read list
middle (the part users write):
  execute index lookups in-memory (with master lock and TID <= tid)
  execute document lookups in the DB (with data lock and TID <= tid)
  execute other user IO
  collect lookups to the read list
  collect updates to the update list
end:
  with master lock:
    check new transactions in master (both pending and completed):
      if they contain updated/deleted DIDs that clash with read list, abort
      if they contain deleted DIDs that clash with update list, abort
      if they contain updated DIDs that clash with update list, abort or ignore
        based on policy
    allocate space and update gaps accordingly
    add new transaction based on the update list to logPend
    update logPos, logSize
    logPos' := logPos + trans size
    write to transaction log:
      increase file size if < logPos'
      write records
      logPos := logPos'
```

Update Manager
--------------

We use an **ACId** with *'eventual durability'* model.
If the program dies before the Update Manager (asynchronously) finished
processing `logPend`, the `logPos` will never be updated (last operation, see below).
Next time the program starts the incomplete transaction will simply be ignored.
Since `gaps` are always rebuild from valid log data,
the previously allocated slots will become available again.

```
repeat:
  with master lock:
    get first (by TID) job from logPend
  with dataHandle lock:
    increase file size if needed
    write updates
  with master lock:
    remove transaction from logPend
    if keepTrans or new pending transactions exist, add it to logComp
    if logPend is empty and not keepTrans, empty logComp
    update mainIdx, intIdx
    add "Completed: TID" to the transaction log
    update logPos in the transaction log
```

Garbage Collector
-----------------

GC runs asynchronously, and can be executed at any time.

```
with master lock:
  grab master ref
  keepTrans = True
collect garbage from grabbed master
reallocate records contiguously (empty gaps)
rebuild mainIdx, intIdx, refIdx
write new log file
write new data file
with master lock:
  write new transactions to new log, update the new empty gaps index
  update mainIdx, intIdx, refIdx from new logComp
  keepTrans = False
  close old log file handle
  rename new log to old name
  update log file handle in master
  with data lock:
    similarly rename & switch the data file
```
