Memory Data Structures
----------------------
All meta data and indexes reside in memory in a `MasterState` structure.

The following "Haskell" is just pseudocode.
Instead of functions and lists we will have `Data.IntMap`, `Data.IntSet`, etc.

A `newtype` based `Handle` wraps a `DBState` ADT that contains `master :: MVar MasterState`, `dataHandle` and `jobs`.
The handle is created by an `open` IO action, used for running transaction monad actions, and closed by a `close` IO action.

### Master State

```haskell
transLog  :: TID -> [(DID, Addr, Size, Del, [(PropID, DID)])]
gaps      :: Size -> [Addr]
fwdIdx    :: DID -> [(TID, Addr, Size, [(PropID, DID)])]
bckIdx    :: PropID -> DID -> [DID]
logHandle :: Handle
```

### Data State

```haskell
dataHandle :: MVar Handle
jobs       :: MVar [(TID, [(DID, ByteString, Addr, Size)])]
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

TRec = Pending TID DID Addr Size Del RefCount [(PropID, DID)]
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

While the record data is asynchronously written by the Update Manager, other transactions can check the DID list of pending transactions and abort themselves in case of conflict.

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

Also range/page queries, and queries on the `bckIdx`.

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
    check new transactions in master:
      if they contain updated/deleted DIDs that clash with read list, abort
      if they contain deleted DIDs that clash with update list, abort
      if they contain updated DIDs that clash with update list, abort or ignore
        based on policy
    allocate space and update gaps accordingly
    update logPos, logSize, transLog
    logPos' := logPos + trans size
    write to transaction log:
      increase file size if < logPos'
      write records
      logPos := logPos'
  with jobs lock:
    add update list and new TID as job
```

Update Manager
--------------

```
repeat:
  with jobs lock:
    get job from list
  with dataHandle lock:
    increase file size if needed
    write updates
  with master lock:
    update transLog, fwdIdx, bckIdx
    add "Completed: TID" to the transaction log
    update logPos in the transaction log
```

Garbage Collector
-----------------

GC runs asynchronously, and can be executed at any time.

```
with master lock:
  grab master ref
  fix a TID
collect garbage from master
write new log (new file)
with master lock:
  grab master ref
write new transactions to new log
with master lock:
  write new transactions to new log
  update master (rebuild gaps)
  rename log files
  switch transaction log file handle to the fresh one
with dataHandle lock:
  truncate file if too big
```
