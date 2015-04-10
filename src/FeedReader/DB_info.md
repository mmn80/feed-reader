Memory Data Structures
----------------------

The following "Haskell" is just pseudocode.
For indexes, instead of functions and lists we will have IntMap, IntSet, Seq, etc.
For data files, we are just describing the format.
`with X lock` means `bracket` on `take MVar` / `put MVar` on `X`.

A `newtype` based `Handle` will contain `Master` and `Data`.
The data inside will be under `MVar`s.
The handle will be created by an `open` IO action, and closed by a `close` one.
You can use then whatever brackets you need with that.

### Master

```haskell
transLog :: TID -> [(DID, Addr, Size, Del, [(refTag, DID)])]
freeSlots :: Size -> [Addr]
fwdIdx :: DID -> [(TID, Addr, Size, [(refTag, DID)])]
bckIdx :: refTag -> DID -> [DID]
logHandle, logNewHandle :: Handle
```

### Data

```haskell
dataHandle :: Handle
jobs :: [(TID, [(DID, ByteString, Addr, Size)])]
```

Data Files Format
-----------------

### Transaction Log File

```haskell
tp  :: Addr
recs :: [TRec]
```
```
TRec :: Pending: TID, DID, Addr, Size, Del, RefCount, [(refTag, DID)]
      | Completed: TID
```

### Data File

```haskell
recs :: [(ByteString, FreeSpace)]
```

Initialization
--------------

All meta data resides in `Master` and is built incrementally from the `recs` in the transaction log.
If log file size > `tp`, file is truncated (failed transaction).
In such cases, the size of the data file is also checked and maybe truncated.

Transaction Monad
-----------------

### Primitive ops

```haskell
lookup :: DID a -> Trans (Maybe a)
insert :: a -> Trans (DID a)
update :: DID a -> a -> Trans ()
delete :: DID a -> Trans ()
```

Also range/page queries, and queries on the `bckIdx`.

### Execution

Pure read queries will be directly executed in the `middle` section.
They only take a lock during `begin` in order to fix a `tid`,
and then just query, potentially asynchronously, with `TID <= tid`.
If some `TID`s are not found, abort.
Of course, the data file handle will be under lock during each read.

See: https://en.wikipedia.org/wiki/Multiversion_concurrency_control

```haskell
execTrans :: Trans a -> IO a
```
```
begin:
  with Master lock:
    fix a TID
    grab a ref to Master
  init update list :: [(DID, ByteString, Deleted)]
  init read list :: [DID]
  put all this in the state monad
middle:
  execute lookups either in the in-mem ref (in case only IDs are queried),
    or directly in the DB, based on the TID in the state monad (with lock on Data)
  collect lookups to the read list
  collect updates to the update list, after serialization
end:
  with Master lock:
    check new transactions in Master:
      if they contain updated/deleted DIDs that clash with read list, abort
      if they contain deleted DIDs that clash with update list, abort
      if they contain updated DIDs that clash with update list, abort or ignore
        based on policy
    generate TID
    allocate memory and update Master
    write to transaction log:
      increase file size
      write records
      tp := file size
  with Data lock:
    add update list and new TID as job to the async update manager
```

Update Manager
--------------

```
for each job:
  with Data lock:
    increase file size if needed
    write all updates
  with Master lock:
    add "Completed: TID" to the transaction log
```

Garbage Collector
-----------------

GC runs asynchronously, and can be executed at any time.

```
with Master lock:
  grab Master ref
  fix a TID
collect garbage from Master
with logNewHandle lock:
  write new log (new file)
with Master lock:
  with logNewHandle lock:
    copy any new transactions from old log
  update Master
  rename log files
  switch transaction log file handle to the fresh one
```
