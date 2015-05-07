{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}

-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.DocDB
-- Copyright : (C) 2015 Călin Ardelean,
-- License : BSD-style (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides the database backend for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.DocDB
  ( Handle
  , open
  , close
  , performGC
  , Transaction
  , runTransaction
  , DocID
  , IntVal
  , Property
  , utcTime2IntVal
  , string2IntVal
  , DocRefList (..)
  , IntValList (..)
  , Document (..)
  , lookup
  , lookupUnsafe
  , insert
  , update
  , delete
  , deleteUnsafe
  , range
  , rangeUnsafe
  , rangeK
  , rangeKUnsafe
  , filter
  , filterUnsafe
  , size
  , debug
  ) where

import           Control.Concurrent    (MVar, ThreadId, forkIO, newMVar,
                                        putMVar, takeMVar, threadDelay)
import           Control.Exception     (bracket, bracketOnError)
import           Control.Monad         (forM, forM_, liftM, replicateM, unless,
                                        when)
import           Control.Monad.State   (StateT)
import qualified Control.Monad.State   as S
import           Control.Monad.Trans   (MonadIO (liftIO))
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import           Data.Function         (on)
import           Data.Hashable         (hash)
import           Data.IntMap.Strict    (IntMap)
import qualified Data.IntMap.Strict    as Map
import           Data.IntSet           (IntSet)
import qualified Data.IntSet           as Set
import qualified Data.List             as L
import           Data.Maybe            (fromJust, fromMaybe)
import           Data.Serialize        (Serialize (..), decode, encode)
import           Data.Serialize.Get    (getWord32be)
import           Data.Serialize.Put    (putWord32be)
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime, getCurrentTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Typeable         (Proxy (..), Typeable, typeRep)
import           Data.Word             (Word32, Word8)
import           FeedReader.Cache      (LRUCache)
import qualified FeedReader.Cache      as Cache
import           Numeric               (showHex)
import           Prelude               hiding (filter, lookup)
import           System.Directory      (renameFile)
import           System.FilePath       ((</>))
import qualified System.IO             as IO

newtype DBWord = DBWord { unDBWord :: Word32 }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral)

instance Serialize DBWord where
  put = putWord32be . unDBWord
  get = DBWord <$> getWord32be

instance Show DBWord where
  show (DBWord d) = show d

type Addr     = DBWord
type TID      = DBWord
type DID      = DBWord
type IntValue = DBWord
type Size     = DBWord
type PropID   = DBWord

data DocReference = DocReference
  { drefPID :: !PropID
  , drefDID :: !DID
  } deriving (Show)

data IntReference = IntReference
  { irefPID :: !PropID
  , irefVal :: !IntValue
  } deriving (Show)

data DocRecord = DocRecord
  { docID    :: !DID
  , docTID   :: !TID
  , docIRefs :: ![IntReference]
  , docDRefs :: ![DocReference]
  , docAddr  :: !Addr
  , docSize  :: !Size
  , docDel   :: !Bool
  } deriving (Show)

type SortIndex = IntMap (IntMap IntSet)

type FilterIndex = IntMap (IntMap SortIndex)

data MasterState = MasterState
  { logHandle :: IO.Handle
  , logPos    :: !Addr
  , logSize   :: !Size
  , newTID    :: !TID
  , keepTrans :: !Bool
  , gaps      :: !(IntMap [Addr])
  , logPend   :: !(IntMap [(DocRecord, ByteString)])
  , logComp   :: !(IntMap [DocRecord])
  , mainIdx   :: !(IntMap [DocRecord])
  , intIdx    :: !SortIndex
  , refIdx    :: !FilterIndex
  }

data DataState = DataState
  { dataHandle :: IO.Handle
  , dataCache  :: !LRUCache
  }

data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

data DBState = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , masterState  :: MVar MasterState
  , dataState    :: MVar DataState
  , updateMan    :: MVar Bool
  , gcState      :: MVar GCState
  }

instance Eq DBState where
  s == s' = logFilePath s == logFilePath s'

data TRec = Pending DocRecord | Completed TID

-- External Types --------------------------------------------------------------

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle s) = logFilePath s

data TransactionState = TransactionState
  { transHandle     :: Handle
  , transTID        :: TID
  , transReadList   :: [DID]
  , transUpdateList :: [(DocRecord, ByteString)]
  }

newtype Transaction m a = Transaction { unTransaction :: StateT TransactionState m a }
  deriving (Functor, Applicative, Monad)

instance MonadIO m => MonadIO (Transaction m) where
  liftIO = Transaction . liftIO

-- Properties ------------------------------------------------------------------

newtype Property a = Property { unProperty :: (PropID, String) }

instance Eq (Property a) where
  Property (pid, _) == Property (pid', _) = pid == pid'

instance Show (Property a) where
  show (Property (pid, s)) = s ++ "[" ++ show pid ++ "]"

instance Typeable a => IsString (Property a) where
  fromString s = Property (pid, s)
    where pid = fromIntegral $ hash (show $ typeRep (Proxy :: Proxy a), s)

-- Values ----------------------------------------------------------------------

newtype IntVal = IntVal { unIntVal :: DID }
  deriving (Eq, Ord, Bounded, Num)

instance Show (IntVal) where show (IntVal k) =  "0x" ++ showHex k ""

data IntValList a = forall b. IntValList { intPropName :: Property a
                                         , intPropVals :: [IntVal] }

newtype DocID a = DocID { unDocID :: DID }
  deriving (Eq, Ord, Bounded, Serialize)

instance Show (DocID a) where show (DocID k) = "0x" ++ showHex k ""

data DocRefList a = forall b. DocRefList { docPropName :: Property a
                                         , docPropVals :: [DocID b] }

-- Records ---------------------------------------------------------------------

class (Typeable a, Serialize a) => Document a where
  getRefProps :: [Property a]
  getRefProps = []
  getDocRefs  :: a -> [DocRefList a]
  getDocRefs _ = []
  getIntProps :: [Property a]
  getIntProps = []
  getIntVals  :: a -> [IntValList a]
  getIntVals _ = []

utcTime2IntVal :: UTCTime -> IntVal
utcTime2IntVal = fromInteger . round . utcTimeToPOSIXSeconds

string2IntVal :: String -> IntVal
string2IntVal s = IntVal . snd $ L.foldl' f (wordSize - 1, 0) bytes
  where bytes = (fromIntegral . fromEnum <$> take wordSize s) :: [Word8]
        f (n, v) b = (n - 1, if n >= 0 then v + fromIntegral b * 2 ^ (8 * n) else v)

open :: MonadIO m => Maybe FilePath -> Maybe FilePath -> m Handle
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  lfh <- liftIO $ IO.openBinaryFile logPath IO.ReadWriteMode
  dfh <- liftIO $ IO.openBinaryFile datPath IO.ReadWriteMode
  liftIO $ IO.hSetBuffering lfh IO.NoBuffering
  liftIO $ IO.hSetBuffering dfh IO.NoBuffering
  (pos, lsz) <- readLogPos lfh
  let m = MasterState { logHandle = lfh
                      , logPos    = pos
                      , logSize   = lsz
                      , newTID    = 1
                      , keepTrans = False
                      , gaps      = emptyGaps 0
                      , logPend   = Map.empty
                      , logComp   = Map.empty
                      , mainIdx   = Map.empty
                      , intIdx    = Map.empty
                      , refIdx    = Map.empty
                      }
  m' <- if (pos > 0) && (toInt lsz > wordSize)
        then readLog m 0
        else return m
  let m'' = m' { gaps = updateGaps m' }
  mv <- liftIO $ newMVar m''
  let d = DataState { dataHandle = dfh
                    , dataCache  = Cache.empty 0x100000 (0x100000 * 10) 60
                    }
  dv <- liftIO $ newMVar d
  um <- liftIO $ newMVar False
  gc <- liftIO $ newMVar IdleGC
  let h = Handle DBState { logFilePath  = logPath
                         , dataFilePath = datPath
                         , masterState  = mv
                         , dataState    = dv
                         , updateMan    = um
                         , gcState      = gc
                         }
  liftIO . forkIO $ updateManThread h True
  liftIO . forkIO $ gcThread h
  return h

close :: MonadIO m => Handle -> m ()
close h = do
  withGC h . const $ return (KillGC, ())
  withUpdateMan h . const $ return (True, ())
  withMasterLock h $ \m -> IO.hClose (logHandle m)
  withDataLock   h $ \(DataState d _) -> IO.hClose d

performGC :: MonadIO m => Handle -> m ()
performGC h = withGC h . const $ return (PerformGC, ())

runTransaction :: MonadIO m => Handle -> Transaction m a -> m (Maybe a)
runTransaction h (Transaction t) = do
  tid <- mkNewTID h
  (a, q, u) <- runUserCode tid
  if null u then return $ Just a
  else withMaster h $ \m ->
    if checkValid tid (logPend m) (logComp m) q u then do
      let tsz = sum $ (tRecSize . Pending . fst) <$> u
      let pos = toInt (logPos m) + tsz
      lsz <- checkLogSize (logHandle m) (toInt (logSize m)) pos
      let (ts, gs) = L.foldl' allocFold ([], gaps m) u
      let m' = m { logPos  = fromIntegral pos
                 , logSize = fromIntegral lsz
                 , gaps    = gs
                 , logPend = Map.insert (toInt tid) ts $ logPend m
                 }
      writeTransactions m ts
      writeLogPos (logHandle m) $ fromIntegral pos
      return (m', Just a)
    else return (m, Nothing)
  where
    runUserCode tid = do
      (a, TransactionState _ _ q u) <- S.runStateT t
        TransactionState
          { transHandle     = h
          , transTID        = tid
          , transReadList   = []
          , transUpdateList = []
          }
      let u' = L.nubBy ((==) `on` docID . fst) u
      return (a, q, u')

    checkValid tid logp logc q u = ck qs && ck us
      where
        us = (\(d,_) -> (toInt (docID d), ())) <$> u
        qs = (\k -> (toInt k, ())) <$> q
        ck lst = Map.null (Map.intersection newPs ml) &&
                 Map.null (Map.intersection newCs ml)
          where ml = Map.fromList lst
        newPs = snd $ Map.split (toInt tid) logp
        newCs = snd $ Map.split (toInt tid) logc

    allocFold (ts, gs) (r, bs) =
      if docDel r then ((r, bs):ts, gs)
      else ((t, bs):ts, gs')
        where (a, gs') = alloc gs $ docSize r
              t = r { docAddr = a }

    writeTransactions m ts = do
      logSeek m
      forM_ ts $ \(t, _) ->
        writeLogTRec (logHandle m) $ Pending t

lookup :: (Document a, MonadIO m) => DocID a -> Transaction m (Maybe (DocID a, a))
lookup = lookupUnsafe . IntVal . unDocID

lookupUnsafe :: (Document a, MonadIO m) => IntVal -> Transaction m (Maybe (DocID a, a))
lookupUnsafe (IntVal did) = Transaction $ do
  t <- S.get
  mbr <- withMasterLock (transHandle t) $ \m ->
           return $ findFirstDoc m t did
  mba <- if null mbr then return Nothing
         else do
           a <- readDocument (transHandle t) (fromJust mbr)
           return $ Just (DocID did, a)
  S.put t { transReadList = did : transReadList t }
  return mba

update :: (Document a, MonadIO m) => DocID a -> a -> Transaction m ()
update did a = Transaction $ do
  t <- S.get
  let bs = encode a
  let r = DocRecord { docID   = unDocID did
                    , docTID  = transTID t
                    , docIRefs = concat $ getIRefs <$> getIntVals a
                    , docDRefs = concat $ getDRefs <$> getDocRefs a
                    , docAddr = 0
                    , docSize = fromIntegral $ B.length bs
                    , docDel  = False
                    }
  S.put t { transUpdateList = (r, bs) : transUpdateList t }
  where
    getDRefs (DocRefList p rs) = (\(DocID k)  -> DocReference (checkRefProp p) k) <$> rs
    getIRefs (IntValList p rs) = (\(IntVal k) -> IntReference (checkIntProp p) k) <$> rs

insert :: (Document a, MonadIO m) => a -> Transaction m (DocID a)
insert a = Transaction $ do
  t <- S.get
  tid <- mkNewTID $ transHandle t
  unTransaction $ update (DocID tid) a
  return $ DocID tid

delete :: (Document a, MonadIO m) => DocID a -> Transaction m ()
delete = deleteUnsafe . IntVal . unDocID

deleteUnsafe :: MonadIO m => IntVal -> Transaction m ()
deleteUnsafe (IntVal did) = Transaction $ do
  t <- S.get
  (addr, sz, irs, drs) <- withMasterLock (transHandle t) $ \m -> return $
    case findFirstDoc m t did of
      Nothing -> (0, 0, [], [])
      Just r  -> (docAddr r, docSize r, docIRefs r, docDRefs r)
  let r = DocRecord { docID    = did
                    , docTID   = transTID t
                    , docIRefs = irs
                    , docDRefs = drs
                    , docAddr  = addr
                    , docSize  = sz
                    , docDel   = True
                    }
  S.put t { transUpdateList = (r, B.empty) : transUpdateList t }

page_ :: (Document a, MonadIO m) => (Int -> MasterState -> [Int]) ->
         Maybe IntVal -> Transaction m [(DocID a, a)]
page_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> do
           let ds = f (ival mdid) m
           let mbds = findFirstDoc m t . fromIntegral <$> ds
           return [ fromJust mb | mb <- mbds, not (null mb) ]
  dds' <- forM (reverse dds) $ \d -> do
    a <- readDocument (transHandle t) d
    return (DocID $ docID d, a)
  S.put t { transReadList = (unDocID . fst <$> dds') ++ transReadList t }
  return dds'

range :: (Document a, MonadIO m) => Maybe IntVal -> Maybe (DocID a) ->
         Property a -> Int -> Transaction m [(DocID a, a)]
range mst msti = rangeUnsafe mst (IntVal . unDocID <$> msti)

rangeUnsafe :: (Document a, MonadIO m) => Maybe IntVal -> Maybe IntVal ->
         Property a -> Int -> Transaction m [(DocID a, a)]
rangeUnsafe mst msti p pg = page_ f mst
  where f st m = fromMaybe [] $ do
                   ds <- Map.lookup (propI p) (intIdx m)
                   return $ getPage st (ival msti) pg ds

filter :: (Document a, MonadIO m) => DocID a -> Maybe IntVal -> Maybe (DocID a) ->
          Property a -> Property a -> Int -> Transaction m [(DocID a, a)]
filter (DocID k) mst msti = filterUnsafe (IntVal k) mst (IntVal . unDocID <$> msti)

filterUnsafe :: (Document a, MonadIO m) => IntVal -> Maybe IntVal -> Maybe IntVal ->
          Property a -> Property a -> Int -> Transaction m [(DocID a, a)]
filterUnsafe (IntVal k) mst msti fprop sprop pg = page_ f mst
  where f _ m = fromMaybe [] . liftM (getPage (ival mst) (ival msti) pg) $
                  Map.lookup (propR fprop) (refIdx m) >>=
                  Map.lookup (toInt k) >>=
                  Map.lookup (propI sprop)

pageK_ :: MonadIO m => (Int -> MasterState -> [Int]) ->
         Maybe IntVal -> Transaction m [DocID a]
pageK_ f mdid = Transaction $ do
  t <- S.get
  dds <- withMasterLock (transHandle t) $ \m -> do
           let ds = f (ival mdid) m
           let mbds = findFirstDoc m t . fromIntegral <$> ds
           return [ docID $ fromJust mb | mb <- mbds, not (null mb) ]
  S.put t { transReadList = dds ++ transReadList t }
  return (DocID <$> dds)

rangeK :: (Document a, MonadIO m) => Maybe IntVal -> Maybe (DocID a) ->
         Property a -> Int -> Transaction m [DocID a]
rangeK mst msti = rangeKUnsafe mst (IntVal . unDocID <$> msti)

rangeKUnsafe :: (Document a, MonadIO m) => Maybe IntVal -> Maybe IntVal ->
         Property a -> Int -> Transaction m [DocID a]
rangeKUnsafe mst msti p pg = pageK_ f mst
  where f st m = fromMaybe [] $ getPage st (ival msti) pg <$>
                                Map.lookup (propI p) (intIdx m)

size :: (Document a, MonadIO m) => Property a -> Transaction m Int
size p = Transaction $ do
  t <- S.get
  withMasterLock (transHandle t) $ \m -> return . fromMaybe 0 $
    (sum . map (Set.size . snd) . Map.toList) <$> Map.lookup (propI p) (intIdx m)

debug :: MonadIO m => Handle -> Bool -> Bool -> m String
debug h sIdx sCache = do
  mstr <- withMasterLock h $ \m -> return $
    "logPos    : "   ++ show (logPos m) ++
    "\nlogSize   : " ++ show (logSize m) ++
    "\nnewTID    : " ++ show (newTID m) ++
    "\nlogPend   :\n  " ++ show (logPend m) ++
    "\nlogComp   :\n  " ++ show (logComp m) ++
    if sIdx then
    "\nmainIdx   :\n  " ++ show (mainIdx m) ++
    "\nintIdx    :\n  " ++ show (intIdx m) ++
    "\nrefIdx    :\n  " ++ show (refIdx m) ++
    "\ngaps      :\n  " ++ show (gaps m)
    else ""
  dstr <- withDataLock h $ \d -> return $
    "\ncacheSize : " ++ show (Cache.cSize $ dataCache d) ++
    if sCache then
    "\ncache     :\n  " ++ show (Cache.cQueue $ dataCache d)
    else ""
  return $ mstr ++ dstr

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

wordSize :: Int
wordSize = 4

withMasterLock :: MonadIO m => Handle -> (MasterState -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar . masterState $ unHandle h)
  (putMVar (masterState $ unHandle h))

withMaster :: MonadIO m => Handle -> (MasterState -> IO (MasterState, a)) -> m a
withMaster h f = liftIO $ bracketOnError
  (takeMVar . masterState $ unHandle h)
  (putMVar (masterState $ unHandle h))
  (\m -> do
    (m', a) <- f m
    putMVar (masterState $ unHandle h) m'
    return a)

withDataLock :: MonadIO m => Handle -> (DataState -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar . dataState $ unHandle h)
  (putMVar (dataState $ unHandle h))

withData :: MonadIO m => Handle -> (DataState -> IO (DataState, a)) -> m a
withData h f = liftIO $ bracketOnError
  (takeMVar . dataState $ unHandle h)
  (putMVar (dataState $ unHandle h))
  (\d -> do
    (d', a) <- f d
    putMVar (dataState $ unHandle h) d'
    return a)

withUpdateMan :: MonadIO m => Handle -> (Bool -> IO (Bool, a)) -> m a
withUpdateMan h f = liftIO $ bracketOnError
  (takeMVar . updateMan $ unHandle h)
  (putMVar (updateMan $ unHandle h))
  (\kill -> do
    (kill', a) <- f kill
    putMVar (updateMan $ unHandle h) kill'
    return a)

withGC :: MonadIO m => Handle -> (GCState -> IO (GCState, a)) -> m a
withGC h f = liftIO $ bracketOnError
  (takeMVar . gcState $ unHandle h)
  (putMVar (gcState $ unHandle h))
  (\s -> do
    (s', a) <- f s
    putMVar (gcState $ unHandle h) s'
    return a)

readWord :: MonadIO m => IO.Handle -> m DBWord
readWord h = do
  bs <- liftIO $ B.hGet h wordSize
  case decode bs of
    Right x  -> return x
    Left err -> logError h err

writeWord :: MonadIO m => IO.Handle -> DBWord -> m ()
writeWord h w = liftIO . B.hPut h $ encode w

readLogPos :: MonadIO m => IO.Handle -> m (Addr, Size)
readLogPos h = do
  sz <- liftIO $ IO.hFileSize h
  if sz >= fromIntegral wordSize then do
    liftIO $ IO.hSeek h IO.AbsoluteSeek 0
    w <- readWord h
    return (w, fromIntegral sz)
  else
    return (0, 0)

writeLogPos :: MonadIO m => IO.Handle -> Addr -> m ()
writeLogPos h p = do
  liftIO $ IO.hSeek h IO.AbsoluteSeek 0
  liftIO $ B.hPut h $ encode p

logError :: MonadIO m => IO.Handle -> String -> m a
logError h err = do
  pos <- liftIO $ IO.hTell h
  liftIO . ioError . userError $ "Corrupted log. " ++ err ++ " Position: " ++ show pos

mkNewTID :: MonadIO m => Handle -> m TID
mkNewTID h = withMaster h $ \m -> return (m { newTID = newTID m + 1 }, newTID m)

updateMainIdx :: IntMap [DocRecord] -> [DocRecord] -> IntMap [DocRecord]
updateMainIdx = L.foldl' f
  where f idx r = let did = toInt (docID r) in
                  let rs' = case Map.lookup did idx of
                              Nothing  -> [r]
                              Just ors -> r : ors in
                  Map.insert did rs' idx

updateIntIdx :: SortIndex -> [DocRecord] -> SortIndex
updateIntIdx = L.foldl' f
  where f idx r = L.foldl' g idx (docIRefs r)
          where
            did = toInt (docID r)
            del = docDel r
            g idx' ref =
                let rpid = toInt (irefPID ref) in
                let rval = toInt (irefVal ref) in
                let sng = Set.singleton did in
                case Map.lookup rpid idx' of
                  Nothing -> if del then idx'
                             else Map.insert rpid (Map.singleton rval sng) idx'
                  Just is -> Map.insert rpid is' idx'
                    where is' = case Map.lookup rval is of
                                  Nothing -> if del then is
                                             else Map.insert rval sng is
                                  Just ss -> Map.insert rval ss' is
                                    where ss' = if del then Set.delete did ss
                                                else Set.insert did ss

updateRefIdx :: FilterIndex -> [DocRecord] -> FilterIndex
updateRefIdx = L.foldl' f
  where f idx r = L.foldl' g idx (docDRefs r)
          where
            did = toInt (docID r)
            del = docDel r
            g idx' ref =
                let rpid = toInt (drefPID ref) in
                let rval = toInt (drefDID ref) in
                let sng = updateIntIdx Map.empty [r] in
                case Map.lookup rpid idx' of
                  Nothing -> if del then idx'
                             else Map.insert rpid (Map.singleton rval sng) idx'
                  Just is -> Map.insert rpid is' idx'
                    where is' = case Map.lookup rval is of
                                  Nothing -> if del then is
                                             else Map.insert rval sng is
                                  Just ss -> Map.insert rval ss' is
                                    where ss' = updateIntIdx ss [r]

emptyGaps :: Addr -> IntMap [Addr]
emptyGaps sz = Map.singleton (toInt $ maxBound - sz) [sz]

addGap :: Size -> Addr -> IntMap [Addr] -> IntMap [Addr]
addGap s addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs
        sz = toInt s

updateGaps :: MasterState -> IntMap [Addr]
updateGaps m = addTail . L.foldl' f (Map.empty, 0) $ L.sortOn docAddr firstD
  where
    firstD = [ r | r:_ <- Map.elems (mainIdx m), not $ docDel r ]
    f (gs, addr) r = (gs', docAddr r + docSize r)
      where gs' = if addr == docAddr r then gs
                  else addGap sz addr gs
            sz = docAddr r - addr
    addTail (gs, addr) = addGap (maxBound - addr) addr gs

alloc :: IntMap [Addr] -> Size -> (Addr, IntMap [Addr])
alloc gs s = let sz = toInt s in
  case Map.lookupGE sz gs of
    Nothing -> error $ "DB full, no more gaps of requested size: " ++ show sz ++ " !"
    Just (gsz, a:as) ->
      if delta == 0 then (a, gs')
      else (a, addGap (fromIntegral delta) (a + fromIntegral sz) gs')
      where gs' = if null as then Map.delete gsz gs
                             else Map.insert gsz as gs
            delta = gsz - sz

toInt :: DBWord -> Int
toInt (DBWord d) = fromIntegral d

checkLogSize :: MonadIO m => IO.Handle -> Int -> Int -> m Int
checkLogSize hnd osz pos =
  let bpos = wordSize * (pos + 1) in
  if bpos > osz then do
    let sz = max bpos $ osz + 4096
    liftIO . IO.hSetFileSize hnd $ fromIntegral sz
    return sz
  else return osz

logSeek :: MonadIO m => MasterState -> m ()
logSeek m = liftIO $ IO.hSeek h IO.AbsoluteSeek p
  where h = logHandle m
        p = fromIntegral $ wordSize * toInt (1 + logPos m)

readLog :: MonadIO m => MasterState -> Int -> m MasterState
readLog m pos = do
  let h = logHandle m
  let l = logPend m
  ln <- readLogTRec h
  m' <- case ln of
          Pending r ->
            let tid = toInt $ docTID r in
            let ntid = max (newTID m) (fromIntegral tid + 1) in
            case Map.lookup tid l of
              Nothing -> return m { newTID  = ntid
                                  , logPend = Map.insert tid [(r, B.empty)] l }
              Just rs -> return m { newTID  = ntid
                                  , logPend = Map.insert tid ((r, B.empty):rs) l }
          Completed tid ->
            case Map.lookup (toInt tid) l of
              Nothing -> logError h $ "Completed TID:" ++ show tid ++
                " found but transaction did not previously occur."
              Just rps -> let rs = fst <$> rps in
                          return m { newTID  = max (newTID m) (tid + 1)
                                   , logPend = Map.delete (toInt tid) l
                                   , mainIdx = updateMainIdx (mainIdx m) rs
                                   , intIdx  = updateIntIdx (intIdx m) rs
                                   , refIdx  = updateRefIdx (refIdx m) rs
                                   }
  let pos' = pos + tRecSize ln
  if pos' >= (fromIntegral (logPos m) - 1) then return m'
  else readLog m' pos'

pndTag :: Int
pndTag = 0x70 -- ASCII 'p'

cmpTag :: Int
cmpTag = 0x63 -- ASCII 'c'

truTag :: Int
truTag = 0x54 -- ASCII 'T'

flsTag :: Int
flsTag = 0x46 -- ASCII 'F'

readLogTRec :: MonadIO m => IO.Handle -> m TRec
readLogTRec h = do
  tag <- readWord h
  case toInt tag of
    x | x == pndTag -> do
      tid <- readWord h
      did <- readWord h
      adr <- readWord h
      siz <- readWord h
      del <- readWord h
      dlb <- case toInt del of
               x | x == truTag  -> return True
               x | x == flsTag -> return False
               _ -> logError h $ "True ('T') or False ('F') tag expected but " ++
                      show del ++ " found."
      irfc <- readWord h
      irfs <- replicateM (toInt irfc) $ do
                rpid <- readWord h
                rval <- readWord h
                return IntReference { irefPID = rpid
                                    , irefVal = rval
                                    }
      drfc <- readWord h
      drfs <- replicateM (toInt drfc) $ do
                rpid <- readWord h
                rdid <- readWord h
                return DocReference { drefPID = rpid
                                    , drefDID = rdid
                                    }
      return $ Pending DocRecord { docID    = did
                                 , docTID   = tid
                                 , docIRefs = irfs
                                 , docDRefs = drfs
                                 , docAddr  = adr
                                 , docSize  = siz
                                 , docDel   = dlb
                                 }
    x | x == cmpTag -> do
      tid <- readWord h
      return $ Completed tid
    _ -> logError h $ "Pending ('p') or Completed ('c') tag expected but " ++
           show tag ++ " found."

writeLogTRec :: MonadIO m => IO.Handle -> TRec -> m ()
writeLogTRec h t =
  case t of
    Pending doc -> do
      writeWord h $ fromIntegral pndTag
      writeWord h $ docTID doc
      writeWord h $ docID doc
      writeWord h $ docAddr doc
      writeWord h $ docSize doc
      writeWord h . fromIntegral $ if docDel doc then truTag else flsTag
      writeWord h . fromIntegral . length $ docIRefs doc
      forM_ (docIRefs doc) $ \r -> do
         writeWord h $ irefPID r
         writeWord h $ irefVal r
      writeWord h . fromIntegral . length $ docDRefs doc
      forM_ (docDRefs doc) $ \r -> do
         writeWord h $ drefPID r
         writeWord h $ drefDID r
    Completed tid -> do
      writeWord h $ fromIntegral cmpTag
      writeWord h tid

tRecSize :: TRec -> Int
tRecSize r = case r of
  Pending dr  -> 8 + (2 * length (docIRefs dr)) + (2 * length (docDRefs dr))
  Completed _ -> 2

ival :: Maybe IntVal -> Int
ival  = toInt . unIntVal . fromMaybe maxBound

propI :: forall a. Document a => Property a -> Int
propI = toInt . checkIntProp

propR :: forall a. Document a => Property a -> Int
propR = toInt . checkRefProp

checkIntProp :: forall a. Document a => Property a -> PropID
checkIntProp p@(Property (pid, _)) =
  if p `notElem` (getIntProps :: [Property a])
  then error $ "Invalid int property name: " ++ show p
  else pid

checkRefProp :: forall a. Document a => Property a -> PropID
checkRefProp p@(Property (pid, _)) =
  if p `notElem` (getRefProps :: [Property a])
  then error $ "Invalid ref property name: " ++ show p
  else pid

readDocument :: (Typeable a, Serialize a, MonadIO m) => Handle -> DocRecord -> m a
readDocument h r =
  withData h $ \(DataState hnd cache) -> do
    now <- getCurrentTime
    let k = toInt $ docAddr r
    case Cache.lookup now k cache of
      Nothing -> do
        bs <- readDocumentFromFile hnd r
        a <- case decode bs of
          Right a  -> return a
          Left err -> liftIO . ioError . userError $ "Deserialization error: " ++ err
        return (DataState hnd $ Cache.insert now k a (B.length bs) cache, a)
      Just (a, _, cache') -> return (DataState hnd cache', a)

readDocumentFromFile :: MonadIO m => IO.Handle -> DocRecord -> m ByteString
readDocumentFromFile hnd r = do
  liftIO . IO.hSeek hnd IO.AbsoluteSeek . fromIntegral $ docAddr r
  liftIO . B.hGet hnd . fromIntegral $ docSize r

writeDocument :: MonadIO m => DocRecord -> ByteString -> IO.Handle -> m ()
writeDocument r bs hnd = unless (docDel r) $ do
  liftIO . IO.hSeek hnd IO.AbsoluteSeek . fromIntegral $ docAddr r
  liftIO $ B.hPut hnd bs

findFirstDoc :: MasterState -> TransactionState -> DID -> Maybe DocRecord
findFirstDoc m t did = do
  let idx = mainIdx m
  let tid = transTID t
  rs <- Map.lookup (toInt did) idx
  r  <- L.find (\r -> docTID r <= tid) rs
  if docDel r then Nothing
  else Just r

getPage :: Int -> Int -> Int -> IntMap IntSet -> [Int]
getPage st sti p idx = go st p []
  where go st p acc =
          if p == 0 then acc
          else case Map.lookupLT st idx of
                 Nothing     -> acc
                 Just (n, is) ->
                   let (p', ids) = getPage2 sti p is in
                   if p' == 0 then ids ++ acc
                   else go n p' $ ids ++ acc

getPage2 :: Int -> Int -> IntSet -> (Int, [Int])
getPage2 st p idx = go st p []
  where go st p acc =
          if p == 0 then (0, acc)
          else case Set.lookupLT st idx of
                 Nothing -> (p, acc)
                 Just a  -> go a (p - 1) (a:acc)

updateManThread :: Handle -> Bool -> IO ()
updateManThread h w = do
  (kill, wait) <- withUpdateMan h $ \kill -> do
    wait <- if kill then return True else do
      when w . threadDelay $ 100 * 1000
      mbj <- withMasterLock h $ \m ->
        let lgp = logPend m in
        if null lgp then return Nothing
        else return . Just $ Map.findMin lgp
      if null mbj then return True
      else do
        let (tid, rs) = fromJust mbj
        withData h $ \(DataState hnd cache) -> do
          let maxAddr = toInteger . maximum $ (\r -> docAddr r + docSize r) . fst <$> rs
          sz <- IO.hFileSize hnd
          when (sz < maxAddr + 1) $ do
            let nsz = max (maxAddr + 1) $ sz + 4096
            IO.hSetFileSize hnd nsz
          forM_ rs $ \(r, bs) -> writeDocument r bs hnd
          let cache' = L.foldl' (\c (r, _) -> Cache.delete (toInt $ docAddr r) c)
                         cache rs
          return (DataState hnd cache', ())
        withMaster h $ \m -> do
          let rs' = fst <$> rs
          let trec = Completed $ fromIntegral tid
          let pos = fromIntegral (logPos m) + tRecSize trec
          lsz <- checkLogSize (logHandle m) (toInt (logSize m)) pos
          logSeek m
          let lh = logHandle m
          writeLogTRec lh trec
          writeLogPos lh $ fromIntegral pos
          let (lgp, lgc) = updateLog tid (keepTrans m) (logPend m) (logComp m)
          let m' = m { logPos  = fromIntegral pos
                     , logSize = fromIntegral lsz
                     , logPend = lgp
                     , logComp = lgc
                     , mainIdx = updateMainIdx (mainIdx m) rs'
                     , intIdx  = updateIntIdx (intIdx m) rs'
                     , refIdx  = updateRefIdx (refIdx m) rs'
                     }
          return (m', null lgp)
    return (kill, (kill, wait))
  unless kill $ updateManThread h wait
  where updateLog tid keep lgp lgc = (lgp', lgc')
          where ors  = map fst . fromMaybe [] $ Map.lookup tid lgp
                lgp' = Map.delete tid lgp
                lc   = if not keep && null lgp'
                       then Map.empty else lgc
                lgc' = if keep || not (null lgp')
                       then Map.insert tid ors lc else lc

gcThread :: Handle -> IO ()
gcThread h = do
  sgn <- withGC h $ \sgn -> do
    when (sgn == PerformGC) $ do
      om <- withMaster h $ \m -> return (m { keepTrans = True }, m)
      let rs = map head . L.filter (not . any docDel) . Map.elems $ mainIdx om
      let (rs2, dpos) = realloc 0 rs
      let rs' = fst <$> rs2
      let ts = concat $ toTRecs <$> L.groupBy ((==) `on` docTID) rs'
      let pos = sum $ tRecSize <$> ts
      let logPath = logFilePath (unHandle h)
      let logPathNew = logPath ++ ".new"
      sz <- IO.withBinaryFile logPathNew IO.ReadWriteMode $ writeTrans 0 pos ts
      let dataPath = dataFilePath (unHandle h)
      let dataPathNew = dataPath ++ ".new"
      IO.withBinaryFile dataPathNew IO.ReadWriteMode $ writeData rs2 dpos h
      let mIdx = updateMainIdx Map.empty rs'
      let iIdx = updateIntIdx Map.empty rs'
      let rIdx = updateRefIdx Map.empty rs'
      when (forceEval mIdx iIdx rIdx) $ withUpdateMan h $ \kill -> do
        withMaster h $ \nm -> do
          let (ncrs', dpos') = realloc dpos $ concat <$> splitNew om $ logComp nm
          let (logp', dpos'') = realloc' dpos' $ logPend nm
          let ncrs = fst <$> ncrs'
          (pos', sz') <- if null ncrs then return (pos, sz)
                         else IO.withBinaryFile logPathNew IO.ReadWriteMode $ \hnd -> do
                                let newts = toTRecs ncrs
                                let pos' = pos + sum (tRecSize <$> newts)
                                sz' <- writeTrans sz pos' newts hnd
                                return (pos', sz')
          IO.hClose $ logHandle nm
          renameFile logPathNew logPath
          hnd <- IO.openBinaryFile logPath IO.ReadWriteMode
          IO.hSetBuffering hnd IO.NoBuffering
          IO.withBinaryFile dataPathNew IO.ReadWriteMode $ writeData ncrs' dpos'' h
          let gs = buildGaps dpos'' . L.filter docDel $
                     ncrs ++ (map fst . concat $ Map.elems logp')
          let m = MasterState { logHandle = hnd
                              , logPos    = fromIntegral pos'
                              , logSize   = fromIntegral sz'
                              , newTID    = newTID nm
                              , keepTrans = False
                              , gaps      = gs
                              , logPend   = logp'
                              , logComp   = Map.empty
                              , mainIdx   = updateMainIdx mIdx ncrs
                              , intIdx    = updateIntIdx  iIdx ncrs
                              , refIdx    = updateRefIdx  rIdx ncrs
                              }
          return (m, ())
        withData h $ \(DataState hnd cache) -> do
          IO.hClose hnd
          renameFile dataPathNew dataPath
          hnd' <- IO.openBinaryFile dataPath IO.ReadWriteMode
          IO.hSetBuffering hnd' IO.NoBuffering
          return (DataState hnd' cache, ())
        return (kill, ())
    let sgn' = if sgn == PerformGC then IdleGC else sgn
    return (sgn', sgn')
  unless (sgn == KillGC) $ do
    threadDelay $ 1000 * 1000
    gcThread h
  where
    splitNew m = Map.elems . snd . Map.split (toInt $ newTID m)

    toTRecs rs = (Pending <$> rs) ++ [Completed . docTID $ head rs]

    writeTrans osz pos ts hnd = do
      sz <- checkLogSize hnd osz pos
      IO.hSeek hnd IO.AbsoluteSeek $ fromIntegral wordSize
      forM_ ts $ writeLogTRec hnd
      writeLogPos hnd $ fromIntegral pos
      return sz

    writeData rs sz h hnd = do
      IO.hSetFileSize hnd $ fromIntegral sz
      forM_ rs $ \(r, oldr) -> do
        bs <- withDataLock h $ \(DataState hnd _) -> readDocumentFromFile hnd oldr
        writeDocument r bs hnd

    realloc st = L.foldl' f ([], st)
      where f (nrs, pos) r =
              if docDel r then ((r, r) : nrs, pos)
              else ((r { docAddr = pos }, r) : nrs, pos + docSize r)

    realloc' st idx = (Map.fromList l, pos)
      where (l, pos) = L.foldl' f ([], st) $ Map.toList idx
            f (lst, pos) (tid, rs) = ((tid, rs') : lst, pos')
              where (rss', pos') = realloc pos $ fst <$> rs
                    rs' = (fst <$> rss') `zip` (snd <$> rs)

    buildGaps pos = L.foldl' f (emptyGaps pos)
      where f gs r = addGap (docSize r) (docAddr r) gs

    forceEval mIdx iIdx rIdx = Map.notMember (-1) mIdx &&
                               Map.size iIdx > (-1) &&
                               Map.size rIdx > (-1)
