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
  , ExtID
  , Property
  , utcTime2ExtID
  , DocRefList (..)
  , ExtRefList (..)
  , Document (..)
  , lookup
  , insert
  , update
  , delete
  , page
  , size
  , debug
  ) where

import           Control.Arrow         (first)
import           Control.Concurrent    (MVar, ThreadId, forkIO, newMVar,
                                        putMVar, takeMVar, threadDelay)
import           Control.Exception     (bracket, bracketOnError)
import           Control.Monad         (forM, forM_, replicateM, unless, when)
import qualified Control.Monad.State   as S
import           Control.Monad.Trans   (MonadIO (liftIO))
import qualified Data.ByteString       as B
import           Data.Function         (on)
import           Data.Hashable         (hash)
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import           Data.List             (find, foldl', nubBy, sortOn)
import           Data.Maybe            (fromJust, fromMaybe)
import qualified Data.Sequence         as Seq
import           Data.Serialize        (Serialize (..), decode, encode)
import           Data.Serialize.Get    (getWord32be)
import           Data.Serialize.Put    (putWord32be)
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Typeable         (Proxy (..), Typeable, typeRep)
import           Data.Word             (Word32)
import           Numeric               (showHex)
import           Prelude               hiding (lookup)
import           System.FilePath       ((</>))
import qualified System.IO             as IO

newtype DBWord = DBWord { unDBWord :: Word32 }
  deriving (Eq, Ord, Bounded, Num, Enum, Real, Integral)

instance Serialize DBWord where
  put = putWord32be . unDBWord
  get = DBWord <$> getWord32be

instance Show DBWord where
  show (DBWord d) = show d

type Addr = DBWord
type TID  = DBWord
type DID  = DBWord
type Size = DBWord
type PropID = DBWord

data Reference = Reference
  { propID :: PropID
  , refDID :: DID
  } deriving (Show)

data DocRecord = DocRecord
  { docID   :: DID
  , docTID  :: TID
  , docRefs :: [Reference]
  , docAddr :: Addr
  , docSize :: Size
  , docDel  :: Bool
  } deriving (Show)

data MasterState = MasterState
  { logHandle :: IO.Handle
  , logPos    :: Addr
  , logSize   :: Size
  , newTID    :: TID
  , gaps      :: Map.IntMap [Addr]
  , transLog  :: Map.IntMap [DocRecord]
  , fwdIdx    :: Map.IntMap [DocRecord]
  , bckIdx    :: Map.IntMap (Map.IntMap Set.IntSet)
  }

type Job = (TID, [(DocRecord, B.ByteString)])

data GCState = IdleGC | PerformGC | KillGC deriving (Eq)

data DBState = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , master       :: MVar MasterState
  , dataHandle   :: MVar IO.Handle
  , jobs         :: MVar [Job]
  , updateMan    :: MVar Bool
  , gcState      :: MVar GCState
  }

instance Eq DBState where
  s == s' = logFilePath s == logFilePath s'

data TRec = Pending DocRecord | Completed TID

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle s) = logFilePath s

data TransactionState = TransactionState
  { transHandle     :: Handle
  , transTID        :: TID
  , transReadList   :: [DID]
  , transUpdateList :: [(DocRecord, B.ByteString)]
  }

newtype Transaction m a = Transaction { unTransaction :: S.StateT TransactionState m a }
  deriving (Functor, Applicative, Monad)

instance MonadIO m => MonadIO (Transaction m) where
  liftIO = Transaction . liftIO

newtype DocID a = DocID { unDocID :: DID }
  deriving (Eq, Ord, Bounded, Serialize)

instance Show (DocID a) where show (DocID k) = "0x" ++ showHex k ""

data DocRefList a = forall b. DocRefList { docPropName :: Property a
                                         , docPropVals :: [DocID b]
                                         }

newtype ExtID a = ExtID { unExtID :: DID }
  deriving (Eq, Ord, Bounded, Num)

instance Show (ExtID a) where show (ExtID k) =  "0x" ++ showHex k ""

data ExtRefList a = forall b. ExtRefList { extPropName :: Property a
                                         , extPropVals :: [ExtID b]
                                         }

newtype Property a = Property { unProperty :: String }
  deriving (Eq, Show, IsString)

class (Typeable a, Serialize a) => Document a where
  getDocRefs :: a -> [DocRefList a]
  getExtRefs :: a -> [ExtRefList a]

utcTime2ExtID :: UTCTime -> ExtID a
utcTime2ExtID = fromInteger . round . utcTimeToPOSIXSeconds

open :: MonadIO m => Maybe FilePath -> Maybe FilePath -> m Handle
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  lfh <- liftIO $ IO.openBinaryFile logPath IO.ReadWriteMode
  dfh <- liftIO $ IO.openBinaryFile datPath IO.ReadWriteMode
  (pos, lsz) <- readLogPos lfh
  let m = MasterState { logHandle = lfh
                      , logPos    = pos
                      , logSize   = lsz
                      , newTID    = 1
                      , gaps      = Map.singleton (toInt (maxBound :: Addr)) [0]
                      , transLog  = Map.empty
                      , fwdIdx    = Map.empty
                      , bckIdx    = Map.empty
                      }
  m' <- if (pos > 0) && (toInt lsz > wordSize)
        then readLog m 0
        else return m
  let m'' = m' { gaps = updateGaps m' }
  mv <- liftIO $ newMVar m''
  dv <- liftIO $ newMVar dfh
  jv <- liftIO $ newMVar []
  um <- liftIO $ newMVar False
  gc <- liftIO $ newMVar IdleGC
  let h = Handle DBState { logFilePath  = logPath
                         , dataFilePath = datPath
                         , master       = mv
                         , dataHandle   = dv
                         , jobs         = jv
                         , updateMan    = um
                         , gcState      = gc
                         }
  liftIO $ forkIO $ updateManThread h True
  liftIO $ forkIO $ gcThread h
  return h

close :: MonadIO m => Handle -> m ()
close h = do
  withGC h $ const $ return (KillGC, ())
  withUpdateMan h $ const $ return (True, ())
  withMasterLock h $ \m -> IO.hClose (logHandle m)
  withDataLock   h $ \d -> IO.hClose d

performGC :: MonadIO m => Handle -> m ()
performGC h = withGC h $ const $ return (PerformGC, ())

runTransaction :: MonadIO m => Handle -> Transaction m a -> m (Maybe a)
runTransaction h (Transaction t) = do
  tid <- mkNewTID h
  (a, q, u) <- runUserCode tid
  if null u then return $ Just a
  else do
    (mba, ts) <- withMaster h $ \m ->
      if checkValid tid (transLog m) q u then do
        let tsz = sum $ (tRecSize . Pending . fst) <$> u
        let pos = toInt (logPos m) + tsz
        lsz <- checkLogSize m pos
        let (ts, gs) = foldl' allocFold ([], gaps m) u
        let m' = m { logPos   = fromIntegral pos
                   , logSize  = fromIntegral lsz
                   , gaps     = gs
                   , transLog = Map.insert (toInt tid) (fst <$> ts) $ transLog m
                   }
        writeTransactions m ts
        writeLogPos (logHandle m) $ fromIntegral pos
        return (m', (Just a, ts))
      else return (m, (Nothing, []))
    unless (null ts) $ withJobs h $ \js -> return ((tid, ts):js, ())
    return mba
  where
    runUserCode tid = do
      (a, TransactionState _ _ q u) <- S.runStateT t
        TransactionState
          { transHandle     = h
          , transTID        = tid
          , transReadList   = []
          , transUpdateList = []
          }
      let u' = nubBy ((==) `on` docID . fst) u
      return (a, q, u')

    checkValid tid l q u = ck qs && ck us
      where
        us = (\(d,_) -> (toInt (docID d), ())) <$> u
        qs = (\k -> (toInt k, ())) <$> q
        ck lst = Map.null $ Map.intersection newTs $ Map.fromList lst
        (_, newTs) = Map.split (toInt tid) l

    allocFold (ts, gs) (r, bs) =
      if docDel r then ((r, bs):ts, gs)
      else ((t, bs):ts, gs')
        where (a, gs') = alloc gs $ docSize r
              t = r { docAddr = a }

    writeTransactions m ts = do
      logSeek m
      forM_ ts $ \(t, _) ->
        writeLogTRec (logHandle m) $ Pending t

lookup :: forall a m. (Document a, MonadIO m) => ExtID a ->
                      Transaction m (Maybe (DocID a, a))
lookup (ExtID did) = Transaction $ do
  t <- S.get
  mbr <- withMasterLock (transHandle t) $ \m ->
           return $ findFirstDoc (fwdIdx m) (transTID t) did
  mba <- if null mbr then return Nothing
         else Just <$> readDocument (transHandle t) (fromJust mbr)
  S.put t { transReadList = did : transReadList t }
  return mba

update :: forall a m. (Document a, MonadIO m) => DocID a -> a -> Transaction m ()
update did a = Transaction $ do
  t <- S.get
  let bs = encode a
  let r = DocRecord { docID   = unDocID did
                    , docTID  = transTID t
                    , docRefs = getRefsConv a
                    , docAddr = 0
                    , docSize = fromIntegral $ B.length bs
                    , docDel  = False
                    }
  S.put t { transUpdateList = (r, bs) : transUpdateList t }

insert :: forall a m. (Document a, MonadIO m) => a -> Transaction m (DocID a)
insert a = Transaction $ do
  t <- S.get
  tid <- mkNewTID $ transHandle t
  unTransaction $ update (DocID tid) a
  return $ DocID tid

delete :: forall a m. (Document a, MonadIO m) => DocID a -> Transaction m ()
delete (DocID did) = Transaction $ do
  t <- S.get
  (addr, sz) <- withMasterLock (transHandle t) $ \m -> return $
                  case findFirstDoc (fwdIdx m) (transTID t) did of
                    Nothing -> (0, 0)
                    Just r  -> (docAddr r, docSize r)
  let r = DocRecord { docID   = did
                    , docTID  = transTID t
                    , docRefs = []
                    , docAddr = addr
                    , docSize = sz
                    , docDel  = True
                    }
  S.put t { transUpdateList = (r, B.empty) : transUpdateList t }

page :: forall a m. (Document a, MonadIO m) => Maybe (ExtID a) ->
                    Property a -> Int -> Transaction m [(DocID a, a)]
page mdid prop pg = Transaction $ do
  t <- S.get
  let pid = mkPropID prop
  dds <- withMasterLock (transHandle t) $ \m -> do
           let ds = case Map.lookup (toInt pid) (bckIdx m) of
                      Nothing -> []
                      Just ds -> concat $ Set.toDescList <$> getPage st pg ds
                        where st = toInt $ unExtID $ fromMaybe maxBound mdid
           let mbds = findFirstDoc (fwdIdx m) (transTID t) . fromIntegral <$> ds
           return [ fromJust mb | mb <- mbds, not (null mb) ]
  dds' <- forM dds $ \d -> readDocument (transHandle t) d
  S.put t { transReadList = (unDocID . fst <$> dds') ++ transReadList t }
  return dds'

size :: forall a m. (Document a, MonadIO m) => Property a -> Transaction m Int
size prop = Transaction $ do
  t <- S.get
  let pid = mkPropID prop
  withMasterLock (transHandle t) $ \m -> return $
    case Map.lookup (toInt pid) (bckIdx m) of
      Nothing -> 0
      Just ds -> sum $ Set.size . snd <$> Map.toList ds

debug :: (MonadIO m) => Handle -> m String
debug h = do
  mbb <- runTransaction h $ Transaction $ do
    t <- S.get
    ms <- withMasterLock (transHandle t) $ \m -> return $
      "logPos  : " ++ show (logPos m) ++
      "\nlogSize : " ++ show (logSize m) ++
      "\nnewTID  : " ++ show (newTID m) ++
      "\ntransLog:\n  " ++ show (transLog m) ++
      "\ngaps    :\n  " ++ show (gaps m) ++
      "\nfwdIdx  :\n  " ++ show (fwdIdx m) ++
      "\nbckIdx  :\n  " ++ show (bckIdx m)
    js <- withJobs (transHandle t) $ \js -> return (js, show js)
    return $ ms ++ "\njobs    :\n  " ++ js
  case mbb of
    Nothing  -> return "Nothing"
    Just mba -> return mba

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

wordSize :: Int
wordSize = 4

withMasterLock :: MonadIO m => Handle -> (MasterState -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar $ master $ unHandle h)
  (putMVar (master $ unHandle h))

withMaster :: MonadIO m => Handle -> (MasterState -> IO (MasterState, a)) -> m a
withMaster h f = liftIO $ bracketOnError
  (takeMVar $ master $ unHandle h)
  (putMVar (master $ unHandle h))
  (\m -> do
    (m', a) <- f m
    putMVar (master $ unHandle h) m'
    return a)

withDataLock :: MonadIO m => Handle -> (IO.Handle -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar $ dataHandle $ unHandle h)
  (putMVar (dataHandle $ unHandle h))

withJobs :: MonadIO m => Handle -> ([Job] -> IO ([Job], a)) -> m a
withJobs h f = liftIO $ bracketOnError
  (takeMVar $ jobs $ unHandle h)
  (putMVar (jobs $ unHandle h))
  (\js -> do
    (js', a) <- f js
    putMVar (jobs $ unHandle h) js'
    return a)

withUpdateMan :: MonadIO m => Handle -> (Bool -> IO (Bool, a)) -> m a
withUpdateMan h f = liftIO $ bracketOnError
  (takeMVar $ updateMan $ unHandle h)
  (putMVar (updateMan $ unHandle h))
  (\kill -> do
    (kill', a) <- f kill
    putMVar (updateMan $ unHandle h) kill'
    return a)

withGC :: MonadIO m => Handle -> (GCState -> IO (GCState, a)) -> m a
withGC h f = liftIO $ bracketOnError
  (takeMVar $ gcState $ unHandle h)
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
writeWord h w = liftIO $ B.hPut h $ encode w

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
  let bs = encode p
  liftIO $ B.hPut h bs

logError :: MonadIO m => IO.Handle -> String -> m a
logError h err = do
  pos <- liftIO $ IO.hTell h
  liftIO $ ioError $ userError $ "Corrupted log. " ++ err ++ " Position: " ++ show pos

mkNewTID :: MonadIO m => Handle -> m TID
mkNewTID h = withMaster h $ \m -> return (m { newTID = newTID m + 1 }, newTID m)

updateFwdIdx :: Map.IntMap [DocRecord] -> [DocRecord] -> Map.IntMap [DocRecord]
updateFwdIdx = foldl' f
  where f idx r = let did = toInt $ docID r in
                  let rs' = case Map.lookup did idx of
                              Nothing  -> [r]
                              Just ors -> r : ors in
                  Map.insert did rs' idx

updateBckIdx :: Map.IntMap (Map.IntMap Set.IntSet) -> [DocRecord] ->
                Map.IntMap (Map.IntMap Set.IntSet)
updateBckIdx = foldl' f
  where f idx r = foldl' g idx (docRefs r)
          where
            did = toInt (docID r)
            del = docDel r
            g idx' ref =
                let pid  = toInt (propID ref) in
                let rfid = toInt (refDID ref) in
                let sng = Set.singleton did in
                case Map.lookup pid idx' of
                  Nothing   -> if del then idx'
                               else Map.insert pid (Map.singleton rfid sng) idx'
                  Just tidx -> Map.insert pid is idx'
                    where is = case Map.lookup rfid tidx of
                                 Nothing -> if del then tidx
                                            else Map.insert rfid sng tidx
                                 Just ss -> Map.insert rfid ss' tidx
                                   where ss' = if del then Set.delete did ss
                                               else Set.insert did ss

addGap :: Size -> Addr -> Map.IntMap [Addr] -> Map.IntMap [Addr]
addGap s addr gs = Map.insert sz (addr:as) gs
  where as = fromMaybe [] $ Map.lookup sz gs
        sz = toInt s

updateGaps :: MasterState -> Map.IntMap [Addr]
updateGaps m = addTail $ foldl' f (Map.empty, 0) $ sortOn docAddr firstD
  where
    firstD = [ r | r:_ <- Map.elems (fwdIdx m), not $ docDel r ]
    f (gs, addr) r = (gs', docAddr r + docSize r)
      where gs' = if addr == docAddr r then gs
                  else addGap sz addr gs
            sz = docAddr r - addr
    addTail (gs, addr) = addGap (maxBound - addr) addr gs

alloc :: Map.IntMap [Addr] -> Size -> (Addr, Map.IntMap [Addr])
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

checkLogSize :: MonadIO m => MasterState -> Int -> m Int
checkLogSize m pos =
  let osz = toInt (logSize m) in
  let bpos = wordSize * (pos + 1) in
  if bpos > osz then do
    let sz = max bpos $ osz + 4096
    liftIO $ IO.hSetFileSize (logHandle m) $ fromIntegral sz
    return sz
  else return osz

logSeek :: MonadIO m => MasterState -> m ()
logSeek m = liftIO $ IO.hSeek h IO.AbsoluteSeek p
  where h = logHandle m
        p = fromIntegral $ wordSize * toInt (1 + logPos m)

readLog :: MonadIO m => MasterState -> Int -> m MasterState
readLog m pos = do
  let h = logHandle m
  let l = transLog m
  ln <- readLogTRec h
  m' <- case ln of
          Pending r ->
            let tid = toInt $ docTID r in
            let ntid = max (newTID m) (fromIntegral tid + 1) in
            case Map.lookup tid l of
              Nothing -> return m { newTID = ntid
                                  , transLog = Map.insert tid [r] l }
              Just rs -> return m { newTID = ntid
                                  , transLog = Map.insert tid (r:rs) l }
          Completed tid ->
            case Map.lookup (toInt tid) l of
              Nothing -> logError h $ "Completed TID:" ++ show tid ++
                " found but transaction did not previously occur."
              Just rs -> return m { newTID   = max (newTID m) (tid + 1)
                                  , transLog = Map.delete (toInt tid) l
                                  , fwdIdx   = updateFwdIdx (fwdIdx m) rs
                                  , bckIdx   = updateBckIdx (bckIdx m) rs
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
      rfc <- readWord h
      rfs <- replicateM (toInt rfc) $ do
               rpid <- readWord h
               rdid <- readWord h
               return Reference { propID = rpid
                                , refDID = rdid
                                }
      return $ Pending DocRecord { docID   = did
                                 , docTID  = tid
                                 , docRefs = rfs
                                 , docAddr = adr
                                 , docSize = siz
                                 , docDel  = dlb
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
      writeWord h $ fromIntegral $ if docDel doc then truTag else flsTag
      writeWord h $ fromIntegral $ length $ docRefs doc
      forM_ (docRefs doc) $ \r -> do
         writeWord h $ propID r
         writeWord h $ refDID r
    Completed tid -> do
      writeWord h $ fromIntegral cmpTag
      writeWord h tid

tRecSize :: TRec -> Int
tRecSize r = case r of
  Pending dr  -> 7 + 2 * length (docRefs dr)
  Completed _ -> 2

mkPropID :: forall a. Typeable a => Property a -> PropID
mkPropID p = fromIntegral $ hash (show $ typeRep (Proxy :: Proxy a), show p)

getRefsConv :: forall a. Document a => a -> [Reference]
getRefsConv a = concat docRs ++ concat extRs
  where docRs = (\(DocRefList p rs) -> mkDRef p <$> rs) <$> getDocRefs a
        extRs = (\(ExtRefList p rs) -> mkERef p <$> rs) <$> getExtRefs a
        mkDRef p (DocID k) = Reference (mkPropID p) k
        mkERef p (ExtID k) = Reference (mkPropID p) k

dataError :: MonadIO m => IO.Handle -> String -> m a
dataError h err = do
  pos <- liftIO $ IO.hTell h
  liftIO $ ioError $ userError $ "Corrupted data file. " ++ err ++
    " Position: " ++ show pos

readDocument :: forall a m. (Serialize a, MonadIO m) => Handle -> DocRecord -> m a
readDocument h r = withDataLock h $ \hnd -> do
  liftIO $ IO.hSeek hnd IO.AbsoluteSeek $ fromIntegral $ docAddr r
  bs <- B.hGet hnd $ fromIntegral $ docSize r
  case decode bs :: Either String a of
    Right x  -> return x
    Left err -> dataError hnd err

findFirstDoc :: Map.IntMap [DocRecord] -> TID -> DID -> Maybe DocRecord
findFirstDoc idx tid did = do
  rs <- Map.lookup (toInt did) idx
  r  <- find (\r -> docTID r <= tid) rs
  if docDel r then Nothing
  else Just r

getPage :: Int -> Int -> Map.IntMap a -> [a]
getPage st p idx = go st p idx []
  where go st p idx acc =
          if p == 0 then acc
          else case Map.lookupLT st idx of
                 Nothing     -> acc
                 Just (n, a) -> go n (p - 1) idx (a:acc)

updateManThread :: Handle -> Bool -> IO ()
updateManThread h w = do
  (kill, wait) <- withUpdateMan h $ \kill -> do
    wait <- if kill then return True else do
      when w $ threadDelay $ 100 * 1000
      (mbj, wait) <- withJobs h $ \js ->
        case js of
          []    -> return (js, (Nothing, True))
          j:js' -> return (js', (Just j, null js'))
      unless (null mbj) $ do
        let (tid, rs) = fromJust mbj
        withDataLock h $ \hnd -> do
          let maxAddr = toInteger $ maximum $ (\r -> docAddr r + docSize r) . fst <$> rs
          sz <- IO.hFileSize hnd
          when (sz < maxAddr + 1) $ do
            let nsz = max (maxAddr + 1) $ sz + 4096
            IO.hSetFileSize hnd nsz
          forM_ rs $ \(r, bs) ->
            unless (docDel r) $ do
              IO.hSeek hnd IO.AbsoluteSeek $ fromIntegral $ docAddr r
              B.hPut hnd bs
        withMaster h $ \m -> do
          let rs' = fst <$> rs
          let trec = Completed tid
          let pos = fromIntegral (logPos m) + tRecSize trec
          lsz <- checkLogSize m pos
          logSeek m
          let lh = logHandle m
          writeLogTRec lh trec
          writeLogPos lh $ fromIntegral pos
          let m' = m { logPos   = fromIntegral pos
                     , logSize  = fromIntegral lsz
                     , transLog = Map.delete (toInt tid) $ transLog m
                     , fwdIdx   = updateFwdIdx (fwdIdx m) rs'
                     , bckIdx   = updateBckIdx (bckIdx m) rs'
                     }
          return (m', ())
      return wait
    return (kill, (kill, wait))
  unless kill $ updateManThread h wait

gcThread :: Handle -> IO ()
gcThread h = do
  sgn <- withGC h $ \sgn -> do
    when (sgn == PerformGC) $
      return ()
    return (sgn, sgn)
  unless (sgn == KillGC) $ do
    threadDelay $ 1000 * 1000
    gcThread h
