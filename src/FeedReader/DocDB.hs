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
  , Transaction
  , runTransaction
  , DocID
  , RefList (..)
  , Document (..)
  , fetch
  , update
  ) where

import           Control.Concurrent  (MVar, newMVar, putMVar, takeMVar)
import           Control.Exception   (bracket, bracketOnError)
import           Control.Monad       (forM_, replicateM)
import qualified Control.Monad.State as S
import           Control.Monad.Trans (MonadIO (liftIO))
import qualified Data.ByteString     as B
import           Data.Hashable       (hash)
import qualified Data.IntMap         as Map
import qualified Data.IntSet         as Set
import           Data.List           (foldl')
import           Data.Maybe          (fromJust, fromMaybe)
import qualified Data.Sequence       as Seq
import           Data.Serialize      (Serialize (..), decode, encode)
import           Data.Serialize.Get  (getWord32be)
import           Data.Serialize.Put  (putWord32be)
import           Data.Typeable       (Proxy (..), Typeable, typeRep)
import           Data.Word           (Word32)
import           System.FilePath     ((</>))
import qualified System.IO           as IO

newtype DBWord = DBWord { unDBWord :: Word32 }
  deriving (Eq, Ord, Num, Enum, Real, Integral)

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
type TypeID = DBWord

data Reference = Reference
  { propID :: PropID
  , refDID :: DID
  }

data DocRecord = DocRecord
  { docID   :: DID
  , docType :: TypeID
  , docRefs :: [Reference]
  , docAddr :: Addr
  , docSize :: Size
  , docDel  :: Bool
  }

data MasterState = MasterState
  { logHandle :: IO.Handle
  , logPos    :: Addr
  , datPos    :: Addr
  , logSize   :: Size
  , datSize   :: Size
  , newTID    :: TID
  , gaps      :: Map.IntMap [Addr]
  , transLog  :: Map.IntMap [DocRecord]
  , fwdIdx    :: Map.IntMap [DocRecord]
  , bckIdx    :: Map.IntMap (Map.IntMap Set.IntSet)
  , tblIdx    :: Map.IntMap Set.IntSet
  }

data JobItem = JobItem
  { jobDocID   :: DID
  , jobDocAddr :: Addr
  , jobDocSize :: Size
  , jobDocStr  :: B.ByteString
  }

type Job = (TID, [JobItem])

data DBState = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , master       :: MVar MasterState
  , dataHandle   :: MVar IO.Handle
  , jobs         :: MVar [Job]
  }

instance Eq DBState where
  DBState r1 _ _ _ _ == DBState r2 _ _ _ _ = r1 == r2

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle (DBState r1 _ _ _ _)) = r1

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
  deriving (Eq, Ord, Serialize)

data RefList a = forall b. RefList [DocID b]

class (Typeable a, Serialize a) => Document a where
  getRefs :: a -> [RefList a]

open :: MonadIO m => Maybe FilePath -> Maybe FilePath -> m Handle
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  lfh <- liftIO $ IO.openBinaryFile logPath IO.ReadWriteMode
  dfh <- liftIO $ IO.openBinaryFile datPath IO.ReadWriteMode
  dsz <- liftIO $ IO.hFileSize dfh
  (pos, lsz) <- readLogPos lfh
  let m = MasterState { logHandle = lfh
                      , logPos    = pos
                      , datPos    = 0
                      , logSize   = lsz
                      , datSize   = fromInteger dsz
                      , newTID    = 1
                      , gaps      = Map.empty
                      , transLog  = Map.empty
                      , fwdIdx    = Map.empty
                      , bckIdx    = Map.empty
                      , tblIdx    = Map.empty
                      }
  m' <- if (pos > 0) && (toInt lsz > wordSize)
        then readLog m
        else return m
  mv <- liftIO $ newMVar m'
  dv <- liftIO $ newMVar dfh
  jv <- liftIO $ newMVar []
  return $ Handle DBState { logFilePath  = logPath
                          , dataFilePath = datPath
                          , master       = mv
                          , dataHandle   = dv
                          , jobs         = jv
                          }

close :: MonadIO m => Handle -> m ()
close h = do
  withMasterLock h $ \m -> IO.hClose (logHandle m)
  withDataLock   h $ \d -> IO.hClose d

runTransaction :: MonadIO m => Handle -> Transaction m a -> m (Maybe a)
runTransaction h (Transaction t) = do
  tid <- withMaster h $ \m -> return (m { newTID = newTID m + 1 }, newTID m)
  (a, q, u) <- runUserCode tid
  (mba, ts) <- withMaster h $ \m -> do
    let l = transLog m
    let (_, newTs) = Map.split (toInt tid) l
    let ck lst f = Map.null $ Map.intersection l $ Map.fromList $ f <$> lst
    if ck q (\k -> (toInt k, ())) && ck u (\(d,_) -> (toInt (docID d), ()))
    then do
      let tsz = sum $ (tRecSize . fst) <$> u
      let pos = toInt (logPos m) + tsz
      lsz <- checkFileSize m pos
      let (ts, gs) = foldl' allocFold ([], gaps m) u
      let m' = m { logPos   = fromIntegral pos
                 , logSize  = fromIntegral lsz
                 , gaps     = gs
                 , transLog = Map.insert (toInt tid) (fst <$> ts) l
                 }
      writeTransactions m tid ts
      writeLogPos (logHandle m) $ fromIntegral pos
      return (m', (Just a, ts))
    else return (m, (Nothing, []))
  enqueueJob tid ts
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
      return (a, q, u)

    checkFileSize m pos = let osz = toInt (logSize m) in
      if pos > osz
      then do
             let sz = max pos $ osz + 4096
             IO.hSetFileSize (logHandle m) $ fromIntegral sz
             return sz
      else return osz

    allocFold (ts, gs) (r, bs) =
      let (a, gs') = alloc gs (toInt $ docSize r) in
      let t = r { docAddr = a } in
      ((t, bs):ts, gs')

    writeTransactions m tid ts = do
      liftIO $ IO.hSeek (logHandle m) IO.AbsoluteSeek $ fromIntegral (logPos m)
      forM_ ts $ \(t, _) ->
        writeLogTRec (logHandle m) $ Pending tid t

    enqueueJob tid ts =
      withJobs h $ \js -> do
        let jis = (\(r, bs) -> JobItem (docID r) (docAddr r) (docSize r) bs) <$> ts
        return ((tid, jis):js, ())

fetch :: MonadIO m => DocID a -> Transaction m (Maybe a)
fetch did = Transaction $ do
  t <- S.get
  S.put t { transReadList = unDocID did : transReadList t }
  return Nothing

update :: forall a m. (Document a, MonadIO m) => DocID a -> a -> Transaction m ()
update did a = Transaction $ do
  t <- S.get
  let bs = encode a
  let r = DocRecord { docID   = unDocID did
                    , docType = mkTypeID (Proxy :: Proxy a)
                    , docRefs = getRefsConv a
                    , docAddr = 0
                    , docSize = fromIntegral $ B.length bs
                    , docDel  = False
                    }
  S.put t { transUpdateList = (r, bs) : transUpdateList t }


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

updateBckIdx :: Map.IntMap (Map.IntMap Set.IntSet) -> [DocRecord] ->
                Map.IntMap (Map.IntMap Set.IntSet)
updateBckIdx = foldl' f
  where f idx r =
          let did = toInt (docID r) in
          let g idx' ref =
                let pid  = toInt (propID ref) in
                let rfid = toInt (refDID ref) in
                let sng = Map.singleton rfid (Set.singleton did) in
                case Map.lookup pid idx' of
                  Nothing   -> Map.insert pid sng idx'
                  Just tidx -> Map.insert pid is idx'
                    where is = case Map.lookup rfid tidx of
                                 Nothing -> sng
                                 Just ss -> Map.insert rfid (Set.insert did ss) tidx in
          foldl' g idx (docRefs r)

updateTblIdx :: Map.IntMap Set.IntSet -> [DocRecord] -> Map.IntMap Set.IntSet
updateTblIdx = foldl' f
  where f idx r =
          let did = toInt (docID r) in
          let yid = toInt (docType r) in
          let ss = case Map.lookup yid idx of
                     Nothing -> Set.singleton did
                     Just ds -> Set.insert did ds in
          Map.insert yid ss idx

updateGaps :: Map.IntMap [Addr] -> [DocRecord] -> Map.IntMap [Addr]
updateGaps = foldl' f
  where f idx r =
          let a = toInt (docAddr r) in
          let s = toInt (docSize r) in
          let did = toInt (docID r) in
          if Map.null idx
          then Map.insert maxBound [fromIntegral (a + s)] $ Map.insert a [0] idx
          else case Map.lookupGE s idx of
                 Nothing          -> error "Gap update error."
                 Just (sz, DBWord fa:as) ->
                   let idx' = Map.insert sz as idx in
                   let d = sz - s in
                   if d == 0 then idx'
                   else let addr = DBWord (fa + fromIntegral d) in
                        case Map.lookup d idx' of
                          Nothing  -> Map.insert d [addr] idx'
                          Just ads -> Map.insert d (addr:ads) idx'

alloc :: Map.IntMap [Addr] -> Int -> (Addr, Map.IntMap [Addr])
alloc gs sz =
  let (gsz, a:as) = fromJust (Map.lookupGE sz gs) in
  let gs' = Map.insert gsz as gs in
  let g' = gsz - sz in
  if g' == 0 then (a, gs')
  else let as' = fromMaybe [] (Map.lookup g' gs') in
       (a, Map.insert g' ((a + fromIntegral sz):as') gs')


data TRec = Pending TID DocRecord | Completed TID

toInt :: DBWord -> Int
toInt (DBWord d) = fromIntegral d

readLog :: MonadIO m => MasterState -> m MasterState
readLog m = do
  let h = logHandle m
  let l = transLog m
  ln <- readLogTRec h
  m' <- case ln of
          Pending tid r ->
            let ntid = max (newTID m) (tid + 1) in
            case Map.lookup (toInt tid) l of
              Nothing -> return m { newTID = ntid
                                  , transLog = Map.insert (toInt tid) [r] l }
              Just rs -> return m { newTID = ntid
                                  , transLog = Map.insert (toInt tid) (r:rs) l }
          Completed tid ->
            case Map.lookup (toInt tid) l of
              Nothing -> logError h $ "Completed TID:" ++ show tid ++
                " found but transaction did not previously occur."
              Just rs -> return m { datSize  = max (datSize m) maxDoc
                                  , newTID   = max (newTID m) (tid + 1)
                                  , transLog = Map.delete (toInt tid) l
                                  , fwdIdx   = Map.insert (toInt tid) rs $ fwdIdx m
                                  , bckIdx   = updateBckIdx (bckIdx m) rs
                                  , tblIdx   = updateTblIdx (tblIdx m) rs
                                  , gaps     = updateGaps (gaps m) rs
                                  }
                         where maxDoc = maximum $ (\d -> docAddr d + docSize d) <$> rs
  pos <- liftIO $ IO.hTell h
  if pos >= (fromIntegral (logPos m) - 1) then return m'
  else readLog m'

pendingTag :: Int
pendingTag = 112 -- ASCII 'p'

completedTag :: Int
completedTag = 99 -- ASCII 'c'

trueTag :: Int
trueTag = 84 -- ASCII 'T'

falseTag :: Int
falseTag = 70 -- ASCII 'F'

readLogTRec :: MonadIO m => IO.Handle -> m TRec
readLogTRec h = do
  tag <- readWord h
  case toInt tag of
    x | x == pendingTag -> do
      tid <- readWord h
      did <- readWord h
      yid <- readWord h
      adr <- readWord h
      siz <- readWord h
      del <- readWord h
      dlb <- case toInt del of
               x | x == trueTag  -> return True
               x | x == falseTag -> return False
               _ -> logError h $ "True ('T') or False ('F') tag expected but " ++
                      show del ++ " found."
      rfc <- readWord h
      rfs <- replicateM (toInt rfc) $ do
               rpid <- readWord h
               rdid <- readWord h
               return Reference { propID = rpid
                                , refDID = rdid
                                }
      return $ Pending tid DocRecord { docID   = did
                                     , docType = yid
                                     , docRefs = rfs
                                     , docAddr = adr
                                     , docSize = siz
                                     , docDel  = dlb
                                     }
    x | x == completedTag -> do
      tid <- readWord h
      return $ Completed tid
    _ -> logError h $ "Pending ('p') or Completed ('c') tag expected but " ++
           show tag ++ " found."

writeLogTRec :: MonadIO m => IO.Handle -> TRec -> m ()
writeLogTRec h t =
  case t of
    Pending tid doc -> do
      writeWord h $ fromIntegral pendingTag
      writeWord h tid
      writeWord h $ docID doc
      writeWord h $ docType doc
      writeWord h $ docAddr doc
      writeWord h $ docSize doc
      writeWord h $ fromIntegral $ if docDel doc then trueTag else falseTag
      writeWord h $ fromIntegral $ length $ docRefs doc
      forM_ (docRefs doc) $ \r -> do
         writeWord h $ propID r
         writeWord h $ refDID r
    Completed tid -> do
      writeWord h $ fromIntegral completedTag
      writeWord h tid

tRecSize :: DocRecord -> Int
tRecSize r = 8 + 2 * length (docRefs r)

mkTypeID :: Typeable a => Proxy a -> TypeID
mkTypeID proxy = fromIntegral $ hash $ show $ typeRep proxy

mkPropID :: TypeID -> Int -> PropID
mkPropID yid i = fromIntegral $ hash (toInt yid, i)

getRefsConv :: forall a. Document a => a -> [Reference]
getRefsConv a = snd $ foldl' f (1, []) $ (\(RefList rs) -> unDocID <$> rs) <$> getRefs a
  where yid = mkTypeID (Proxy :: Proxy a)
        f (i, rfs) rs = (i + 1, (Reference (mkPropID yid i) <$> rs) ++ rfs)
