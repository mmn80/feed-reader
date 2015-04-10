{-# LANGUAGE GeneralizedNewtypeDeriving #-}

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
  , writeMaster
  , readMaster
  ) where

import           Control.Concurrent  (MVar, newMVar, putMVar, takeMVar)
import           Control.Exception   (bracket)
import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.ByteString     (ByteString, hGet, hPut)
import qualified Data.IntMap         as Map
import qualified Data.IntSet         as Set
import           Data.Maybe          (fromMaybe)
import qualified Data.Sequence       as Seq
import           Data.Serialize
import           Data.Serialize.Get  (getWord32be)
import           Data.Serialize.Put  (putWord32be)
import           Data.Word           (Word32)
import           System.FilePath     ((</>))
import qualified System.IO           as F

newtype DBWord = DBWord { unDBWord :: Word32 }
  deriving (Eq, Ord, Num, Enum, Real, Integral)

instance Serialize DBWord where
  put = putWord32be . unDBWord
  get = DBWord <$> getWord32be

type Addr = DBWord
type DID  = DBWord
type TID  = DBWord
type Size = DBWord
type RefTag = DBWord

data Reference = Reference
  { refTag :: RefTag
  , refDID :: DID
  }

data DocRecord = DocRecord
  { docID   :: DID
  , docRefs :: [Reference]
  , docAddr :: Addr
  , docSize :: Size
  , docDel  :: Bool
  }

data MasterState = MasterState
  { logHandle :: F.Handle
  , logPos    :: Addr
  , datPos    :: Addr
  , logSize   :: Size
  , datSize   :: Size
  , gaps      :: Map.IntMap [Addr]
  , transLog  :: Map.IntMap [DocRecord]
  , fwdIdx    :: Map.IntMap [DocRecord]
  , bckIdx    :: Map.IntMap (Map.IntMap Set.IntSet)
  }

data JobItem = JobItem
  { jobDocID   :: DID
  , jobDocAddr :: Addr
  , jobDocSize :: Size
  , jobDocStr  :: ByteString
  }

type Job = (TID, [JobItem])

data DBState = DBState
  { logFilePath  :: FilePath
  , dataFilePath :: FilePath
  , master       :: MVar MasterState
  , dataHandle   :: MVar F.Handle
  , jobs         :: MVar [Job]
  }

instance Eq DBState where
  DBState r1 _ _ _ _ == DBState r2 _ _ _ _ = r1 == r2

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle (DBState r1 _ _ _ _)) = r1

open :: MonadIO m => Maybe FilePath -> Maybe FilePath -> m Handle
open lf df = do
  let logPath = fromMaybe ("data" </> "docdb.log") lf
  let datPath = fromMaybe ("data" </> "docdb.dat") df
  lfh <- liftIO $ F.openBinaryFile logPath F.ReadWriteMode
  dfh <- liftIO $ F.openBinaryFile datPath F.ReadWriteMode
  dsz <- liftIO $ F.hFileSize dfh
  (pos, lsz) <- readLogPos lfh
  let m = MasterState { logHandle = lfh
                      , logPos    = pos
                      , datPos    = 0
                      , logSize   = lsz
                      , datSize   = fromInteger dsz
                      , gaps      = Map.empty
                      , transLog  = Map.empty
                      , fwdIdx    = Map.empty
                      , bckIdx    = Map.empty
                      }
  mv <- liftIO $ newMVar m
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
  withMasterLock h $ \m -> F.hClose (logHandle m)
  withDataLock   h $ \d -> F.hClose d

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

wordSize :: Int
wordSize = 4

withMasterLock :: MonadIO m => Handle -> (MasterState -> IO a) -> m a
withMasterLock h = liftIO . bracket
  (takeMVar $ master $ unHandle h)
  (putMVar (master $ unHandle h))

withDataLock :: MonadIO m => Handle -> (F.Handle -> IO a) -> m a
withDataLock h = liftIO . bracket
  (takeMVar $ dataHandle $ unHandle h)
  (putMVar (dataHandle $ unHandle h))

withJobsLock :: MonadIO m => Handle -> ([Job] -> IO a) -> m a
withJobsLock h = liftIO . bracket
  (takeMVar $ jobs $ unHandle h)
  (putMVar (jobs $ unHandle h))

writeMaster :: (MonadIO m) => Addr -> ByteString -> Handle -> m ()
writeMaster pos str h = withMasterLock h $ \m -> do
  let fh = logHandle m
  liftIO $ F.hSeek fh F.AbsoluteSeek $ toInteger pos
  liftIO $ hPut fh str

readMaster :: (MonadIO m) => Addr -> Size -> Handle -> m ByteString
readMaster pos sz h = withMasterLock h $ \m -> do
  let fh = logHandle m
  liftIO $ F.hSeek fh F.AbsoluteSeek $ toInteger pos
  liftIO $ hGet fh $ fromIntegral sz

readLogPos :: MonadIO m => F.Handle -> m (Addr, Size)
readLogPos h = do
  sz <- liftIO $ F.hFileSize h
  if sz >= fromIntegral wordSize then do
    liftIO $ F.hSeek h F.AbsoluteSeek 0
    bs <- liftIO $ hGet h wordSize
    case decode bs of
     Right x  -> return (x, fromIntegral sz)
     Left err -> liftIO $ ioError $ userError err
  else
    return (0, 0)

writeLogPos :: MonadIO m => F.Handle -> Addr -> m ()
writeLogPos h p = do
  liftIO $ F.hSeek h F.AbsoluteSeek 0
  let bs = encode p
  liftIO $ hPut h bs
