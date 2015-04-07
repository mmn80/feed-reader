-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.Data.DB
-- Copyright : (C) 2015 Călin Ardelean,
-- License : BSD-style (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides the block database backend for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.BlockDB
  ( Handle
  , open
  , close
  , get
  , put
  ) where

import           Control.Concurrent  (MVar, newMVar, putMVar, takeMVar)
import           Control.Exception   (bracket)
import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.ByteString     (hGet, hPut)
import           Data.Maybe          (fromMaybe)
import           Data.SafeCopy       (SafeCopy, safeGet, safePut)
import           Data.Serialize.Get  (runGet)
import           Data.Serialize.Put  (runPut)
import           System.FilePath     ((</>))
import qualified System.IO           as F

data BlockDBState = BlockDBState
  { filePath   :: FilePath
  , fileHandle :: MVar F.Handle
  }

instance Eq BlockDBState where
  BlockDBState r1 _ == BlockDBState r2 _ = r1 == r2

newtype Handle = Handle { unHandle :: BlockDBState } deriving (Eq)

instance Show Handle where
  show (Handle (BlockDBState r1 _)) = r1

--(hClose, hFileSize, hGetBuf, hPutBuf, hSeek,
--                            openBinaryFile)
open :: MonadIO m => Maybe FilePath -> m Handle
open f = do
  let path = fromMaybe ("data" </> "blocks.db") f
  fh <- liftIO $ F.openBinaryFile path F.ReadWriteMode
  mv <- liftIO $ newMVar fh
  let h = Handle BlockDBState { filePath   = path
                              , fileHandle = mv
                              }
  return h

close :: MonadIO m => Handle -> m ()
close h = withHandle h $ \fh -> F.hClose fh

withHandle :: MonadIO m => Handle -> (F.Handle -> IO a) -> m a
withHandle h = liftIO . bracket
  (takeMVar $ fileHandle $ unHandle h)
  (putMVar (fileHandle $ unHandle h))

put :: (SafeCopy a, MonadIO m) => Handle -> a -> m ()
put h a = withHandle h $ \fh ->
  hPut fh $ runPut $ safePut a

get :: (SafeCopy a, MonadIO m) => Handle -> Int -> m (Either String a)
get h sz = withHandle h $ \fh -> do
  bs <- hGet fh sz
  return $ runGet safeGet bs
