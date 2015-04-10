{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}

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
-- This module provides the acid-state based database backend for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.DB
  ( module FeedReader.Types
  , Handle
  , TableMethods (..)
  , open
  , close
  , getStats
  , getShardStats
  , getItemPage
  , addItemConv
  , addFeedConv
  , checkpoint
  , archive
  , deleteArchives
  , deleteDB
  ) where

import           Control.Concurrent  (MVar, modifyMVar_, newMVar, putMVar,
                                      takeMVar)
import           Control.Exception   (bracket)
import           Control.Monad       (forM, forM_, unless, when)
import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.Acid           (AcidState, EventResult, QueryEvent,
                                      UpdateEvent, closeAcidState,
                                      createArchive, createCheckpoint,
                                      openLocalStateFrom, query, update)
import           Data.Acid.Advanced  (MethodResult, MethodState)
import           Data.Acid.Local     (createCheckpointAndClose)
import           Data.List           (groupBy, sortBy)
import qualified Data.Map            as Map
import           Data.Maybe          (fromJust, fromMaybe)
import qualified Data.Sequence       as Seq
import           Data.Time.Clock     (UTCTime, getCurrentTime)
import qualified FeedReader.DocDB  as DB
import           FeedReader.Queries
import           FeedReader.Types
import           System.Directory    (doesDirectoryExist,
                                      removeDirectoryRecursive)
import           System.FilePath     ((</>))

type OpenedShards = Map.Map ShardID (UTCTime, AcidState Shard)

data DBState = DBState
  { rootDir    :: FilePath
  , master     :: AcidState Master
  , masterLock :: MVar Bool
  , shards     :: MVar OpenedShards
  , blockHnd   :: DB.Handle
  }

instance Eq DBState where
  DBState r1 _ _ _ _ == DBState r2 _ _ _ _ = r1 == r2

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle (DBState r1 _ _ _ _)) = r1

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

maxOpenedShards :: Int
maxOpenedShards = 5

maxShardItems :: Int
maxShardItems = 100

mquery :: (MonadIO m, QueryEvent e, MethodState e ~ Master) =>
          Handle -> e -> m (EventResult e)
mquery  h = liftIO . query  (master $ unHandle h)

withMasterLock :: MonadIO m => Handle -> IO a -> m a
withMasterLock h a = liftIO $ bracket
  (takeMVar $ masterLock $ unHandle h)
  (\_ -> putMVar (masterLock $ unHandle h) False)
  (const a)

mupdate_ :: (MonadIO m, UpdateEvent e, MethodState e ~ Master) =>
            Handle -> e -> m (EventResult e)
mupdate_ h e = liftIO $ update (master $ unHandle h) e

mupdate :: (MonadIO m, UpdateEvent e, MethodState e ~ Master) =>
          Handle -> e -> m (EventResult e)
mupdate h e = withMasterLock h $ mupdate_ h e

shardPath :: Handle -> ShardID -> FilePath
shardPath h i = rootDir (unHandle h) </> "shard_" ++ show i

masterPath :: FilePath -> FilePath
masterPath r = r </> "master"

groupByShard :: [(t, ShardID)] -> [[(t, ShardID)]]
groupByShard = groupBy (\(_,x) (_,y) -> x == y) . sortBy (\(_,x) (_,y) -> compare y x)

checkOpenedShards :: MonadIO m => OpenedShards -> m OpenedShards
checkOpenedShards ss =
  if Map.size ss > maxOpenedShards then do
    t0 <- liftIO getCurrentTime
    let older sid (t, _) (t', sid') =
          if t < t' then (t, sid) else (t', sid')
    let (_, sid) = Map.foldWithKey older (t0, unsetID) ss
    let Just (_, a) = Map.lookup sid ss
    liftIO $ putStrLn $ "  Closing shard #" ++ show sid ++ "..."
    liftIO $ closeAcidState a
    return $ Map.delete sid ss
  else
    return ss

withShard :: MonadIO m => Handle -> ShardID ->
             ((AcidState Shard, OpenedShards) -> IO a) -> m a
withShard h sid = liftIO . bracket
  (do ss <- takeMVar $ shards $ unHandle h
      let mb = Map.lookup sid ss
      t0 <- getCurrentTime
      let ins a = (a, Map.insert sid (t0, a) ss)
      if null mb then do
        putStrLn $ "  Opening shard #" ++ show sid ++ "..."
        acid <- openLocalStateFrom (shardPath h sid) emptyShard
        t1 <- getCurrentTime
        putStrLn $ "  Shard opened in " ++ show (diffMs t0 t1) ++ " ms."
        return $ ins acid
      else
        return $ ins $ snd $ fromJust mb )
  (\(_, ss) -> do
    ss' <- checkOpenedShards ss
    putMVar (shards $ unHandle h) ss')

closeShards :: MonadIO m => Handle -> m ()
closeShards h =
  liftIO $ modifyMVar_ (shards $ unHandle h) $ \ss -> do
    mapM_ (liftIO . closeAcidState . snd) ss
    return Map.empty

checkPending :: MonadIO m => Handle -> m ()
checkPending h = do
  p <- mquery h GetPendingOpAcid
  case p of
    PendingAddItem k i -> do
      sid <- mquery h $ FindItemShardAcid (toInt k)
      withShard h sid $ \(a, _) -> do
        _ <- liftIO $ update a $ AddShardItemAcid i
        mupdate_ h $ UpdatePendingOpAcid NoPendingOp
    PendingSplitPhase0 lid rid -> do
      Just lix <- mquery h $ GetShardIdxAcid lid
      Just rix <- mquery h $ GetShardIdxAcid rid
      let r = foldrShard (:) [] rix
      let lsz = shardSize lix
      let rsz = shardSize rix
      withShard h lid $ \(a, _) -> do
        splitPhase0 h a r lid rid rsz
        splitPhase1 h a rid lsz
    PendingSplitPhase1 lid rid -> do
      Just lix <- mquery h $ GetShardIdxAcid lid
      let lsz = shardSize lix
      withShard h lid $ \(a, _) -> splitPhase1 h a rid lsz
    NoPendingOp -> return ()

splitPhase0 :: MonadIO m => Handle -> AcidState Shard -> [ItemID] ->
                            ShardID -> ShardID -> ShardSize -> m ()
splitPhase0 h a r sid rid rsz = do
  rs <- liftIO $ query a $ GetShardItemsAcid r
  let f = \case Just i -> [(toInt $ itemID i, i)]
                _      -> []
  createShard h (tableFromList $ concatMap f rs) rid rsz
  mupdate_ h $ UpdatePendingOpAcid $ PendingSplitPhase1 sid rid

splitPhase1 :: MonadIO m => Handle -> AcidState Shard -> ShardID -> ShardSize -> m ()
splitPhase1 h a rid lsz = do
  liftIO $ update a $ SplitShardAcid (shardID2ItemID rid) lsz
  mupdate_ h $ UpdatePendingOpAcid NoPendingOp

resyncShard :: MonadIO m => Handle -> AcidState Shard -> ShardID -> m [ShardID]
resyncShard h a sid = do
  c <- liftIO $ query a GetShardSizeAcid
  if c > maxShardItems then do
    (_, r, lsz, rsz, rid) <- mupdate_ h $ SplitShardMasterAcid sid
    splitPhase0 h a r sid rid rsz
    splitPhase1 h a rid lsz
    return [sid, rid]
  else return []

resyncShards :: MonadIO m => Handle -> [ShardID] -> m ()
resyncShards h ss = do
  rss <- forM ss $ \sid ->
    withShard h sid $ \(a, _) ->
      resyncShard h a sid
  let rsys = concat rss
  unless (null rsys) $ resyncShards h rsys

deleteShardDir :: MonadIO m => Handle -> ShardID -> m Bool
deleteShardDir h s = do
  let sf = shardPath h s
  ex <- liftIO $ doesDirectoryExist sf
  when ex $ liftIO $ removeDirectoryRecursive sf
  return $ not ex

createShard :: MonadIO m => Handle -> Table Item -> ShardID -> Int -> m ()
createShard h t sid sz = liftIO $ bracket
  (do
     _ <- deleteShardDir h sid
     openLocalStateFrom (shardPath h sid) Shard { shSize  = sz
                                                , tblItem = t })
  createCheckpointAndClose
  (\_ -> return ())

------------------------------------------------------------------------------
-- Main DB Functions
------------------------------------------------------------------------------

class TableMethods a where
  findRecord    :: MonadIO m => Handle -> Int -> m (Maybe a)
  getAllRecords :: MonadIO m => Handle -> m (Seq.Seq a)
  addRecord     :: MonadIO m => Handle -> a -> m a

  addRecords    :: MonadIO m => Handle -> [a] -> m [a]
  addRecords h as = forM as $ addRecord h

instance TableMethods Cat where
  findRecord h k  = mquery h $ FindCatAcid k
  getAllRecords h = mquery h GetCatsAcid
  addRecord  h a  = mupdate h $ AddCatAcid a

instance TableMethods Feed where
  findRecord h k  = mquery h $ FindFeedAcid k
  getAllRecords h = mquery h GetFeedsAcid
  addRecord  h a  = mupdate h $ AddFeedAcid a

instance TableMethods Person where
  findRecord h k  = mquery h $ FindPersonAcid k
  getAllRecords h = mquery h GetPersonsAcid
  addRecord  h a  = mupdate h $ AddPersonAcid a

instance TableMethods Item where
  findRecord h k = do
    s <- mquery h $ FindItemShardAcid k
    withShard h s $ \(a, _) ->
      liftIO $ query a $ FindShardItemAcid k

  getAllRecords h = undefined

  addRecord h a = head <$> addRecords h [a]

  addRecords h is = withMasterLock h $ do
    iss <- forM is $ \i -> do
      iid <- mquery h $ MkUniqueIDAcid i     {- TODO: check for self collisions -}
      s   <- mquery h $ FindItemShardAcid $ toInt iid
      return (i { itemID = iid }, s)
    ps <- forM (groupByShard iss) $ \gs ->
      withShard h (snd $ head gs) $ \(a, _) -> do
        gs' <- forM gs $ \(i, _) -> do
                 _ <- mupdate_ h $ AddMasterItemAcid i
                 _ <- liftIO $ update a $ AddShardItemAcid i
                 mupdate_ h $ UpdatePendingOpAcid NoPendingOp
                 return i
        rsy <- resyncShard h a $ snd $ head gs
        return (gs', rsy)
    resyncShards h $ concat $ snd <$> ps
    return $ concat $ fst <$> ps

getItemPage :: MonadIO m => Handle -> Int -> Int -> m [Item]
getItemPage h k l = do
  iss <- mquery h $ GetItemPageAcid k l
  mbs <- forM (groupByShard iss) $ \gs ->
    withShard h (snd $ head gs) $ \(a, _) ->
      liftIO $ query a $ GetShardItemsAcid $ fst <$> gs
  return $ fromJust <$> filter (not . null) (concat $ reverse <$> mbs)

open :: MonadIO m => Maybe FilePath -> m Handle
open r = do
  let root = fromMaybe "data" r
  acid <- liftIO $ openLocalStateFrom (masterPath root) emptyMaster
  s <- liftIO $ newMVar Map.empty
  l <- liftIO $ newMVar False
  bk <- DB.open Nothing Nothing
  let h = Handle DBState { rootDir    = root
                         , master     = acid
                         , masterLock = l
                         , shards     = s
                         , blockHnd   = bk
                         }
  checkPending h
  return h

getStats :: MonadIO m => Handle -> m (StatsMaster, [(ShardID, UTCTime, ShardSize)])
getStats h = do
  s <- mquery h GetStatsAcid
  --BK.put (blockHnd $ unHandle h) s
  liftIO $ bracket
    (takeMVar $ shards $ unHandle h)
    (putMVar $ shards $ unHandle h)
    (\ss -> do
      sds <- forM (Map.toList ss) $ \(k,(t,a)) -> do
        sz <- liftIO $ query a GetShardSizeAcid
        return (k, t, sz)
      return (s, sds))

getShardStats :: MonadIO m => Handle -> m (Seq.Seq (ShardID, ShardSize))
getShardStats h = --do
  --a <- BK.get (blockHnd $ unHandle h) 53
  -- liftIO $ putStrLn $ "TEST: " ++ show (a :: Either String StatsMaster)
  mquery h GetShardsAcid

checkpoint :: MonadIO m => Handle -> m ()
checkpoint h = do
  liftIO $ createCheckpoint $ master $ unHandle h
  ss <- liftIO $ mquery h GetShardsAcid
  forM_ ss $ \(sid, _) ->
    withShard h sid $ \(a, _) ->
      liftIO $ createCheckpoint a

archive :: MonadIO m => Handle -> m ()
archive h = do
  liftIO $ createArchive $ master $ unHandle h
  ss <- liftIO $ mquery h GetShardsAcid
  forM_ ss $ \(sid, _) ->
    withShard h sid $ \(a, _) ->
      liftIO $ createArchive a

deleteArchives :: MonadIO m => Handle -> m ()
deleteArchives h = do
  let f = masterPath (rootDir $ unHandle h) </> "Archive"
  ex <- liftIO $ doesDirectoryExist f
  when ex $ liftIO $ removeDirectoryRecursive f
  ss <- mquery h GetShardsAcid
  forM_ ss $ \(s, _) -> do
    let sf = shardPath h s </> "Archive"
    ex <- liftIO $ doesDirectoryExist sf
    when ex $ liftIO $ removeDirectoryRecursive sf

close :: MonadIO m => Handle -> m ()
close h = do
  liftIO $ closeAcidState $ master $ unHandle h
  closeShards h
  DB.close $ blockHnd $ unHandle h

deleteDB :: MonadIO m => Handle -> m ()
deleteDB h = do
  ss <- mquery h GetShardsAcid
  mupdate h WipeMasterAcid
  liftIO $ createCheckpoint $ master $ unHandle h
  liftIO $ createArchive $ master $ unHandle h
  closeShards h
  forM_ ss $ \(s, _) -> do
    nf <- deleteShardDir h s
    when nf $ liftIO $ putStrLn $ "Directory for shard #" ++ show s ++ " not found"

------------------------------------------------------------------------------
-- Conversion
------------------------------------------------------------------------------

addItemConv :: (MonadIO m, ToItem i) => Handle -> i -> FeedID -> URL -> m Item
addItemConv h it fid u = do
  df <- liftIO getCurrentTime
  let (i, as, cs) = toItem it fid u df
  as' <- sequence $ addRecord h <$> as
  cs' <- sequence $ addRecord h <$> cs
  let i' = i { itemAuthors      = personID <$> as'
             , itemContributors = personID <$> cs'
             }
  mupdate h $ AddMasterItemAcid i' --TODO: add into shard

addFeedConv :: (MonadIO m, ToFeed f) => Handle -> f -> CatID -> URL -> m Feed
addFeedConv h it cid u = do
  df <- liftIO getCurrentTime
  let (f, as, cs) = toFeed it cid u df
  as' <- sequence $ addRecord h <$> as
  cs' <- sequence $ addRecord h <$> cs
  let f' = f { feedAuthors      = personID <$> as'
             , feedContributors = personID <$> cs'
             }
  mupdate h $ AddFeedAcid f'
