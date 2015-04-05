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
  , open
  , close
  , getStats
  , getShardStats
  , getCat
  , getFeed
  , getPerson
  , getItem
  , getItemPage
  , getCats
  , getFeeds
  , addCat
  , addFeed
  , addPerson
  , addItems
  , addItemConv
  , addFeedConv
  , checkpoint
  , archive
  , wipeDB
  ) where

import           Control.Concurrent  (MVar, modifyMVar_, newMVar, putMVar,
                                      takeMVar)
import           Control.Exception   (bracket)
import           Control.Monad       (forM, forM_, when)
import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.Acid           (AcidState, EventResult, QueryEvent,
                                      UpdateEvent, closeAcidState,
                                      createArchive, createCheckpoint,
                                      openLocalStateFrom, query, update)
import           Data.Acid.Advanced  (MethodState)
import           Data.Acid.Local     (createCheckpointAndClose)
import qualified Data.IntMap         as Map
import qualified Data.IntSet         as Set
import           Data.List           (groupBy, sortBy)
import           Data.Maybe          (fromJust, fromMaybe)
import qualified Data.Sequence       as Seq
import           Data.Time.Clock     (UTCTime, getCurrentTime)
import           FeedReader.Queries
import           FeedReader.Types
import           System.Directory    (doesDirectoryExist,
                                      removeDirectoryRecursive)
import           System.FilePath     ((</>))


type OpenedShards = Map.IntMap (UTCTime, AcidState Shard)

data DBState = DBState
  { rootDir    :: FilePath
  , master     :: AcidState Master
  , masterLock :: MVar Bool
  , shards     :: MVar OpenedShards
  }

instance Eq DBState where
  DBState r1 _ _ _ == DBState r2 _ _ _ = r1 == r2

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle (DBState r1 _ _ _)) = r1

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

shardPath :: Handle -> ItemID -> FilePath
shardPath h i = rootDir (unHandle h) </> "shard_" ++ show (unItemID i)

groupByShard :: [(t, ItemID)] -> [[(t, ItemID)]]
groupByShard = groupBy (\(_,x) (_,y) -> x == y) . sortBy (\(_,x) (_,y) -> compare x y)

checkOpenedShards :: MonadIO m => OpenedShards -> m OpenedShards
checkOpenedShards ss =
  if Map.size ss > maxOpenedShards then do
    t0 <- liftIO getCurrentTime
    let older sid (t, _) (t', sid') =
          if t < t' then (t, sid) else (t', sid')
    let (_, sid) = Map.foldWithKey older (t0, 0) ss
    let Just (_, a) = Map.lookup sid ss
    liftIO $ putStrLn $ "  Closing shard #" ++ show sid ++ "..."
    liftIO $ closeAcidState a
    return $ Map.delete sid ss
  else
    return ss

withShard :: MonadIO m => Handle -> ItemID ->
             ((AcidState Shard, OpenedShards) -> IO a) -> m a
withShard h s = liftIO . bracket
  (do ss <- takeMVar $ shards $ unHandle h
      let sid = unItemID s
      let mb = Map.lookup sid ss
      t0 <- getCurrentTime
      let ins a = (a, Map.insert sid (t0, a) ss)
      if null mb then do
        putStrLn $ "  Opening shard #" ++ show sid ++ "..."
        acid <- openLocalStateFrom (shardPath h s) emptyShard
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
      sid <- mquery h $ GetItemShardAcid k
      withShard h sid $ \(a, _) -> do
        _ <- liftIO $ update a $ AddShardItemAcid i
        mupdate_ h $ UpdatePendingOpAcid NoPendingOp
    PendingSplitPhase0 lid rid -> do
      Just lix <- mquery h $ GetShardIdxAcid $ unItemID lid
      Just rix <- mquery h $ GetShardIdxAcid $ unItemID rid
      let r = ItemID <$> Set.toList (shardKeys rix)
      let lsz = shardSize lix
      let rsz = shardSize rix
      withShard h lid $ \(a, _) -> do
        splitPhase0 h a r lid rid rsz
        splitPhase1 h a rid lsz
    PendingSplitPhase1 lid rid -> do
      Just lix <- mquery h $ GetShardIdxAcid $ unItemID lid
      let lsz = shardSize lix
      withShard h lid $ \(a, _) -> splitPhase1 h a rid lsz
    NoPendingOp -> return ()

splitPhase0 :: MonadIO m => Handle -> AcidState Shard -> [ItemID] ->
                            ItemID -> ItemID -> ShardSize -> m ()
splitPhase0 h a r sid rid rsz = do
  rs <- liftIO $ query a $ GetShardItemsAcid r
  let f = \case Just i -> [(unItemID $ itemID i, i)]
                _      -> []
  createShard h (Map.fromList $ concatMap f rs) rid rsz
  mupdate_ h $ UpdatePendingOpAcid $ PendingSplitPhase1 sid rid

splitPhase1 :: MonadIO m => Handle -> AcidState Shard -> ItemID -> ShardSize -> m ()
splitPhase1 h a rid lsz = do
  liftIO $ update a $ SplitShardAcid rid lsz
  mupdate_ h $ UpdatePendingOpAcid NoPendingOp

resyncShard :: MonadIO m => Handle -> AcidState Shard -> ItemID -> m ()
resyncShard h a sid = do
  StatsShard c _ <- liftIO $ query a GetStatsShardAcid
  when (c > maxShardItems) $ do
    (l, r, lsz, rsz, rid) <- mupdate_ h $ SplitShardMasterAcid sid
    splitPhase0 h a r sid rid rsz
    splitPhase1 h a rid lsz

deleteShardDir :: MonadIO m => Handle -> ItemID -> m Bool
deleteShardDir h s = do
  let sf = shardPath h s
  ex <- liftIO $ doesDirectoryExist sf
  when ex $ liftIO $ removeDirectoryRecursive sf
  return $ not ex

createShard :: MonadIO m => Handle -> Map.IntMap Item -> ItemID -> Int -> m ()
createShard h t s sz = liftIO $ bracket
  (do
     _ <- deleteShardDir h s
     openLocalStateFrom (shardPath h s) Shard { shSize  = sz
                                              , tblItem = t })
  createCheckpointAndClose
  (\a -> return ())

------------------------------------------------------------------------------
-- Main DB Functions
------------------------------------------------------------------------------

open :: MonadIO m => Maybe FilePath -> m Handle
open r = do
  let root = fromMaybe "data" r
  acid <- liftIO $ openLocalStateFrom (root </> "master") emptyMaster
  s <- liftIO $ newMVar Map.empty
  l <- liftIO $ newMVar False
  let h = Handle DBState { rootDir    = root
                         , master     = acid
                         , masterLock = l
                         , shards     = s
                         }
  checkPending h
  return h

close :: MonadIO m => Handle -> m ()
close h = do
  liftIO $ closeAcidState $ master $ unHandle h
  closeShards h

checkpoint :: MonadIO m => Handle -> m ()
checkpoint = liftIO . createCheckpoint . master . unHandle

archive :: MonadIO m => Handle -> m ()
archive = liftIO . createArchive . master . unHandle

getStats :: MonadIO m => Handle -> m (StatsMaster, [(ItemID, UTCTime, StatsShard)])
getStats h = do
  s <- mquery h GetStatsAcid
  liftIO $ bracket
    (takeMVar $ shards $ unHandle h)
    (putMVar $ shards $ unHandle h)
    (\ss -> do
      sds <- forM (Map.toList ss) $ \(k,(t,a)) -> do
        stats <- liftIO $ query a GetStatsShardAcid
        return (ItemID k, t, stats)
      return (s, sds))

getShardStats :: MonadIO m => Handle -> m (Seq.Seq (ItemID, ShardSize))
getShardStats h = mquery h GetShardsAcid

getCat :: MonadIO m => Handle -> Int -> m (Maybe Cat)
getCat h c = mquery h $ GetCatAcid c

getFeed :: MonadIO m => Handle -> Int -> m (Maybe Feed)
getFeed h f = mquery h $ GetFeedAcid f

getPerson :: MonadIO m => Handle -> Int -> m (Maybe Person)
getPerson h p = mquery h $ GetPersonAcid p

getItem :: MonadIO m => Handle -> Int -> m (Maybe Item)
getItem h k = do
  s <- mquery h $ GetItemShardAcid $ ItemID k
  withShard h s $ \(a, _) ->
    liftIO $ query a $ GetShardItemAcid $ ItemID k

getItemPage :: MonadIO m => Handle -> Int -> Int -> m [Item]
getItemPage h k l = do
  iss <- mquery h $ GetItemPageAcid k l
  mbs <- forM (groupByShard iss) $ \gs ->
    withShard h (snd $ head gs) $ \(a, _) ->
      liftIO $ query a $ GetShardItemsAcid $ fst <$> gs
  return $ fromJust <$> filter (not . null) (concat $ reverse <$> mbs)

getCats :: MonadIO m => Handle -> m (Seq.Seq Cat)
getCats h = mquery h GetCatsAcid

getFeeds :: MonadIO m => Handle -> m (Seq.Seq Feed)
getFeeds h = mquery h GetFeedsAcid

addCat :: MonadIO m => Handle -> Cat -> m Cat
addCat h c = mupdate h $ AddCatAcid c

addFeed :: MonadIO m => Handle -> Feed -> m Feed
addFeed h f = mupdate h $ AddFeedAcid f

addPerson :: MonadIO m => Handle -> Person -> m Person
addPerson h c = mupdate h $ AddPersonAcid c

addItems :: MonadIO m => Handle -> [Item] -> m [Item]
addItems h is = withMasterLock h $ do
  iss <- forM is $ \i -> do
    iid <- mquery h $ MkUniqueIDAcid i     {- TODO: check for self collisions -}
    s   <- mquery h $ GetItemShardAcid iid
    return (i { itemID = iid }, s)
  gss <- forM (groupByShard iss) $ \gs ->
    withShard h (snd $ head gs) $ \(a, _) -> do
      gs' <- forM gs $ \(i, _) -> do
               _ <- mupdate_ h $ AddMasterItemAcid i
               _ <- liftIO $ update a $ AddShardItemAcid i
               mupdate_ h $ UpdatePendingOpAcid NoPendingOp
               return i
      resyncShard h a $ snd $ head gs
      return gs'
  return $ concat gss

wipeDB :: MonadIO m => Handle -> m ()
wipeDB h = do
  ss <- mquery h GetShardsAcid
  mupdate h WipeMasterAcid
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
  as' <- sequence $ addPerson h <$> as
  cs' <- sequence $ addPerson h <$> cs
  let i' = i { itemAuthors      = personID <$> as'
             , itemContributors = personID <$> cs'
             }
  mupdate h $ AddMasterItemAcid i' --TODO: add into shard

addFeedConv :: (MonadIO m, ToFeed f) => Handle -> f -> CatID -> URL -> m Feed
addFeedConv h it cid u = do
  df <- liftIO getCurrentTime
  let (f, as, cs) = toFeed it cid u df
  as' <- sequence $ addPerson h <$> as
  cs' <- sequence $ addPerson h <$> cs
  let f' = f { feedAuthors      = personID <$> as'
             , feedContributors = personID <$> cs'
             }
  mupdate h $ AddFeedAcid f'
