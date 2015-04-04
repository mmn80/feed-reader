{-# LANGUAGE TypeFamilies    #-}

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

import           Control.Concurrent    (MVar, modifyMVar_, newMVar, putMVar,
                                        takeMVar)
import           Control.Exception     (bracket)
import           Control.Monad         (forM, forM_)
import           Control.Monad.Trans   (MonadIO (liftIO))
import           Data.Acid
import           Data.Acid.Advanced
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import           Data.List             (groupBy, sortBy)
import           Data.Maybe            (fromJust, fromMaybe)
import qualified Data.Sequence         as Seq
import           Data.Time.Clock       (UTCTime, getCurrentTime)
import           FeedReader.Types
import           FeedReader.Queries
import           System.Directory      (doesDirectoryExist,
                                        removeDirectoryRecursive)
import           System.FilePath       ((</>))


type OpenedShards = Map.IntMap (UTCTime, AcidState Shard)

data DBState = DBState
  { rootDir :: FilePath
  , master  :: AcidState Master
  , shards  :: MVar OpenedShards
  }

instance Eq DBState where
  DBState r1 _ _ == DBState r2 _ _ = r1 == r2

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle (DBState r1 _ _)) = r1

maxOpenedShards :: Int
maxOpenedShards = 5

maxShardItems :: Int
maxShardItems = 100

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

mquery :: (MonadIO m, QueryEvent e, MethodState e ~ Master) =>
          Handle -> e -> m (EventResult e)
mquery  h = liftIO . query  (master $ unHandle h)

mupdate :: (MonadIO m, UpdateEvent e, MethodState e ~ Master) =>
          Handle -> e -> m (EventResult e)
mupdate h = liftIO . update (master $ unHandle h)

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
  mupdate h $ AddMasterItemAcid i'

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

------------------------------------------------------------------------------
-- Main DB Functions
------------------------------------------------------------------------------

open :: MonadIO m => Maybe FilePath -> m Handle
open r = do
  let root = fromMaybe "data" r
  acid <- liftIO $ openLocalStateFrom (root </> "master") emptyMaster
  s <- liftIO $ newMVar Map.empty
  return $ Handle DBState { rootDir = root
                          , master  = acid
                          , shards  = s
                          }

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
addItems h is = do
  iss <- forM is $ \i -> do
    iid <- mquery h $ MkUniqueIDAcid i
    s   <- mquery h $ GetItemShardAcid iid
    return (i { itemID = iid }, s)
  gss <- forM (groupByShard iss) $ \gs ->
    withShard h (snd $ head gs) $ \(a, _) ->
      forM gs $ \(i, _) -> do
        _ <- liftIO $ update a $ AddShardItemAcid i
        _ <- mupdate h $ AddMasterItemAcid i
        return i
  return $ concat gss

wipeDB :: MonadIO m => Handle -> m ()
wipeDB h = do
  ss <- mquery h GetShItemAcid
  mupdate h WipeMasterAcid
  closeShards h
  let ss' = 0 : Set.toList ss
  forM_ ss' $ \s -> do
    let sf = shardPath h $ ItemID s
    ex <- liftIO $ doesDirectoryExist sf
    if ex then liftIO $ removeDirectoryRecursive sf
    else liftIO $ putStrLn $ "  Shard dir not found: " ++ sf
