{-# LANGUAGE OverloadedStrings #-}

module FeedReader.DB
  ( module FeedReader.Types
  , module FeedReader.DocDB
  , runRange
  , runFilter
  , runLookup
  , runInsert
  , runDelete
  , runDeleteRange
  , getStats
  , DBStats (..)
  , addItemConv
  , addFeedConv
  ) where

import           Control.Monad       (forM_)
import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.Maybe          (fromJust, fromMaybe)
import           Data.Time.Clock     (getCurrentTime)
import           FeedReader.DocDB
import           FeedReader.Types
import           Prelude             hiding (filter, lookup)

runLookup :: (Document a, MonadIO m) => Handle -> IntVal -> m (Maybe (DocID a, a))
runLookup h k = do
  mbb <- runTransaction h $
    lookupUnsafe k
  return $ fromMaybe Nothing mbb

runRange :: (Document a, MonadIO m) => Handle -> Maybe IntVal ->
            Property a -> Int -> m [(DocID a, a)]
runRange h s prop pg = do
  mb <- runTransaction h $
    range s Nothing prop pg
  return $ fromMaybe [] mb

runFilter :: (Document a, MonadIO m) => Handle -> IntVal -> Maybe IntVal ->
             Property a -> Property a -> Int -> m [(DocID a, a)]
runFilter h k s fprop sprop pg = do
  mb <- runTransaction h $
    filterUnsafe k s Nothing fprop sprop pg
  return $ fromMaybe [] mb

runInsert :: (Document a, MonadIO m) => Handle -> a -> m (Maybe (DocID a))
runInsert h a = runTransaction h $ insert a

runDelete :: (MonadIO m) => Handle -> IntVal -> m ()
runDelete h did = do
  runTransaction h $ deleteUnsafe did
  return ()

deleteRange :: (Document a, MonadIO m) => IntVal -> Property a -> Int ->
               Transaction m Int
deleteRange did prop pg = do
  ks <- rangeKUnsafe (Just did) Nothing prop pg
  forM_ ks $ \k -> delete k
  return $ length ks

runDeleteRange :: (Document a, MonadIO m) => Handle -> IntVal ->
                  Property a -> Int -> m Int
runDeleteRange h did prop pg = do
  ms <- runTransaction h $ deleteRange did prop pg
  return $ fromMaybe 0 ms

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: MonadIO m => Handle -> m DBStats
getStats h = do
  mb <- runTransaction h $ DBStats
    <$> size ("Name"    :: Property Cat)
    <*> size ("Updated" :: Property Feed)
    <*> size ("Name"    :: Property Person)
    <*> size ("Updated" :: Property Item)
  return $ fromMaybe (DBStats 0 0 0 0) mb

clean :: [Maybe a] -> [a]
clean as = [ fromJust x | x <- as, not $ null x ]

addItemConv :: (MonadIO m, ToItem i) => Handle -> i -> DocID Feed -> URL -> m Item
addItemConv h it fid u = do
  df <- liftIO getCurrentTime
  let (i, as, cs) = toItem it fid u df
  as' <- sequence $ runInsert h <$> as
  cs' <- sequence $ runInsert h <$> cs
  let i' = i { itemAuthors      = clean as'
             , itemContributors = clean cs'
             }
  runInsert h i'
  return i'

addFeedConv :: (MonadIO m, ToFeed f) => Handle -> f -> DocID Cat -> URL -> m Feed
addFeedConv h it cid u = do
  df <- liftIO getCurrentTime
  let (f, as, cs) = toFeed it cid u df
  as' <- sequence $ runInsert h <$> as
  cs' <- sequence $ runInsert h <$> cs
  let f' = f { feedAuthors      = clean as'
             , feedContributors = clean cs'
             }
  runInsert h f'
  return f'
