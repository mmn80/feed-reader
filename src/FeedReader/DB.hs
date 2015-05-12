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
import qualified Data.List           as L
import           Data.Maybe          (fromJust, fromMaybe)
import           Data.Time.Clock     (getCurrentTime)
import           FeedReader.DocDB
import           FeedReader.Types
import           Prelude             hiding (filter, lookup)

runLookup :: (Document a, MonadIO m) => Handle -> IntVal -> m (Maybe (DocID a, a))
runLookup h k = fromMaybe Nothing <$>
  runTransaction h (lookupUnsafe k)

runRange :: (Document a, MonadIO m) => Handle -> Maybe IntVal ->
            Property a -> Int -> m [(DocID a, a)]
runRange h s prop pg = fromMaybe [] <$>
  runTransaction h (range s Nothing prop pg)

runFilter :: (Document a, MonadIO m) => Handle -> IntVal -> Maybe IntVal ->
             Property a -> Property a -> Int -> m [(DocID a, a)]
runFilter h k s fprop sprop pg = fromMaybe [] <$>
  runTransaction h (filterUnsafe k s Nothing fprop sprop pg)

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
runDeleteRange h did prop pg = fromMaybe 0 <$>
  runTransaction h (deleteRange did prop pg)

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: MonadIO m => Handle -> m DBStats
getStats h = fromMaybe (DBStats 0 0 0 0) <$>
  runTransaction h (DBStats
    <$> size ("Name"    :: Property Cat)
    <*> size ("Updated" :: Property Feed)
    <*> size ("Name"    :: Property Person)
    <*> size ("Updated" :: Property Item))

clean :: [Maybe a] -> [a]
clean = map fromJust . L.filter (not . null)

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
