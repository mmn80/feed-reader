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
  , runToItem
  , runToFeed
  ) where

import           Control.Monad       (forM_)
import           Control.Monad.Trans (MonadIO (liftIO))
import qualified Data.List           as L
import           Data.Maybe          (fromJust, fromMaybe)
import           Data.Time.Clock     (getCurrentTime)
import           FeedReader.Convert
import           FeedReader.DocDB
import           FeedReader.Types
import           Prelude             hiding (filter, lookup)

runLookup :: (Document a, MonadIO m) => Handle -> DocID a -> m (Maybe (DocID a, a))
runLookup h k = fromMaybe Nothing <$>
  runTransaction h (lookup k)

runRange :: (Document a, MonadIO m) => Handle -> Maybe (IntVal b) ->
            Property a -> Int -> m [(DocID a, a)]
runRange h s prop pg = fromMaybe [] <$>
  runTransaction h (range s Nothing prop pg)

runFilter :: (Document a, MonadIO m) => Handle -> DocID b -> Maybe (IntVal c) ->
             Property a -> Property a -> Int -> m [(DocID a, a)]
runFilter h k s fprop sprop pg = fromMaybe [] <$>
  runTransaction h (filter k s Nothing fprop sprop pg)

runInsert :: (Document a, MonadIO m) => Handle -> a -> m (Maybe (DocID a))
runInsert h a = runTransaction h $ insert a

runDelete :: MonadIO m => Handle -> DocID a -> m ()
runDelete h did = do
  runTransaction h $ delete did
  return ()

deleteRange :: (Document a, MonadIO m) => IntVal b -> Property a -> Int ->
               Transaction m Int
deleteRange did prop pg = do
  ks <- rangeK (Just did) Nothing prop pg
  forM_ ks $ \k -> delete k
  return $ length ks

runDeleteRange :: (Document a, MonadIO m) => Handle -> IntVal b -> Property a ->
                  Int -> m Int
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
    <$> size ("catName"     :: Property Cat)
    <*> size ("feedUpdated" :: Property Feed)
    <*> size ("personName"  :: Property Person)
    <*> size ("itemUpdated" :: Property Item))

clean :: [Maybe a] -> [a]
clean = map fromJust . L.filter (not . null)

runToItem :: (MonadIO m, ToItem i) => Handle -> i -> DocID Feed -> URL ->
             m (Maybe (DocID Item, Item))
runToItem h it fid u = do
  df <- liftIO getCurrentTime
  runTransaction h $ toItem it fid u df

runToFeed :: (MonadIO m, ToFeed f) => Handle -> f -> DocID Cat -> URL ->
             m (Maybe (DocID Feed, Feed))
runToFeed h it cid u = do
  df <- liftIO getCurrentTime
  runTransaction h $ toFeed it cid u df
