{-# LANGUAGE OverloadedStrings #-}

module FeedReader.DB
  ( module FeedReader.Types
  , module FeedReader.DocDB
  , runPage
  , runLookup
  , runInsert
  , getStats
  , DBStats (..)
  , addItemConv
  , addFeedConv
  ) where

import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.Maybe          (fromJust)
import           Data.Time.Clock     (getCurrentTime)
import           FeedReader.DocDB
import           FeedReader.Types
import           Prelude             hiding (lookup)

runLookup :: (Document a, MonadIO m) => Handle -> ExtID a -> m (Maybe (DocID a, a))
runLookup h k = do
  mbb <- runTransaction h $
    lookup k
  case mbb of
    Nothing  -> return Nothing
    Just mba -> return mba

runPage :: (Document a, MonadIO m) => Handle -> Maybe (ExtID a) ->
           Property a -> Int -> m [(DocID a, a)]
runPage h k prop pg = do
  mb <- runTransaction h $
    page k prop pg
  case mb of
    Nothing -> return []
    Just ps -> return ps

runInsert :: (Document a, MonadIO m) => Handle -> a -> m (Maybe (DocID a))
runInsert h a = runTransaction h $ insert a

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: MonadIO m => Handle -> m DBStats
getStats h = do
  mb <- runTransaction h $ DBStats
    <$> size ("ID"      :: Property Cat)
    <*> size ("Updated" :: Property Feed)
    <*> size ("ID"      :: Property Person)
    <*> size ("Updated" :: Property Item)
  case mb of
    Nothing -> return $ DBStats 0 0 0 0
    Just s  -> return s

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
