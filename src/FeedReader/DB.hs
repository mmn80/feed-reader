{-# LANGUAGE OverloadedStrings #-}

module FeedReader.DB
  ( module FeedReader.Types
  , module FeedReader.DocDB
  , getStats
  , DBStats (..)
  , addItemConv
  , addFeedConv
  ) where

import           Control.Monad.Trans (MonadIO (liftIO))
import           Data.Time.Clock     (getCurrentTime)
import           FeedReader.DocDB
import           FeedReader.Types

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

addItemConv :: (MonadIO m, ToItem i) => Handle -> i -> DocID Feed -> URL -> m Item
addItemConv h it fid u = do
  df <- liftIO getCurrentTime
  let (i, as, cs) = toItem it fid u df
  as' <- sequence $ runInsert h <$> as
  cs' <- sequence $ runInsert h <$> cs
  let i' = i { itemAuthors      = as'
             , itemContributors = cs'
             }
  runInsert h i'
  return i'

addFeedConv :: (MonadIO m, ToFeed f) => Handle -> f -> DocID Cat -> URL -> m Feed
addFeedConv h it cid u = do
  df <- liftIO getCurrentTime
  let (f, as, cs) = toFeed it cid u df
  as' <- sequence $ runInsert h <$> as
  cs' <- sequence $ runInsert h <$> cs
  let f' = f { feedAuthors      = as'
             , feedContributors = cs'
             }
  runInsert h f'
  return f'
