{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}

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
module FeedReader.Data.DB
  (
    FeedCategory (..)
  , Feed (..)
  , FeedItem (..)
  , FeedsDB (..)
  , TimeMap
  , insertItem
  , InsertItem (..)
  , getNextItem
  , GetNextItem (..)
  ) where

import qualified Data.Map as M
import Data.Time.Clock (UTCTime)
import Control.Monad.State (get, put)
import Control.Monad.Reader (ask)
import Data.Acid
import Data.SafeCopy
import Data.Typeable
import Control.Applicative-- (<$>)

data FeedCategory = FeedCategory {
    categoryName :: String
  }
  deriving (Eq, Ord, Typeable, Show)

data Feed = Feed {
    feedID       :: Int
  , feedName     :: String
  , feedUrl      :: String
  , feedCategory :: FeedCategory
  }
  deriving (Typeable, Show)

instance Eq Feed where
  (Feed _ n1 _ _) == (Feed _ n2 _ _) = n1 == n2

instance Ord Feed where
  compare (Feed _ n1 _ _) (Feed _ n2 _ _) = compare n1 n2

data FeedItem = FeedItem {
    itemID       :: Int
  , itemFeed     :: Feed
  , itemUrl      :: String
  , itemTitle    :: String
  , itemDate     :: UTCTime
  , itemAuthor   :: String
  , itemSynopsis :: String
  }
  deriving (Typeable, Show)

type TimeMap k = M.Map k (M.Map UTCTime FeedItem)

insertTimeMap k i m = M.insert k (M.insert (itemDate i) i tm') m
  where
    tm' = case M.lookup k m of
            Just tm -> tm
            Nothing -> M.empty

data FeedsDB = FeedsDB {
    idxAll         :: !(M.Map UTCTime FeedItem)
  , idxFeeds       :: !(TimeMap Feed)
  , idxCategories  :: !(TimeMap FeedCategory)
  }
  deriving Typeable

$(deriveSafeCopy 0 'base ''FeedCategory)
$(deriveSafeCopy 0 'base ''Feed)
$(deriveSafeCopy 0 'base ''FeedItem)
$(deriveSafeCopy 0 'base ''FeedsDB)

insertItem :: FeedItem -> Update FeedsDB ()
insertItem item =
  do
    db <- get
    put $ FeedsDB (M.insert (itemDate item) item (idxAll db))
                  (insertTimeMap (itemFeed item) item (idxFeeds db))
                  (insertTimeMap (feedCategory $ itemFeed item) item (idxCategories db))

getNextItem :: UTCTime -> Query FeedsDB (Maybe FeedItem)
getNextItem date =
  do
    db <- ask
    return $ snd <$> M.lookupGT date (idxAll db)

$(makeAcidic ''FeedsDB ['insertItem, 'getNextItem])
