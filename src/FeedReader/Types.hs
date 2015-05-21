{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

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
-- This module provides the core types and functions for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.Types
  ( Cat (..)
  , Feed (..)
  , Person (..)
  , Item (..)
  , URL
  , Language
  , Tag
  , Content (..)
  , Image (..)
  , ToFeed (..)
  , ToPerson (..)
  , ToItem (..)
  , Sortable (..)
  , Unique (..)
  ) where

import           Control.Monad         (liftM)
import           Control.Monad.Trans   (MonadIO)
import           Data.Maybe            (fromMaybe)
import           Data.Serialize        (Get (..), Serialize (..))
import           Data.Time.Clock       (UTCTime (..))
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           Data.Time.LocalTime   (LocalTime)
import           Database.Muesli.Query
import           GHC.Generics          (Generic)

type URL      = String
type Language = String
type Tag      = String

data Content  = Text String | HTML String | XHTML String
  deriving (Show, Generic, Serialize, Indexable)

data Cat = Cat
  { catName   :: Sortable String
  , catParent :: Maybe (Reference Cat)
  } deriving (Show, Generic, Serialize)

instance Document Cat

data Person = Person
  { personName  :: Unique (Sortable String)
  , personURL   :: URL
  , personEmail :: String
  } deriving (Show, Generic, Serialize)

instance Document Person

data Image = Image
  { imageURL         :: URL
  , imageTitle       :: String
  , imageDescription :: String
  , imageLink        :: URL
  , imageWidth       :: Int
  , imageHeight      :: Int
  } deriving (Show, Generic, Serialize, Indexable)

data Feed = Feed
  { feedCatID        :: Reference Cat
  , feedURL          :: Unique URL
  , feedWebURL       :: URL
  , feedTitle        :: Sortable Content
  , feedDescription  :: Content
  , feedLanguage     :: Language
  , feedAuthors      :: [Reference Person]
  , feedContributors :: [Reference Person]
  , feedRights       :: Content
  , feedImage        :: Maybe Image
  , feedUpdated      :: Sortable UTCTime
  } deriving (Show, Generic, Serialize)

instance Document Feed

data Item = Item
  { itemFeedID       :: Reference Feed
  , itemURL          :: Unique URL
  , itemTitle        :: Content
  , itemSummary      :: Content
  , itemTags         :: [Sortable Tag]
  , itemAuthors      :: [Reference Person]
  , itemContributors :: [Reference Person]
  , itemRights       :: Content
  , itemContent      :: Content
  , itemPublished    :: Sortable UTCTime
  , itemUpdated      :: Sortable UTCTime
  } deriving (Show, Generic, Serialize)

instance Document Item

instance Serialize UTCTime where
  put = put . toRational . utcTimeToPOSIXSeconds
  get = liftM (posixSecondsToUTCTime . fromRational) get

class ToFeed f where
  toFeed :: MonadIO m => f -> Reference Cat -> URL -> UTCTime ->
            Transaction m (Reference Feed, Feed)

class ToPerson p where
  toPerson :: MonadIO m => p -> Transaction m (Reference Person, Person)

class ToItem i where
  toItem :: MonadIO m => i -> Reference Feed -> UTCTime ->
            Transaction m (Reference Item, Item)
