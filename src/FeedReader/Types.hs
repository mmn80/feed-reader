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
  , Indexable (..)
  , Unique (..)
  ) where

import           Control.Monad         (liftM)
import           Control.Monad.Trans   (MonadIO)
import           Data.Maybe            (fromMaybe)
import           Data.Serialize        (Get (..), Serialize (..))
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           FeedReader.DocDB      (DBValue, DocID, Document (..),
                                        Indexable (..), Transaction,
                                        Unique (..))
import           GHC.Generics          (Generic)

type URL      = String
type Language = String
type Tag      = String

data Content  = Text String | HTML String | XHTML String
  deriving (Show, Generic, Serialize, DBValue)

data Cat = Cat
  { catName   :: Indexable String
  , catParent :: Maybe (DocID Cat)
  } deriving (Show, Generic, Serialize)

instance Document Cat

data Person = Person
  { personName  :: Unique (Indexable String)
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
  } deriving (Show, Generic, Serialize, DBValue)

data Feed = Feed
  { feedCatID        :: DocID Cat
  , feedURL          :: Unique URL
  , feedWebURL       :: URL
  , feedTitle        :: Indexable Content
  , feedDescription  :: Content
  , feedLanguage     :: Language
  , feedAuthors      :: [DocID Person]
  , feedContributors :: [DocID Person]
  , feedRights       :: Content
  , feedImage        :: Maybe Image
  , feedUpdated      :: Indexable UTCTime
  } deriving (Show, Generic, Serialize)

instance Document Feed

data Item = Item
  { itemFeedID       :: DocID Feed
  , itemURL          :: Unique URL
  , itemTitle        :: Content
  , itemSummary      :: Content
  , itemTags         :: [Indexable Tag]
  , itemAuthors      :: [DocID Person]
  , itemContributors :: [DocID Person]
  , itemRights       :: Content
  , itemContent      :: Content
  , itemPublished    :: Indexable UTCTime
  , itemUpdated      :: Indexable UTCTime
  } deriving (Show, Generic, Serialize)

instance Document Item

instance Serialize UTCTime where
  put = put . toRational . utcTimeToPOSIXSeconds
  get = liftM (posixSecondsToUTCTime . fromRational) get

class ToFeed f where
  toFeed :: MonadIO m => f -> DocID Cat -> URL -> UTCTime ->
            Transaction m (DocID Feed, Feed)

class ToPerson p where
  toPerson :: MonadIO m => p -> Transaction m (DocID Person, Person)

class ToItem i where
  toItem :: MonadIO m => i -> DocID Feed -> UTCTime ->
            Transaction m (DocID Item, Item)
