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
  , text2UTCTime
  , imageFromURL
  , diffMs
  ) where

import           Control.Applicative   ((<|>))
import           Control.Monad         (liftM)
import           Data.Maybe            (fromMaybe)
import           Data.Serialize        (Get (..), Serialize (..))
import           Data.Time.Clock       (UTCTime, diffUTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           Data.Time.Format      (defaultTimeLocale, iso8601DateFormat,
                                        parseTimeM, rfc822DateFormat)
import           FeedReader.DocDB      (DocID, DocRefList (..), Document (..),
                                        IntValList (..), string2IntVal,
                                        utcTime2IntVal)
import           GHC.Generics          (Generic)

type URL      = String
type Language = String
type Tag      = String

data Content  = Text String | HTML String | XHTML String
  deriving (Show, Generic)

instance Serialize Content

getContentText (Text s) = s
getContentText (HTML s) = s
getContentText (XHTML s) = s

data Cat = Cat
  { catName :: String
  } deriving (Show, Generic)

instance Serialize Cat

instance Document Cat where
  getIntProps = [ "Name" ]
  getIntVals a = [ IntValList "Name" [ string2IntVal $ catName a ] ]

data Person = Person
  { personName  :: String
  , personURL   :: URL
  , personEmail :: String
  } deriving (Show, Generic)

instance Serialize Person

instance Document Person where
  getIntProps = [ "Name" ]
  getIntVals a = [ IntValList "Name" [ string2IntVal $ personName a ] ]

data Image = Image
  { imageURL         :: URL
  , imageTitle       :: String
  , imageDescription :: String
  , imageLink        :: URL
  , imageWidth       :: Int
  , imageHeight      :: Int
  } deriving (Show, Generic)

instance Serialize Image

data Feed = Feed
  { feedCatID        :: DocID Cat
  , feedURL          :: URL
  , feedTitle        :: Content
  , feedDescription  :: Content
  , feedLanguage     :: Language
  , feedAuthors      :: [DocID Person]
  , feedContributors :: [DocID Person]
  , feedRights       :: Content
  , feedImage        :: Maybe Image
  , feedUpdated      :: UTCTime
  } deriving (Show, Generic)

instance Serialize Feed

instance Document Feed where
  getIntProps = [ "Updated", "Title" ]
  getIntVals a = [ IntValList "Updated" [ utcTime2IntVal $ feedUpdated a ]
                 , IntValList "Title" [ string2IntVal . getContentText $ feedTitle a ]
                 ]
  getRefProps = [ "CatID", "Authors", "Contributors" ]
  getDocRefs a = [ DocRefList "CatID"        [ feedCatID a ]
                 , DocRefList "Authors"      $ feedAuthors a
                 , DocRefList "Contributors" $ feedContributors a
                 ]

data Item = Item
  { itemFeedID       :: DocID Feed
  , itemURL          :: URL
  , itemTitle        :: Content
  , itemSummary      :: Content
  , itemTags         :: [Tag]
  , itemAuthors      :: [DocID Person]
  , itemContributors :: [DocID Person]
  , itemRights       :: Content
  , itemContent      :: Content
  , itemPublished    :: UTCTime
  , itemUpdated      :: UTCTime
  } deriving (Show, Generic)

instance Serialize UTCTime where
  put t = put . toRational $ utcTimeToPOSIXSeconds t
  get = liftM (posixSecondsToUTCTime . fromRational) get

instance Serialize Item

instance Document Item where
  getIntProps = [ "Updated", "Published" ]
  getIntVals a = [ IntValList "Published" [ utcTime2IntVal $ itemPublished a ]
                 , IntValList "Updated"   [ utcTime2IntVal $ itemUpdated a ]
                 ]
  getRefProps = [ "FeedID", "Authors", "Contributors" ]
  getDocRefs a = [ DocRefList "FeedID"       [ itemFeedID a ]
                 , DocRefList "Authors"      $ itemAuthors a
                 , DocRefList "Contributors" $ itemContributors a
                 ]

class ToFeed f where
  toFeed :: f -> DocID Cat -> URL -> UTCTime -> (Feed, [Person], [Person])

class ToPerson p where
  toPerson :: p -> Person

class ToItem i where
  toItem :: i -> DocID Feed -> URL -> UTCTime -> (Item, [Person], [Person])

text2UTCTime :: String -> UTCTime -> UTCTime
text2UTCTime t df = fromMaybe df $ iso <|> iso' <|> rfc
  where
    iso  = tryParse . iso8601DateFormat $ Just "%H:%M:%S"
    iso' = tryParse $ iso8601DateFormat Nothing
    rfc  = tryParse rfc822DateFormat
    tryParse f = parseTimeM True defaultTimeLocale f t

diffMs :: UTCTime -> UTCTime -> Float
diffMs t0 t1 = 1000 * fromRational (toRational $ diffUTCTime t1 t0)

imageFromURL :: URL -> Image
imageFromURL u = Image
  { imageURL         = u
  , imageTitle       = ""
  , imageDescription = ""
  , imageLink        = u
  , imageWidth       = 0
  , imageHeight      = 0
  }
