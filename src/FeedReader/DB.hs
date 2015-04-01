{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}

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
  (
    UserCategory (..)
  , Feed (..)
  , Item (..)
  , URL
  , Language
  , Tag
  , Content (..)
  , Person (..)
  , Image (..)
  , FeedsDB (..)
  , NestedMap
  , DBStats (..)
  , Feed2DB (..)
  , text2UTCTime
  , imageFromURL
  , getStats
  , GetStats (..)
  , getNextItem
  , GetNextItem (..)
  , insertItem
  , InsertItem (..)
  , insertFeed
  , InsertFeed (..)
  , wipeDB
  , WipeDB (..)
  ) where

import           Control.Monad.Reader  (ask)
import           Control.Monad.State   (get, put)
import           Data.Acid
import           Data.Acid.Advanced
import           Data.Hashable         (hash)
import qualified Data.IntMap           as M
import           Data.Maybe            (fromMaybe)
import           Data.Monoid           (First (..), getFirst, (<>))
import           Data.SafeCopy
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Time.Format      (defaultTimeLocale, iso8601DateFormat,
                                        parseTimeM, rfc822DateFormat)

------------------------------------------------------------------------------
-- Record Types
------------------------------------------------------------------------------

type URL      = String
type Language = String
type Tag      = String
data Content  = Text String | HTML String | XHTML String
  deriving (Show)

data UserCategory = UserCategory
  { catID   :: Int
  , catName :: String
  } deriving (Show)

data Person = Person
  { personID    :: Int
  , personName  :: String
  , personURL   :: URL
  , personEmail :: String
  } deriving (Show)

data Image = Image
  { imageURL         :: URL
  , imageTitle       :: String
  , imageDescription :: String
  , imageLink        :: URL
  , imageWidth       :: Int
  , imageHeight      :: Int
  } deriving (Show)

data Feed = Feed
  { feedID           :: Int
  , feedCat          :: UserCategory
  , feedURL          :: URL
  , feedTitle        :: Content
  , feedDescription  :: Content
  , feedLanguage     :: Language
  , feedAuthors      :: [Person]
  , feedContributors :: [Person]
  , feedRights       :: Content
  , feedImage        :: Maybe Image
  , feedUpdated      :: UTCTime
  } deriving (Show)

data Item = Item
  { itemID           :: Int
  , itemFeed         :: Feed
  , itemURL          :: URL
  , itemTitle        :: Content
  , itemSummary      :: Content
  , itemTags         :: [Tag]
  , itemAuthors      :: [Person]
  , itemContributors :: [Person]
  , itemRights       :: Content
  , itemContent      :: Content
  , itemPublished    :: UTCTime
  , itemUpdated      :: UTCTime
  } deriving (Show)

------------------------------------------------------------------------------
-- Conversion Class & Utilities
------------------------------------------------------------------------------

class Feed2DB f i where
  feed2DB :: f -> UserCategory -> URL -> UTCTime -> Feed
  item2DB :: i -> Feed -> URL -> UTCTime -> Item

text2UTCTime :: String -> UTCTime -> UTCTime
text2UTCTime t df = fromMaybe df $ getFirst $ iso <> iso' <> rfc
  where
    iso  = tryParse $ iso8601DateFormat $ Just "%H:%M:%S"
    iso' = tryParse $ iso8601DateFormat Nothing
    rfc  = tryParse rfc822DateFormat
    tryParse f = First $ parseTimeM True defaultTimeLocale f t

imageFromURL :: URL -> Image
imageFromURL u = Image
  { imageURL         = u
  , imageTitle       = ""
  , imageDescription = ""
  , imageLink        = u
  , imageWidth       = 0
  , imageHeight      = 0
  }

updatePersonID :: Person -> Person
updatePersonID p = p { personID = hash $ personName p ++ personEmail p }

calcFeedID :: Feed -> Int
calcFeedID f = hash $ feedURL f

------------------------------------------------------------------------------
-- DataBase Index Types
------------------------------------------------------------------------------

type NestedMap = M.IntMap (M.IntMap Item)

insertNested :: Int -> Item -> NestedMap -> NestedMap
insertNested k i m = M.insert k newInner m
  where
    newInner = M.insert (itemID i) i $ fromMaybe M.empty $ M.lookup k m

data FeedsDB = FeedsDB {
    tblItems        :: !(M.IntMap Item)
  , idxItemsByFeeds :: !NestedMap
  , idxItemsByCats  :: !NestedMap
  }

checkUniqueID idx x = if M.notMember x idx then x
                      else checkUniqueID idx (x + 1)

data DBStats = DBStats
  { countCats  :: Int
  , countFeeds :: Int
  , countItems :: Int
  }

------------------------------------------------------------------------------
-- SafeCopy Instances
------------------------------------------------------------------------------

$(deriveSafeCopy 0 'base ''Content)
$(deriveSafeCopy 0 'base ''UserCategory)
$(deriveSafeCopy 0 'base ''Person)
$(deriveSafeCopy 0 'base ''Image)
$(deriveSafeCopy 0 'base ''Feed)
$(deriveSafeCopy 0 'base ''Item)
$(deriveSafeCopy 0 'base ''DBStats)
$(deriveSafeCopy 0 'base ''FeedsDB)

------------------------------------------------------------------------------
-- Queries
------------------------------------------------------------------------------

getStats :: Query FeedsDB DBStats
getStats = do
  db <- ask
  return DBStats
           { countCats  = M.size $ idxItemsByCats db
           , countFeeds = M.size $ idxItemsByFeeds db
           , countItems = M.size $ tblItems db
           }

getNextItem :: Int -> Query FeedsDB (Maybe Item)
getNextItem k = do
  db <- ask
  return $ snd <$> M.lookupGT k (tblItems db)

insertItem :: Item -> Update FeedsDB ()
insertItem i = do
  db <- get
  let ix = fromInteger $ round $ utcTimeToPOSIXSeconds $ itemUpdated i
  let f = itemFeed i
  let f' = f { feedID = calcFeedID f
             , feedAuthors = updatePersonID <$> feedAuthors f
             , feedContributors = updatePersonID <$> feedContributors f
             }
  let i' = i { itemID = checkUniqueID (tblItems db) ix
             , itemFeed = f'
             , itemAuthors = updatePersonID <$> itemAuthors i
             , itemContributors = updatePersonID <$> itemContributors i
             }
  put $ FeedsDB (M.insert     (itemID i') i' $ tblItems db)
                (insertNested (feedID f') i' $ idxItemsByFeeds db)
                (insertNested (catID $ feedCat f') i' $ idxItemsByCats db)

insertFeed :: Feed -> Update FeedsDB ()
insertFeed f = do
  db <- get
  let f' = f { feedID = calcFeedID f
             , feedAuthors = updatePersonID <$> feedAuthors f
             , feedContributors = updatePersonID <$> feedContributors f
             }
  put $ FeedsDB (tblItems db)
                (idxItemsByFeeds db)
                (idxItemsByCats db)

wipeDB :: Update FeedsDB ()
wipeDB = do
  db <- get
  put $ FeedsDB M.empty M.empty M.empty



------------------------------------------------------------------------------
-- makeAcidic
------------------------------------------------------------------------------

-- getStats

data GetStats = GetStats

$(deriveSafeCopy 0 'base ''GetStats)

instance Method GetStats where
  type MethodResult GetStats = DBStats
  type MethodState GetStats = FeedsDB

instance QueryEvent GetStats

-- getNextItem

data GetNextItem = GetNextItem Int

$(deriveSafeCopy 0 'base ''GetNextItem)

instance Method GetNextItem where
  type MethodResult GetNextItem = Maybe Item
  type MethodState GetNextItem = FeedsDB

instance QueryEvent GetNextItem

-- insertItem

data InsertItem = InsertItem Item

$(deriveSafeCopy 0 'base ''InsertItem)

instance Method InsertItem where
  type MethodResult InsertItem = ()
  type MethodState InsertItem = FeedsDB

instance UpdateEvent InsertItem

-- insertFeed

data InsertFeed = InsertFeed Feed

$(deriveSafeCopy 0 'base ''InsertFeed)

instance Method InsertFeed where
  type MethodResult InsertFeed = ()
  type MethodState InsertFeed = FeedsDB

instance UpdateEvent InsertFeed

-- wipeDB

data WipeDB = WipeDB

$(deriveSafeCopy 0 'base ''WipeDB)

instance Method WipeDB where
  type MethodResult WipeDB = ()
  type MethodState WipeDB = FeedsDB

instance UpdateEvent WipeDB

-- FeedsDB

instance IsAcidic FeedsDB where
  acidEvents = [ UpdateEvent (\(InsertItem i)  -> insertItem i)
               , UpdateEvent (\(InsertFeed f)  -> insertFeed f)
               , UpdateEvent (\ WipeDB         -> wipeDB)
               , QueryEvent  (\ GetStats       -> getStats)
               , QueryEvent  (\(GetNextItem k) -> getNextItem k)
               ]
