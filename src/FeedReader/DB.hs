{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies    #-}

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
    Cat    (..), CatID,    unsetCatID
  , Feed   (..), FeedID,   unsetFeedID
  , Person (..), PersonID, unsetPersonID
  , Item   (..), ItemID,   unsetItemID
  , URL, Language, Tag, Content (..)
  , Image   (..), imageFromURL
  , FeedsDB (..)
  , ToFeed   (..)
  , ToPerson (..)
  , ToItem   (..)
  , emptyDB
  , text2UTCTime
  , getStats,     GetStats     (..), DBStats (..)
  , lookupCat,    LookupCat    (..)
  , lookupFeed,   LookupFeed   (..)
  , lookupPerson, LookupPerson (..)
  , lookupItem,   LookupItem   (..)
  , cats2Seq,     Cats2Seq     (..)
  , feeds2Seq,    Feeds2Seq    (..)
  , insertCat,    InsertCat    (..)
  , insertFeed,   InsertFeed   (..)
  , insertPerson, InsertPerson (..)
  , insertItem,   InsertItem   (..)
  , wipeDB,       WipeDB       (..)
  ) where

import           Control.Monad.Reader  (ask)
import           Control.Monad.State   (get, put)
import           Data.Acid
import           Data.Acid.Advanced
import           Data.Hashable         (hash)
import qualified Data.IntMap           as M
import qualified Data.IntSet           as Set
import           Data.Maybe            (fromJust, fromMaybe)
import           Data.Monoid           (First (..), getFirst, (<>))
import           Data.SafeCopy
import qualified Data.Sequence         as Seq
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

newtype CatID = CatID { unCatID :: Int } deriving (Show)

data Cat = Cat
  { catID   :: CatID
  , catName :: String
  } deriving (Show)

newtype PersonID = PersonID { unPersonID :: Int } deriving (Show)

data Person = Person
  { personID    :: PersonID
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

newtype FeedID = FeedID { unFeedID :: Int } deriving (Show)

data Feed = Feed
  { feedID           :: FeedID
  , feedCatID        :: CatID
  , feedURL          :: URL
  , feedTitle        :: Content
  , feedDescription  :: Content
  , feedLanguage     :: Language
  , feedAuthors      :: [PersonID]
  , feedContributors :: [PersonID]
  , feedRights       :: Content
  , feedImage        :: Maybe Image
  , feedUpdated      :: UTCTime
  } deriving (Show)

newtype ItemID = ItemID { unItemID :: Int } deriving (Show)

data Item = Item
  { itemID           :: ItemID
  , itemFeedID       :: FeedID
  , itemURL          :: URL
  , itemTitle        :: Content
  , itemSummary      :: Content
  , itemTags         :: [Tag]
  , itemAuthors      :: [PersonID]
  , itemContributors :: [PersonID]
  , itemRights       :: Content
  , itemContent      :: Content
  , itemPublished    :: UTCTime
  , itemUpdated      :: UTCTime
  } deriving (Show)

------------------------------------------------------------------------------
-- Conversion Classes & Utilities
------------------------------------------------------------------------------

class ToFeed f where
  toFeed :: f -> CatID -> URL -> UTCTime -> (Feed, [Person], [Person])

class ToPerson p where
  toPerson :: p -> Person

class ToItem i where
  toItem :: i -> FeedID -> URL -> UTCTime -> (Item, [Person], [Person])

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

calcCatID :: Cat -> CatID
calcCatID = CatID . hash . catName

unsetCatID :: CatID
unsetCatID = CatID 0

calcFeedID :: Feed -> FeedID
calcFeedID = FeedID . hash . feedURL

unsetFeedID :: FeedID
unsetFeedID = FeedID 0

calcItemID :: Item -> ItemID
calcItemID = ItemID . fromInteger . round . utcTimeToPOSIXSeconds . itemUpdated

unsetItemID :: ItemID
unsetItemID = ItemID 0

calcPersonID :: Person -> PersonID
calcPersonID p = PersonID $ hash $ personName p ++ personEmail p

unsetPersonID :: PersonID
unsetPersonID = PersonID 0

------------------------------------------------------------------------------
-- DataBase Index Types
------------------------------------------------------------------------------

type NestedMap = M.IntMap Set.IntSet

insertNested :: Int -> Int -> NestedMap -> NestedMap
insertNested k i m = M.insert k newInner m
  where
    newInner = Set.insert i $ fromMaybe Set.empty $ M.lookup k m

data FeedsDB = FeedsDB {
    tblCats    :: !(M.IntMap Cat)
  , tblFeeds   :: !(M.IntMap Feed)
  , tblPersons :: !(M.IntMap Person)
  , tblItems   :: !(M.IntMap Item)
  , idxCats    :: !NestedMap
  , idxFeeds   :: !NestedMap
  }

emptyDB = FeedsDB M.empty M.empty M.empty M.empty M.empty M.empty

checkUniqueID idx x = if M.notMember x idx then x
                      else checkUniqueID idx (x + 1)

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  }

------------------------------------------------------------------------------
-- SafeCopy Instances
------------------------------------------------------------------------------

$(deriveSafeCopy 0 'base ''Content)
$(deriveSafeCopy 0 'base ''CatID)
$(deriveSafeCopy 0 'base ''Cat)
$(deriveSafeCopy 0 'base ''PersonID)
$(deriveSafeCopy 0 'base ''Person)
$(deriveSafeCopy 0 'base ''Image)
$(deriveSafeCopy 0 'base ''FeedID)
$(deriveSafeCopy 0 'base ''Feed)
$(deriveSafeCopy 0 'base ''ItemID)
$(deriveSafeCopy 0 'base ''Item)
$(deriveSafeCopy 0 'base ''DBStats)
$(deriveSafeCopy 0 'base ''FeedsDB)

------------------------------------------------------------------------------
-- Read Queries
------------------------------------------------------------------------------

getStats :: Query FeedsDB DBStats
getStats = do
  db <- ask
  return DBStats
           { countCats    = M.size $ tblCats  db
           , countFeeds   = M.size $ tblFeeds db
           , countPersons = M.size $ tblPersons db
           , countItems   = M.size $ tblItems db
           }

cats2Seq :: Query FeedsDB (Seq.Seq Cat)
cats2Seq = do
  db <- ask
  return $ M.foldr (flip (Seq.|>)) Seq.empty $ tblCats db

feeds2Seq :: Query FeedsDB (Seq.Seq Feed)
feeds2Seq = do
  db <- ask
  return $ M.foldr (flip (Seq.|>)) Seq.empty $ tblFeeds db

lookupCat :: Int -> Query FeedsDB (Maybe Cat)
lookupCat k = do
  db <- ask
  return $ M.lookup k (tblCats db)

lookupFeed :: Int -> Query FeedsDB (Maybe Feed)
lookupFeed k = do
  db <- ask
  return $ M.lookup k (tblFeeds db)

lookupPerson :: Int -> Query FeedsDB (Maybe Person)
lookupPerson k = do
  db <- ask
  return $ M.lookup k (tblPersons db)

lookupItem :: Int -> Query FeedsDB (Maybe Item)
lookupItem k = do
  db <- ask
  return $ snd <$> M.lookupGT k (tblItems db)

------------------------------------------------------------------------------
-- Update Queries
------------------------------------------------------------------------------

insertCat :: Cat -> Update FeedsDB Cat
insertCat c = do
  db <- get
  let cid = calcCatID c
  let c' = c { catID = cid }
  put $ db { tblCats = M.insert (unCatID cid) c' $ tblCats db }
  return c'

insertFeed :: Feed -> Update FeedsDB Feed
insertFeed f = do
  db <- get
  let fid = calcFeedID f
  let f' = f { feedID = fid }
  put $ db { tblFeeds = M.insert (unFeedID fid) f' $ tblFeeds db }
  return f'

insertPerson :: Person -> Update FeedsDB Person
insertPerson p = do
  db <- get
  let pid = calcPersonID p
  let p' = p { personID = pid }
  put $ db { tblPersons = M.insert (unPersonID pid) p' $ tblPersons db }
  return p'

insertItem :: Item -> Update FeedsDB Item
insertItem i = do
  db <- get
  let iid = checkUniqueID (tblItems db) $ unItemID $ calcItemID i
  let i' = i { itemID = ItemID iid }
  let fid = unFeedID $ itemFeedID i'
  let mbf = M.lookup fid (tblFeeds db)
  let cid = unCatID $ feedCatID $ fromJust mbf
  put db { tblItems = M.insert iid i' $ tblItems db
         , idxFeeds = insertNested fid iid $ idxFeeds db
         , idxCats  = case mbf of
             Just f  -> insertNested cid iid $ idxCats db
             Nothing -> idxCats db
         }
  return i'

wipeDB :: Update FeedsDB ()
wipeDB = put emptyDB



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

-- cats2Seq

data Cats2Seq = Cats2Seq

$(deriveSafeCopy 0 'base ''Cats2Seq)

instance Method Cats2Seq where
  type MethodResult Cats2Seq = Seq.Seq Cat
  type MethodState Cats2Seq = FeedsDB

instance QueryEvent Cats2Seq

-- feeds2Seq

data Feeds2Seq = Feeds2Seq

$(deriveSafeCopy 0 'base ''Feeds2Seq)

instance Method Feeds2Seq where
  type MethodResult Feeds2Seq = Seq.Seq Feed
  type MethodState Feeds2Seq = FeedsDB

instance QueryEvent Feeds2Seq

-- lookupCat

data LookupCat = LookupCat Int

$(deriveSafeCopy 0 'base ''LookupCat)

instance Method LookupCat where
  type MethodResult LookupCat = Maybe Cat
  type MethodState LookupCat = FeedsDB

instance QueryEvent LookupCat

-- lookupFeed

data LookupFeed = LookupFeed Int

$(deriveSafeCopy 0 'base ''LookupFeed)

instance Method LookupFeed where
  type MethodResult LookupFeed = Maybe Feed
  type MethodState LookupFeed = FeedsDB

instance QueryEvent LookupFeed

-- lookupPerson

data LookupPerson = LookupPerson Int

$(deriveSafeCopy 0 'base ''LookupPerson)

instance Method LookupPerson where
  type MethodResult LookupPerson = Maybe Person
  type MethodState LookupPerson = FeedsDB

instance QueryEvent LookupPerson

-- lookupItem

data LookupItem = LookupItem Int

$(deriveSafeCopy 0 'base ''LookupItem)

instance Method LookupItem where
  type MethodResult LookupItem = Maybe Item
  type MethodState LookupItem = FeedsDB

instance QueryEvent LookupItem

-- insertCat

data InsertCat = InsertCat Cat

$(deriveSafeCopy 0 'base ''InsertCat)

instance Method InsertCat where
  type MethodResult InsertCat = Cat
  type MethodState InsertCat = FeedsDB

instance UpdateEvent InsertCat

-- insertFeed

data InsertFeed = InsertFeed Feed

$(deriveSafeCopy 0 'base ''InsertFeed)

instance Method InsertFeed where
  type MethodResult InsertFeed = Feed
  type MethodState InsertFeed = FeedsDB

instance UpdateEvent InsertFeed

-- insertPerson

data InsertPerson = InsertPerson Person

$(deriveSafeCopy 0 'base ''InsertPerson)

instance Method InsertPerson where
  type MethodResult InsertPerson = Person
  type MethodState InsertPerson = FeedsDB

instance UpdateEvent InsertPerson

-- insertItem

data InsertItem = InsertItem Item

$(deriveSafeCopy 0 'base ''InsertItem)

instance Method InsertItem where
  type MethodResult InsertItem = Item
  type MethodState InsertItem = FeedsDB

instance UpdateEvent InsertItem

-- wipeDB

data WipeDB = WipeDB

$(deriveSafeCopy 0 'base ''WipeDB)

instance Method WipeDB where
  type MethodResult WipeDB = ()
  type MethodState WipeDB = FeedsDB

instance UpdateEvent WipeDB

-- FeedsDB

instance IsAcidic FeedsDB where
  acidEvents = [ QueryEvent  (\ GetStats        -> getStats)
               , QueryEvent  (\ Cats2Seq        -> cats2Seq)
               , QueryEvent  (\ Feeds2Seq       -> feeds2Seq)
               , QueryEvent  (\(LookupCat  k)   -> lookupCat k)
               , QueryEvent  (\(LookupFeed k)   -> lookupFeed k)
               , QueryEvent  (\(LookupPerson k) -> lookupPerson k)
               , QueryEvent  (\(LookupItem k)   -> lookupItem k)
               , UpdateEvent (\(InsertCat  c)   -> insertCat c)
               , UpdateEvent (\(InsertFeed f)   -> insertFeed f)
               , UpdateEvent (\(InsertPerson p) -> insertPerson p)
               , UpdateEvent (\(InsertItem i)   -> insertItem i)
               , UpdateEvent (\ WipeDB          -> wipeDB)
               ]
