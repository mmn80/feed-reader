{-# LANGUAGE DeriveFoldable         #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}

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
  ( TableID (..)
  , CatID
  , FeedID
  , PersonID
  , ItemID
  , ShardID
  , Cat (..)
  , Feed (..)
  , Person (..)
  , Item (..)
  , URL
  , Language
  , Tag
  , Content (..)
  , Image (..)
  , StatsMaster (..)
  , ShardSet
  , ShardIdx (..)
  , ShardSize
  , PrimaryIdx
  , NestedIdx
  , Table
  , PendingOp (..)
  , Master (..)
  , Shard (..)
  , ToFeed (..)
  , ToPerson (..)
  , ToItem (..)
  , tableSize
  , tableLookup
  , tableInsert
  , tableSplit
  , tableFromList
  , foldrShard
  , findUnusedID
  , findShard
  , findParentShard
  , sizeShards
  , sizePrimary
  , getShardsInfo
  , nextPage
  , splitShard
  , insertPrimary
  , insertNested
  , emptyMaster
  , emptyShard
  , shardID2ItemID
  , text2UTCTime
  , imageFromURL
  , diffMs
  ) where

import           Control.Applicative   ((<|>))
import           Data.Hashable         (hash)
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import           Data.Maybe            (fromJust, fromMaybe)
import qualified Data.Sequence         as Seq
import           Data.Time.Clock       (UTCTime, diffUTCTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Time.Format      (defaultTimeLocale, iso8601DateFormat,
                                        parseTimeM, rfc822DateFormat)

------------------------------------------------------------------------------
-- Table IDs
------------------------------------------------------------------------------

newtype CatID    = CatID    { unCatID :: Int    } deriving (Eq, Ord)
newtype PersonID = PersonID { unPersonID :: Int } deriving (Eq, Ord)
newtype FeedID   = FeedID   { unFeedID :: Int   } deriving (Eq, Ord)
newtype ItemID   = ItemID   { unItemID :: Int   } deriving (Eq, Ord)
newtype ShardID  = ShardID  { unShardID :: Int  } deriving (Eq, Ord)

instance Show CatID    where show (CatID k)    = show k
instance Show PersonID where show (PersonID k) = show k
instance Show FeedID   where show (FeedID k)   = show k
instance Show ItemID   where show (ItemID k)   = show k
instance Show ShardID  where show (ShardID k)  = show k

class TableID a b | a -> b, b -> a where
  unsetID :: a
  toInt :: a -> Int
  fromRecord :: b -> a

instance TableID CatID Cat where
  unsetID = CatID 0
  toInt = unCatID
  fromRecord = CatID . hash . catName

instance TableID PersonID Person where
  unsetID = PersonID 0
  toInt = unPersonID
  fromRecord p = PersonID $ hash $ personName p ++ personEmail p

instance TableID FeedID Feed where
  unsetID = FeedID 0
  toInt = unFeedID
  fromRecord = FeedID . hash . feedURL

instance TableID ItemID Item where
  unsetID = ItemID 0
  toInt = unItemID
  fromRecord = ItemID . fromInteger . round . utcTimeToPOSIXSeconds . itemUpdated

instance TableID ShardID ItemID where
  unsetID = ShardID 0
  toInt = unShardID
  fromRecord = ShardID . unItemID

shardID2ItemID :: ShardID -> ItemID
shardID2ItemID (ShardID sid) = ItemID sid

------------------------------------------------------------------------------
-- Table Data
------------------------------------------------------------------------------

type URL      = String
type Language = String
type Tag      = String
data Content  = Text String | HTML String | XHTML String
  deriving (Show)

data Cat = Cat
  { catID   :: CatID
  , catName :: String
  } deriving (Show)

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

class ToFeed f where
  toFeed :: f -> CatID -> URL -> UTCTime -> (Feed, [Person], [Person])

class ToPerson p where
  toPerson :: p -> Person

class ToItem i where
  toItem :: i -> FeedID -> URL -> UTCTime -> (Item, [Person], [Person])

text2UTCTime :: String -> UTCTime -> UTCTime
text2UTCTime t df = fromMaybe df $ iso <|> iso' <|> rfc
  where
    iso  = tryParse $ iso8601DateFormat $ Just "%H:%M:%S"
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

------------------------------------------------------------------------------
-- Table
------------------------------------------------------------------------------

newtype Table a = Table { unTable :: Map.IntMap a } deriving (Foldable)

tableSize :: Table a -> Int
tableSize (Table t) = Map.size t

tableLookup :: Int -> Table a -> Maybe a
tableLookup k (Table t) = Map.lookup k t

tableInsert :: Int -> a -> Table a -> Table a
tableInsert k v (Table t) = Table $ Map.insert k v t

tableSplit :: Int -> Table a -> (Table a, Table a)
tableSplit k (Table t) = (Table l, Table r)
  where (l, r) = Map.split k t

tableFromList :: [(Int, a)] -> Table a
tableFromList as = Table $ Map.fromList as

------------------------------------------------------------------------------
-- Primary Index
------------------------------------------------------------------------------

type ShardSize = Int

data ShardIdx a = ShardIdx
  { shardSize :: ShardSize
  , shardKeys :: ShardSet a
  }

newtype ShardSet a = ShardSet { unShardSet :: Set.IntSet }

newtype PrimaryIdx a = PrimaryIdx { unPrimaryIdx :: Map.IntMap (ShardIdx a) }

foldrShard :: (ItemID -> b -> b) -> b -> ShardIdx ItemID -> b
foldrShard f b (ShardIdx _ (ShardSet ks)) = Set.foldr f' b ks
  where f' k = f (ItemID k)

findShard :: ShardID -> PrimaryIdx a -> Maybe (ShardIdx a)
findShard sid idx = Map.lookup (unShardID sid) $ unPrimaryIdx idx

findParentShard :: Int -> PrimaryIdx a -> (ShardID, ShardSize)
findParentShard k idx =
  case Map.lookupLE k (unPrimaryIdx idx) of
    Just (s, ShardIdx sz _) -> (ShardID s, sz)
    Nothing                 -> (unsetID, 0)

sizeShards :: PrimaryIdx a -> Int
sizeShards = Map.size . unPrimaryIdx

sizePrimary :: PrimaryIdx a -> Int
sizePrimary (PrimaryIdx idx) = sum $ shardSize <$> Map.elems idx

getShardsInfo :: PrimaryIdx a -> Seq.Seq (ShardID, ShardSize)
getShardsInfo idx =
  if null (unPrimaryIdx idx) then Seq.singleton (unsetID, 0)
  else foldrS toSeq Seq.empty idx
  where
    toSeq k (ShardIdx l _) s = s Seq.|> (k, l)
    foldrS f b is = Map.foldrWithKey (f . ShardID) b (unPrimaryIdx is)

foldPage :: ShardID -> (ShardID -> ShardIdx a -> b -> b) ->
                   (ShardID -> b -> Bool) -> b -> PrimaryIdx a -> b
foldPage sid f stop x idx =
  let m = unPrimaryIdx idx in
  let mbs = Map.lookup (unShardID sid) m in
  if null mbs then x
  else let x' = f sid (fromJust mbs) x in
       let mbs' = Map.lookupLT (unShardID sid) m in
       if stop sid x' || null mbs' then x'
       else let (sid', _) = fromJust mbs' in
            foldPage (ShardID sid') f stop x' idx

findUnusedID :: ItemID -> PrimaryIdx ItemID -> ItemID
findUnusedID k0 idx =
  foldPage (fst $ findParentShard (toInt k0) idx) checkSet stop k0 idx
  where
    checkSet sid is k =
      if Set.notMember (unItemID k) (unShardSet $ shardKeys is) then k
      else checkSet sid is $ ItemID $ unItemID k - 1
    stop sid x = unItemID x >= unShardID sid

nextPage :: Int -> Int -> PrimaryIdx ItemID -> [(ItemID, ShardID)]
nextPage i l idx =
  snd $ foldPage (fst $ findParentShard i idx) f stop (l, []) idx
    where
      go sid x (n, rs) m =
        if n == 0 then (0, rs)
        else case getNext x m of
               Nothing -> (n, rs)
               Just x' -> go sid x' (n - 1, (x', sid) : rs) m
      f sid (ShardIdx _ (ShardSet s)) (n, rs) = go sid (ItemID i) (n, rs) s
      stop _ (n, _) = n == 0
      getNext k ks = ItemID <$> Set.lookupLT (unItemID k) ks

splitShard :: ShardID -> PrimaryIdx ItemID -> (PrimaryIdx ItemID,
                     ([ItemID], [ItemID], ShardSize, ShardSize, ShardID))
splitShard sid idx =
  (PrimaryIdx idx', (ItemID <$> ls, ItemID <$> rs, lsz, rsz, ShardID rid))
  where
    is = Set.toList $ unShardSet $ shardKeys $ fromJust $ findShard sid idx
    (lsz, r) = length is `divMod` 2
    rsz = lsz + r
    (ls, rs) = (take lsz is, drop lsz is)
    (lis, ris) = (Set.fromList ls, Set.fromList rs)
    rid = head rs
    idx' = Map.insert rid (ShardIdx rsz (ShardSet ris)) $
           Map.insert (unShardID sid) (ShardIdx lsz (ShardSet lis)) (unPrimaryIdx idx)

insertPrimary :: TableID a b => ShardID -> a -> PrimaryIdx a -> PrimaryIdx a
insertPrimary s i idx = PrimaryIdx $ Map.insert (unShardID s)
                                                (ShardIdx (sz + 1) (ShardSet ix')) m
  where
    m = unPrimaryIdx idx
    ShardIdx sz (ShardSet ix) = fromMaybe (ShardIdx 0 (ShardSet Set.empty)) $
                                  Map.lookup (unShardID s) m
    ix' = Set.insert (toInt i) ix

------------------------------------------------------------------------------
-- Nested Index
------------------------------------------------------------------------------

newtype NestedIdx a b = NestedIdx { unNestedIdx :: Map.IntMap Set.IntSet }

insertNested :: (TableID a c, TableID b d) => a -> b -> NestedIdx a b -> NestedIdx a b
insertNested k i idx = NestedIdx $ Map.insert (toInt k) new m
  where
    m = unNestedIdx idx
    new = Set.insert (toInt i) $ fromMaybe Set.empty $ Map.lookup (toInt k) m

------------------------------------------------------------------------------
-- DataBase
------------------------------------------------------------------------------

data PendingOp = NoPendingOp
               | PendingAddItem ItemID Item
               | PendingSplitPhase0 ShardID ShardID
               | PendingSplitPhase1 ShardID ShardID
               deriving (Show)

data Master = Master
  { pendingOp  :: !PendingOp
  , idxItem    :: !(PrimaryIdx ItemID)
  , ixCatItem  :: !(NestedIdx CatID ItemID)
  , ixFeedItem :: !(NestedIdx FeedID ItemID)
  , tblCat     :: !(Table Cat)
  , tblFeed    :: !(Table Feed)
  , tblPerson  :: !(Table Person)
  }

emptyMaster :: Master
emptyMaster = Master
  { pendingOp  = NoPendingOp
  , idxItem    = PrimaryIdx Map.empty
  , ixCatItem  = NestedIdx Map.empty
  , ixFeedItem = NestedIdx Map.empty
  , tblCat     = Table Map.empty
  , tblFeed    = Table Map.empty
  , tblPerson  = Table Map.empty
  }

data StatsMaster = StatsMaster
  { statsPending  :: PendingOp
  , countCats     :: Int
  , countFeeds    :: Int
  , countPersons  :: Int
  , countItemsAll :: Int
  , countShards   :: Int
  } deriving (Show)

data Shard = Shard
  { shSize  :: !ShardSize
  , tblItem :: !(Table Item)
  }

emptyShard :: Shard
emptyShard = Shard 0 $ Table Map.empty
