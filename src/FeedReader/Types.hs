module FeedReader.Types
  ( CatID (..)
  , unsetCatID
  , FeedID (..)
  , unsetFeedID
  , PersonID (..)
  , unsetPersonID
  , ItemID (..)
  , unsetItemID
  , ShardID (..)
  , unsetShardID
  , Cat (..)
  , Feed (..)
  , Person (..)
  , Item (..)
  , URL
  , Language
  , Tag
  , Content (..)
  , Image (..)
  , imageFromURL
  , StatsMaster (..)
  , StatsShard (..)
  , ShardSize
  , ShardIdx (..)
  , PrimaryIdx
  , NestedIdx
  , Table
  , PendingOp (..)
  , Master (..)
  , Shard (..)
  , insertPrimaryIdx
  , pageFoldPrimaryIdx
  , insertNestedIdx
  , emptyMaster
  , emptyShard
  , ToFeed (..)
  , ToPerson (..)
  , ToItem (..)
  , text2UTCTime
  , diffMs
  ) where

import           Control.Applicative ((<|>))
import qualified Data.IntMap         as Map
import qualified Data.IntSet         as Set
import           Data.Maybe          (fromJust, fromMaybe)
import           Data.Time.Clock     (UTCTime, diffUTCTime)
import           Data.Time.Format    (defaultTimeLocale, iso8601DateFormat,
                                      parseTimeM, rfc822DateFormat)


type URL      = String
type Language = String
type Tag      = String
data Content  = Text String | HTML String | XHTML String
  deriving (Show)

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

type ShardSize  = Int

data ShardIdx = ShardIdx
  { shardSize :: ShardSize
  , shardKeys :: Set.IntSet
  }

type PrimaryIdx = Map.IntMap ShardIdx
type NestedIdx  = Map.IntMap Set.IntSet
type Table a    = Map.IntMap a

data PendingOp = NoPendingOp
               | PendingAddItem ItemID Item
               | PendingSplitPhase0 ShardID ShardID
               | PendingSplitPhase1 ShardID ShardID
               deriving (Show)

data Master = Master
  { pendingOp  :: !PendingOp
  , idxItem    :: !PrimaryIdx
  , ixCatItem  :: !NestedIdx
  , ixFeedItem :: !NestedIdx
  , tblCat     :: !(Table Cat)
  , tblFeed    :: !(Table Feed)
  , tblPerson  :: !(Table Person)
  }

data Shard = Shard
  { shSize  :: !ShardSize
  , tblItem :: !(Table Item)
  }

class ToFeed f where
  toFeed :: f -> CatID -> URL -> UTCTime -> (Feed, [Person], [Person])

class ToPerson p where
  toPerson :: p -> Person

class ToItem i where
  toItem :: i -> FeedID -> URL -> UTCTime -> (Item, [Person], [Person])

data StatsMaster = StatsMaster
  { statsPending  :: PendingOp
  , countCats     :: Int
  , countFeeds    :: Int
  , countPersons  :: Int
  , countItemsAll :: Int
  , countShards   :: Int
  }

data StatsShard = StatsShard
  { countItems :: Int
  , shardID    :: ShardID
  }

insertNestedIdx :: Int -> Int -> NestedIdx -> NestedIdx
insertNestedIdx k i m = Map.insert k new m
  where
    new = Set.insert i $ fromMaybe Set.empty $ Map.lookup k m

insertPrimaryIdx :: Int -> Int -> PrimaryIdx -> PrimaryIdx
insertPrimaryIdx s i m = Map.insert s (ShardIdx (sz + 1) $ Set.insert i ix) m
  where
    ShardIdx sz ix = fromMaybe (ShardIdx 0 Set.empty) $ Map.lookup s m

pageFoldPrimaryIdx :: ShardID -> (ShardID -> ShardIdx -> a -> a) ->
                      (ShardID -> a -> Bool) -> a -> PrimaryIdx -> a
pageFoldPrimaryIdx sid f stop x m =
  let mbs = Map.lookup (unShardID sid) m in
  if null mbs then x
  else let x' = f sid (fromJust mbs) x in
       let mbs' = Map.lookupGT (unShardID sid) m in
       if null mbs' then x'
       else let (sid', _) = fromJust mbs' in
            if stop (ShardID sid') x' then x'
            else pageFoldPrimaryIdx (ShardID sid') f stop x' m

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

emptyMaster :: Master
emptyMaster = Master NoPendingOp Map.empty Map.empty Map.empty Map.empty Map.empty Map.empty

emptyShard :: Shard
emptyShard = Shard 0 Map.empty

unsetCatID :: CatID
unsetCatID = CatID 0

unsetFeedID :: FeedID
unsetFeedID = FeedID 0

unsetItemID :: ItemID
unsetItemID = ItemID 0

unsetPersonID :: PersonID
unsetPersonID = PersonID 0

unsetShardID :: ShardID
unsetShardID = ShardID 0
