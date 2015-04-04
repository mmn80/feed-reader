module FeedReader.Types
  ( CatID (..)
  , unsetCatID
  , FeedID (..)
  , unsetFeedID
  , PersonID (..)
  , unsetPersonID
  , ItemID (..)
  , unsetItemID
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
  , NestedMap
  , insertNested
  , Master (..)
  , emptyMaster
  , Shard (..)
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
import           Data.Maybe          (fromMaybe)
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

instance Show CatID    where show (CatID k)    = show k
instance Show PersonID where show (PersonID k) = show k
instance Show FeedID   where show (FeedID k)   = show k
instance Show ItemID   where show (ItemID k)   = show k

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

data StatsMaster = StatsMaster
  { countCats     :: Int
  , countFeeds    :: Int
  , countPersons  :: Int
  , countItemsAll :: Int
  , countShards   :: Int
  }

data StatsShard = StatsShard
  { countItems :: Int
  , shardID    :: ItemID
  }

type NestedMap = Map.IntMap Set.IntSet

insertNested :: Int -> Int -> NestedMap -> NestedMap
insertNested k i m = Map.insert k newInner m
  where
    newInner = Set.insert i $ fromMaybe Set.empty $ Map.lookup k m

data Master = Master
  { idxShard   :: !(Map.IntMap Int)
  , idxItem    :: !Set.IntSet
  , ixCatItem  :: !NestedMap
  , ixFeedItem :: !NestedMap
  , tblCat     :: !(Map.IntMap Cat)
  , tblFeed    :: !(Map.IntMap Feed)
  , tblPerson  :: !(Map.IntMap Person)
  }

emptyMaster :: Master
emptyMaster = Master Map.empty Set.empty Map.empty Map.empty Map.empty Map.empty Map.empty

data Shard = Shard
  { shSize  :: !Int
  , tblItem :: !(Map.IntMap Item)
  }

emptyShard :: Shard
emptyShard = Shard 0 Map.empty

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

unsetCatID :: CatID
unsetCatID = CatID 0

unsetFeedID :: FeedID
unsetFeedID = FeedID 0

unsetItemID :: ItemID
unsetItemID = ItemID 0

unsetPersonID :: PersonID
unsetPersonID = PersonID 0
