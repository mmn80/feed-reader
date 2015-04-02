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
    CatID
  , FeedID
  , PersonID
  , ItemID
  , Cat (..)
  , Feed (..)
  , Person (..)
  , Item (..)
  , URL
  , Language
  , Tag
  , Content (..)
  , Image (..)
  , Master (..)
-- utils
  , imageFromURL
  , unsetItemID
  , unsetPersonID
  , unsetFeedID
  , unsetCatID
  , emptyDB
  , text2UTCTime
  , diffMs
-- conversion classes
  , ToFeed (..)
  , ToPerson (..)
  , ToItem (..)
  , addItem2DB
  , addFeed2DB
-- master read queries
  , DBStats (..)
  , getStats
  , GetStats (..)
  , lookupCat
  , LookupCat (..)
  , lookupFeed
  , LookupFeed (..)
  , lookupPerson
  , LookupPerson (..)
  , cats2Seq
  , Cats2Seq (..)
  , feeds2Seq
  , Feeds2Seq (..)
-- master update queries
  , insertCat
  , InsertCat (..)
  , insertFeed
  , InsertFeed (..)
  , insertPerson
  , InsertPerson (..)
-- complex queries
  , lookupItem
  , insertItems
  , wipeDB
  ) where

import           Control.Exception     (bracket)
import           Control.Monad         (forM, forM_)
import           Control.Monad.Reader  (ask)
import           Control.Monad.State   (get, put)
import           Data.Acid
import           Data.Acid.Advanced
import           Data.Hashable         (hash)
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import           Data.List             (groupBy, sortBy)
import           Data.Maybe            (fromJust, fromMaybe)
import           Data.Monoid           (First (..), getFirst, (<>))
import           Data.SafeCopy
import qualified Data.Sequence         as Seq
import           Data.Time.Clock       (UTCTime, diffUTCTime, getCurrentTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Time.Format      (defaultTimeLocale, iso8601DateFormat,
                                        parseTimeM, rfc822DateFormat)
import           System.Directory      (doesDirectoryExist, removeDirectoryRecursive)
import           System.FilePath       ((</>))

type URL      = String
type Language = String
type Tag      = String
data Content  = Text String | HTML String | XHTML String
  deriving (Show)

newtype CatID    = CatID    { unCatID :: Int } deriving (Show, Eq, Ord)
newtype PersonID = PersonID { unPersonID :: Int } deriving (Show, Eq, Ord)
newtype FeedID   = FeedID   { unFeedID :: Int } deriving (Show, Eq, Ord)
newtype ItemID   = ItemID   { unItemID :: Int } deriving (Show, Eq, Ord)

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

type NestedMap = Map.IntMap Set.IntSet

data Master = Master
  { shItem     :: !Set.IntSet
  , idxItem    :: !Set.IntSet
  , ixCatItem  :: !NestedMap
  , ixFeedItem :: !NestedMap
  , tblCat     :: !(Map.IntMap Cat)
  , tblFeed    :: !(Map.IntMap Feed)
  , tblPerson  :: !(Map.IntMap Person)
  }

data ShardItem = ShardItem
  { shItemSize :: !Int
  , tblItem    :: !(Map.IntMap Item)
  }

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  , countShards  :: Int
  }

------------------------------------------------------------------------------
-- Utilities
------------------------------------------------------------------------------

checkUniqueID :: Set.IntSet -> Int -> Int
checkUniqueID idx x = if Set.notMember x idx then x
                      else checkUniqueID idx (x + 1)

emptyDB :: Master
emptyDB = Master Set.empty Set.empty Map.empty Map.empty Map.empty Map.empty Map.empty

emptyShardItem :: ShardItem
emptyShardItem = ShardItem 0 Map.empty

insertNested :: Int -> Int -> NestedMap -> NestedMap
insertNested k i m = Map.insert k newInner m
  where
    newInner = Set.insert i $ fromMaybe Set.empty $ Map.lookup k m

text2UTCTime :: String -> UTCTime -> UTCTime
text2UTCTime t df = fromMaybe df $ getFirst $ iso <> iso' <> rfc
  where
    iso  = tryParse $ iso8601DateFormat $ Just "%H:%M:%S"
    iso' = tryParse $ iso8601DateFormat Nothing
    rfc  = tryParse rfc822DateFormat
    tryParse f = First $ parseTimeM True defaultTimeLocale f t

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
$(deriveSafeCopy 0 'base ''Master)
$(deriveSafeCopy 0 'base ''ShardItem)

------------------------------------------------------------------------------
-- Read Queries
------------------------------------------------------------------------------

getStats :: Query Master DBStats
getStats = do
  db <- ask
  return DBStats
           { countCats    = Map.size $ tblCat    db
           , countFeeds   = Map.size $ tblFeed   db
           , countPersons = Map.size $ tblPerson db
           , countItems   = Set.size $ idxItem   db
           , countShards  = 1 + Set.size (shItem db)
           }

cats2Seq :: Query Master (Seq.Seq Cat)
cats2Seq = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblCat db

feeds2Seq :: Query Master (Seq.Seq Feed)
feeds2Seq = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblFeed db

getShItem :: Query Master Set.IntSet
getShItem = do
  db <- ask
  return $ shItem db

lookupCat :: Int -> Query Master (Maybe Cat)
lookupCat k = do
  db <- ask
  return $ Map.lookup k (tblCat db)

lookupFeed :: Int -> Query Master (Maybe Feed)
lookupFeed k = do
  db <- ask
  return $ Map.lookup k (tblFeed db)

lookupPerson :: Int -> Query Master (Maybe Person)
lookupPerson k = do
  db <- ask
  return $ Map.lookup k (tblPerson db)

getItemShard :: ItemID -> Query Master ItemID
getItemShard (ItemID k) = do
  db <- ask
  return $ ItemID $ case Set.lookupLE k (shItem db) of
                           Just s -> s
                           _      -> 0

mkUniqueItemID :: Item -> Query Master ItemID
mkUniqueItemID i = do
  db <- ask
  return $ ItemID $ checkUniqueID (idxItem db) $ unItemID $ calcItemID i

-- ShardItem

lookupShardItem :: ItemID -> Query ShardItem (Maybe Item)
lookupShardItem (ItemID k) = do
  db <- ask
  return $ Map.lookup k (tblItem db)

------------------------------------------------------------------------------
-- Update Queries
------------------------------------------------------------------------------

insertCat :: Cat -> Update Master Cat
insertCat c = do
  db <- get
  let cid = calcCatID c
  let c' = c { catID = cid }
  put $ db { tblCat = Map.insert (unCatID cid) c' $ tblCat db }
  return c'

insertFeed :: Feed -> Update Master Feed
insertFeed f = do
  db <- get
  let fid = calcFeedID f
  let f' = f { feedID = fid }
  put $ db { tblFeed = Map.insert (unFeedID fid) f' $ tblFeed db }
  return f'

insertPerson :: Person -> Update Master Person
insertPerson p = do
  db <- get
  let pid = calcPersonID p
  let p' = p { personID = pid }
  put $ db { tblPerson = Map.insert (unPersonID pid) p' $ tblPerson db }
  return p'

insertMasterItem :: Item -> Update Master Item
insertMasterItem i = do
  db <- get
  let iid = unItemID $ itemID i
  let fid = unFeedID $ itemFeedID i
  let mbf = Map.lookup fid (tblFeed db)
  let cid = unCatID $ feedCatID $ fromJust mbf
  put db { idxItem = Set.insert iid $ idxItem db
         , ixFeedItem = insertNested fid iid $ ixFeedItem db
         , ixCatItem  = case mbf of
             Just f  -> insertNested cid iid $ ixCatItem db
             Nothing -> ixCatItem db
         }
  return i

wipeMaster :: Update Master ()
wipeMaster = put emptyDB

-- ShardItem

insertShardItem :: Item -> Update ShardItem Item
insertShardItem i = do
  db <- get
  put $ db { shItemSize = shItemSize db + 1
           , tblItem = Map.insert (unItemID $ itemID i) i $ tblItem db
           }
  return i


------------------------------------------------------------------------------
-- makeAcidic
------------------------------------------------------------------------------

-- getStats

data GetStats = GetStats

$(deriveSafeCopy 0 'base ''GetStats)

instance Method GetStats where
  type MethodResult GetStats = DBStats
  type MethodState GetStats = Master

instance QueryEvent GetStats

-- cats2Seq

data Cats2Seq = Cats2Seq

$(deriveSafeCopy 0 'base ''Cats2Seq)

instance Method Cats2Seq where
  type MethodResult Cats2Seq = Seq.Seq Cat
  type MethodState Cats2Seq = Master

instance QueryEvent Cats2Seq

-- feeds2Seq

data Feeds2Seq = Feeds2Seq

$(deriveSafeCopy 0 'base ''Feeds2Seq)

instance Method Feeds2Seq where
  type MethodResult Feeds2Seq = Seq.Seq Feed
  type MethodState Feeds2Seq = Master

instance QueryEvent Feeds2Seq

-- getShItem

data GetShItem = GetShItem

$(deriveSafeCopy 0 'base ''GetShItem)

instance Method GetShItem where
  type MethodResult GetShItem = Set.IntSet
  type MethodState GetShItem = Master

instance QueryEvent GetShItem

-- lookupCat

data LookupCat = LookupCat Int

$(deriveSafeCopy 0 'base ''LookupCat)

instance Method LookupCat where
  type MethodResult LookupCat = Maybe Cat
  type MethodState LookupCat = Master

instance QueryEvent LookupCat

-- lookupFeed

data LookupFeed = LookupFeed Int

$(deriveSafeCopy 0 'base ''LookupFeed)

instance Method LookupFeed where
  type MethodResult LookupFeed = Maybe Feed
  type MethodState LookupFeed = Master

instance QueryEvent LookupFeed

-- lookupPerson

data LookupPerson = LookupPerson Int

$(deriveSafeCopy 0 'base ''LookupPerson)

instance Method LookupPerson where
  type MethodResult LookupPerson = Maybe Person
  type MethodState LookupPerson = Master

instance QueryEvent LookupPerson

-- GetItemShard

data GetItemShard = GetItemShard ItemID

$(deriveSafeCopy 0 'base ''GetItemShard)

instance Method GetItemShard where
  type MethodResult GetItemShard = ItemID
  type MethodState GetItemShard = Master

instance QueryEvent GetItemShard

-- MkUniqueItemID

data MkUniqueItemID = MkUniqueItemID Item

$(deriveSafeCopy 0 'base ''MkUniqueItemID)

instance Method MkUniqueItemID where
  type MethodResult MkUniqueItemID = ItemID
  type MethodState MkUniqueItemID = Master

instance QueryEvent MkUniqueItemID

-- insertCat

data InsertCat = InsertCat Cat

$(deriveSafeCopy 0 'base ''InsertCat)

instance Method InsertCat where
  type MethodResult InsertCat = Cat
  type MethodState InsertCat = Master

instance UpdateEvent InsertCat

-- insertFeed

data InsertFeed = InsertFeed Feed

$(deriveSafeCopy 0 'base ''InsertFeed)

instance Method InsertFeed where
  type MethodResult InsertFeed = Feed
  type MethodState InsertFeed = Master

instance UpdateEvent InsertFeed

-- insertPerson

data InsertPerson = InsertPerson Person

$(deriveSafeCopy 0 'base ''InsertPerson)

instance Method InsertPerson where
  type MethodResult InsertPerson = Person
  type MethodState InsertPerson = Master

instance UpdateEvent InsertPerson

-- insertMasterItem

data InsertMasterItem = InsertMasterItem Item

$(deriveSafeCopy 0 'base ''InsertMasterItem)

instance Method InsertMasterItem where
  type MethodResult InsertMasterItem = Item
  type MethodState InsertMasterItem = Master

instance UpdateEvent InsertMasterItem

-- wipeMaster

data WipeMaster = WipeMaster

$(deriveSafeCopy 0 'base ''WipeMaster)

instance Method WipeMaster where
  type MethodResult WipeMaster = ()
  type MethodState WipeMaster = Master

instance UpdateEvent WipeMaster

-- Master

instance IsAcidic Master where
  acidEvents = [ QueryEvent  (\ GetStats            -> getStats)
               , QueryEvent  (\ Cats2Seq            -> cats2Seq)
               , QueryEvent  (\ Feeds2Seq           -> feeds2Seq)
               , QueryEvent  (\ GetShItem           -> getShItem)
               , QueryEvent  (\(LookupCat  k)       -> lookupCat k)
               , QueryEvent  (\(LookupFeed k)       -> lookupFeed k)
               , QueryEvent  (\(LookupPerson k)     -> lookupPerson k)
               , QueryEvent  (\(GetItemShard k)     -> getItemShard k)
               , QueryEvent  (\(MkUniqueItemID i)   -> mkUniqueItemID i)
               , UpdateEvent (\(InsertCat  c)       -> insertCat c)
               , UpdateEvent (\(InsertFeed f)       -> insertFeed f)
               , UpdateEvent (\(InsertPerson p)     -> insertPerson p)
               , UpdateEvent (\(InsertMasterItem i) -> insertMasterItem i)
               , UpdateEvent (\ WipeMaster          -> wipeMaster)
               ]



-- lookupShardItem

data LookupShardItem = LookupShardItem ItemID

$(deriveSafeCopy 0 'base ''LookupShardItem)

instance Method LookupShardItem where
  type MethodResult LookupShardItem = Maybe Item
  type MethodState LookupShardItem = ShardItem

instance QueryEvent LookupShardItem

-- insertShardItem

data InsertShardItem = InsertShardItem Item

$(deriveSafeCopy 0 'base ''InsertShardItem)

instance Method InsertShardItem where
  type MethodResult InsertShardItem = Item
  type MethodState InsertShardItem = ShardItem

instance UpdateEvent InsertShardItem


-- ItemShard

instance IsAcidic ShardItem where
  acidEvents = [ QueryEvent  (\(LookupShardItem k) -> lookupShardItem k)
               , UpdateEvent (\(InsertShardItem i) -> insertShardItem i)
               ]

------------------------------------------------------------------------------
-- Conversion Classes and Functions
------------------------------------------------------------------------------

class ToFeed f where
  toFeed :: f -> CatID -> URL -> UTCTime -> (Feed, [Person], [Person])

class ToPerson p where
  toPerson :: p -> Person

class ToItem i where
  toItem :: i -> FeedID -> URL -> UTCTime -> (Item, [Person], [Person])

addItem2DB :: ToItem i => AcidState Master -> i ->
                       FeedID -> URL -> IO Item
addItem2DB acid it fid u = do
  df <- getCurrentTime
  let (i, as, cs) = toItem it fid u df
  as' <- sequence $ addPerson acid <$> as
  cs' <- sequence $ addPerson acid <$> cs
  let i' = i { itemAuthors      = as'
             , itemContributors = cs'
             }
  update acid $ InsertMasterItem i'

addFeed2DB :: ToFeed f => AcidState Master -> f ->
                       CatID -> URL -> IO Feed
addFeed2DB acid it cid u = do
  df <- getCurrentTime
  let (f, as, cs) = toFeed it cid u df
  as' <- sequence $ addPerson acid <$> as
  cs' <- sequence $ addPerson acid <$> cs
  let f' = f { feedAuthors      = as'
             , feedContributors = cs'
             }
  update acid $ InsertFeed f'

addPerson acid p = do
  p' <- update acid $ InsertPerson p
  return $ personID p'


------------------------------------------------------------------------------
-- Complex queries
------------------------------------------------------------------------------

shardRoot = "data" </> "shard_"

withShard s = bracket
  (do t0 <- getCurrentTime
      putStrLn $ "  Opening shard #" ++ sid ++ "..."
      acid <- openLocalStateFrom (shardRoot ++ sid) emptyShardItem
      t1 <- getCurrentTime
      putStrLn $ "  Shard opened in " ++ show (diffMs t0 t1) ++ " ms."
      return acid )
  (\acid -> do
      putStrLn $ "  Closing shard #" ++ sid ++ "..."
      closeAcidState acid )
  where sid = show (unItemID s)

lookupItem :: AcidState Master -> Int -> IO (Maybe Item)
lookupItem acid k = do
  s <- query acid $ GetItemShard $ ItemID k
  withShard s $ \acid' ->
    query acid' $ LookupShardItem $ ItemID k

insertItems :: AcidState Master -> [Item] -> IO [Item]
insertItems acid is = do
  iss <- forM is $ \i -> do
    iid <- query acid $ MkUniqueItemID i
    s   <- query acid $ GetItemShard iid
    return (i { itemID = iid }, s)
  let gss = groupBy (\(_,x) (_,y) -> x == y) $ sortBy (\(_,x) (_,y) -> compare x y) iss
  gss' <- forM gss $ \gs ->
    withShard (snd $ head gs) $ \acid' ->
      forM gs $ \(i, _) -> do
        update acid' $ InsertShardItem i
        update acid $ InsertMasterItem i
        return i
  return $ concat gss'

wipeDB :: AcidState Master -> IO ()
wipeDB acid = do
  ss <- query acid GetShItem
  update acid WipeMaster
  let ss' = 0 : Set.toList ss
  forM_ ss' $ \s -> do
    let sf = shardRoot ++ show s
    ex <- doesDirectoryExist sf
    if ex then removeDirectoryRecursive sf
    else putStrLn $ "  Shard dir not found: " ++ sf
