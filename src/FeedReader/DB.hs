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
  , Stats (..)
  , Handle
-- utils
  , imageFromURL
  , unsetItemID
  , unsetPersonID
  , unsetFeedID
  , unsetCatID
  , text2UTCTime
  , diffMs
-- conversion classes
  , ToFeed (..)
  , ToPerson (..)
  , ToItem (..)
  , addItemConv
  , addFeedConv
-- main
  , open
  , close
  , checkpoint
  , archive
  , wipeDB
-- read queries
  , getStats
  , getCat
  , getFeed
  , getPerson
  , getItem
  , getCats
  , getFeeds
-- update queries
  , addCat
  , addFeed
  , addPerson
  , addItems
  ) where

import           Control.Applicative   ((<|>))
import           Control.Concurrent    (MVar, modifyMVar_, newMVar, putMVar,
                                        takeMVar)
import           Control.Exception     (bracket)
import           Control.Monad         (forM, forM_)
import           Control.Monad.Reader  (ask)
import           Control.Monad.State   (get, put)
import           Control.Monad.Trans   (MonadIO (liftIO))
import           Data.Acid
import           Data.Acid.Advanced
import           Data.Hashable         (hash)
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import           Data.List             (groupBy, sortBy)
import           Data.Maybe            (fromJust, fromMaybe)
import           Data.SafeCopy
import qualified Data.Sequence         as Seq
import           Data.Time.Clock       (UTCTime, diffUTCTime, getCurrentTime)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Time.Format      (defaultTimeLocale, iso8601DateFormat,
                                        parseTimeM, rfc822DateFormat)
import           System.Directory      (doesDirectoryExist,
                                        removeDirectoryRecursive)
import           System.FilePath       ((</>))

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

type OpenedShards = Map.IntMap (UTCTime, AcidState ShardItem)

data DBState = DBState
  { rootDir :: FilePath
  , master  :: AcidState Master
  , shards  :: MVar OpenedShards
  }

instance Eq DBState where
  DBState r1 _ _ == DBState r2 _ _ = r1 == r2

newtype Handle = Handle { unHandle :: DBState } deriving (Eq)

instance Show Handle where
  show (Handle (DBState r1 _ _)) = r1

data Stats = Stats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  , countShards  :: Int
  }

------------------------------------------------------------------------------
-- Utilities
------------------------------------------------------------------------------

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

------------------------------------------------------------------------------
-- Internal
------------------------------------------------------------------------------

insertNested :: Int -> Int -> NestedMap -> NestedMap
insertNested k i m = Map.insert k newInner m
  where
    newInner = Set.insert i $ fromMaybe Set.empty $ Map.lookup k m

emptyShardItem :: ShardItem
emptyShardItem = ShardItem 0 Map.empty

calcCatID :: Cat -> CatID
calcCatID = CatID . hash . catName

calcFeedID :: Feed -> FeedID
calcFeedID = FeedID . hash . feedURL

calcItemID :: Item -> ItemID
calcItemID = ItemID . fromInteger . round . utcTimeToPOSIXSeconds . itemUpdated

calcPersonID :: Person -> PersonID
calcPersonID p = PersonID $ hash $ personName p ++ personEmail p

checkUniqueID :: Set.IntSet -> Int -> Int
checkUniqueID idx x = if Set.notMember x idx then x
                      else checkUniqueID idx (x + 1)

emptyDB :: Master
emptyDB = Master Set.empty Set.empty Map.empty Map.empty Map.empty Map.empty Map.empty

mquery :: (MonadIO m, QueryEvent e, MethodState e ~ Master) =>
          Handle -> e -> m (EventResult e)
mquery  h = liftIO . query  (master $ unHandle h)

mupdate :: (MonadIO m, UpdateEvent e, MethodState e ~ Master) =>
          Handle -> e -> m (EventResult e)
mupdate h = liftIO . update (master $ unHandle h)

shardPath :: Handle -> ItemID -> FilePath
shardPath h i = rootDir (unHandle h) </> "shard_" ++ show (unItemID i)

checkOpenedShards :: MonadIO m => OpenedShards -> m OpenedShards
checkOpenedShards ss =
  if Map.size ss > 5 then do
    t0 <- liftIO getCurrentTime
    let older sid (t, a) (t', sid') =
          if t < t' then (t, sid) else (t', sid')
    let (t, sid) = Map.foldWithKey older (t0, 0) ss
    let Just (_, a) = Map.lookup sid ss
    liftIO $ putStrLn $ "  Closing shard #" ++ show sid ++ "..."
    liftIO $ closeAcidState a
    return $ Map.delete sid ss
  else
    return ss

withShard :: MonadIO m => Handle -> ItemID ->
             ((AcidState ShardItem, OpenedShards) -> IO a) -> m a
withShard h s = liftIO . bracket
  (do ss <- takeMVar $ shards $ unHandle h
      let sid = unItemID s
      let mb = Map.lookup sid ss
      t0 <- getCurrentTime
      let ins a = (a, Map.insert sid (t0, a) ss)
      if null mb then do
        putStrLn $ "  Opening shard #" ++ show sid ++ "..."
        acid <- openLocalStateFrom (shardPath h s) emptyShardItem
        t1 <- getCurrentTime
        putStrLn $ "  Shard opened in " ++ show (diffMs t0 t1) ++ " ms."
        return $ ins acid
      else
        return $ ins $ snd $ fromJust mb )
  (\(_, ss) -> do
    ss' <- checkOpenedShards ss
    putMVar (shards $ unHandle h) ss')

closeShards :: MonadIO m => Handle -> m ()
closeShards h =
  liftIO $ modifyMVar_ (shards $ unHandle h) $ \ss -> do
    mapM_ (liftIO . closeAcidState . snd) ss
    return Map.empty

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
$(deriveSafeCopy 0 'base ''Stats)
$(deriveSafeCopy 0 'base ''Master)
$(deriveSafeCopy 0 'base ''ShardItem)

------------------------------------------------------------------------------
-- Read Queries
------------------------------------------------------------------------------

getStatsAcid :: Query Master Stats
getStatsAcid = do
  db <- ask
  return Stats
           { countCats    = Map.size $ tblCat    db
           , countFeeds   = Map.size $ tblFeed   db
           , countPersons = Map.size $ tblPerson db
           , countItems   = Set.size $ idxItem   db
           , countShards  = 1 + Set.size (shItem db)
           }

getCatsAcid :: Query Master (Seq.Seq Cat)
getCatsAcid = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblCat db

getFeedsAcid :: Query Master (Seq.Seq Feed)
getFeedsAcid = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblFeed db

getShItemAcid :: Query Master Set.IntSet
getShItemAcid = do
  db <- ask
  return $ shItem db

getCatAcid :: Int -> Query Master (Maybe Cat)
getCatAcid k = do
  db <- ask
  return $ Map.lookup k (tblCat db)

getFeedAcid :: Int -> Query Master (Maybe Feed)
getFeedAcid k = do
  db <- ask
  return $ Map.lookup k (tblFeed db)

getPersonAcid :: Int -> Query Master (Maybe Person)
getPersonAcid k = do
  db <- ask
  return $ Map.lookup k (tblPerson db)

getItemShardAcid :: ItemID -> Query Master ItemID
getItemShardAcid (ItemID k) = do
  db <- ask
  return $ ItemID $ case Set.lookupLE k (shItem db) of
                           Just s -> s
                           _      -> 0

mkUniqueIDAcid :: Item -> Query Master ItemID
mkUniqueIDAcid i = do
  db <- ask
  return $ ItemID $ checkUniqueID (idxItem db) $ unItemID $ calcItemID i

-- ShardItem

getShardItemAcid :: ItemID -> Query ShardItem (Maybe Item)
getShardItemAcid (ItemID k) = do
  db <- ask
  return $ Map.lookup k (tblItem db)

------------------------------------------------------------------------------
-- Update Queries
------------------------------------------------------------------------------

addCatAcid :: Cat -> Update Master Cat
addCatAcid c = do
  db <- get
  let cid = calcCatID c
  let c' = c { catID = cid }
  put $ db { tblCat = Map.insert (unCatID cid) c' $ tblCat db }
  return c'

addFeedAcid :: Feed -> Update Master Feed
addFeedAcid f = do
  db <- get
  let fid = calcFeedID f
  let f' = f { feedID = fid }
  put $ db { tblFeed = Map.insert (unFeedID fid) f' $ tblFeed db }
  return f'

addPersonAcid :: Person -> Update Master Person
addPersonAcid p = do
  db <- get
  let pid = calcPersonID p
  let p' = p { personID = pid }
  put $ db { tblPerson = Map.insert (unPersonID pid) p' $ tblPerson db }
  return p'

addMasterItemAcid :: Item -> Update Master Item
addMasterItemAcid i = do
  db <- get
  let iid = unItemID $ itemID i
  let fid = unFeedID $ itemFeedID i
  let mbc = (unCatID . feedCatID) <$> Map.lookup fid (tblFeed db)
  put db { idxItem = Set.insert iid $ idxItem db
         , ixFeedItem = insertNested fid iid $ ixFeedItem db
         , ixCatItem  = case mbc of
             Just cid -> insertNested cid iid $ ixCatItem db
             _        -> ixCatItem db
         }
  return i

wipeMasterAcid :: Update Master ()
wipeMasterAcid = put emptyDB

-- ShardItem

addShardItemAcid :: Item -> Update ShardItem Item
addShardItemAcid i = do
  db <- get
  put $ db { shItemSize = shItemSize db + 1
           , tblItem = Map.insert (unItemID $ itemID i) i $ tblItem db
           }
  return i


------------------------------------------------------------------------------
-- makeAcidic
------------------------------------------------------------------------------

-- getStatsAcid

data GetStatsAcid = GetStatsAcid

$(deriveSafeCopy 0 'base ''GetStatsAcid)

instance Method GetStatsAcid where
  type MethodResult GetStatsAcid = Stats
  type MethodState GetStatsAcid = Master

instance QueryEvent GetStatsAcid

-- getCatsAcid

data GetCatsAcid = GetCatsAcid

$(deriveSafeCopy 0 'base ''GetCatsAcid)

instance Method GetCatsAcid where
  type MethodResult GetCatsAcid = Seq.Seq Cat
  type MethodState GetCatsAcid = Master

instance QueryEvent GetCatsAcid

-- getFeedsAcid

data GetFeedsAcid = GetFeedsAcid

$(deriveSafeCopy 0 'base ''GetFeedsAcid)

instance Method GetFeedsAcid where
  type MethodResult GetFeedsAcid = Seq.Seq Feed
  type MethodState GetFeedsAcid = Master

instance QueryEvent GetFeedsAcid

-- getShItemAcid

data GetShItemAcid = GetShItemAcid

$(deriveSafeCopy 0 'base ''GetShItemAcid)

instance Method GetShItemAcid where
  type MethodResult GetShItemAcid = Set.IntSet
  type MethodState GetShItemAcid = Master

instance QueryEvent GetShItemAcid

-- getCatAcid

data GetCatAcid = GetCatAcid Int

$(deriveSafeCopy 0 'base ''GetCatAcid)

instance Method GetCatAcid where
  type MethodResult GetCatAcid = Maybe Cat
  type MethodState GetCatAcid = Master

instance QueryEvent GetCatAcid

-- getFeedAcid

data GetFeedAcid = GetFeedAcid Int

$(deriveSafeCopy 0 'base ''GetFeedAcid)

instance Method GetFeedAcid where
  type MethodResult GetFeedAcid = Maybe Feed
  type MethodState GetFeedAcid = Master

instance QueryEvent GetFeedAcid

-- getPersonAcid

data GetPersonAcid = GetPersonAcid Int

$(deriveSafeCopy 0 'base ''GetPersonAcid)

instance Method GetPersonAcid where
  type MethodResult GetPersonAcid = Maybe Person
  type MethodState GetPersonAcid = Master

instance QueryEvent GetPersonAcid

-- GetItemShardAcid

data GetItemShardAcid = GetItemShardAcid ItemID

$(deriveSafeCopy 0 'base ''GetItemShardAcid)

instance Method GetItemShardAcid where
  type MethodResult GetItemShardAcid = ItemID
  type MethodState GetItemShardAcid = Master

instance QueryEvent GetItemShardAcid

-- MkUniqueIDAcid

data MkUniqueIDAcid = MkUniqueIDAcid Item

$(deriveSafeCopy 0 'base ''MkUniqueIDAcid)

instance Method MkUniqueIDAcid where
  type MethodResult MkUniqueIDAcid = ItemID
  type MethodState MkUniqueIDAcid = Master

instance QueryEvent MkUniqueIDAcid

-- addCatAcid

data AddCatAcid = AddCatAcid Cat

$(deriveSafeCopy 0 'base ''AddCatAcid)

instance Method AddCatAcid where
  type MethodResult AddCatAcid = Cat
  type MethodState AddCatAcid = Master

instance UpdateEvent AddCatAcid

-- addFeedAcid

data AddFeedAcid = AddFeedAcid Feed

$(deriveSafeCopy 0 'base ''AddFeedAcid)

instance Method AddFeedAcid where
  type MethodResult AddFeedAcid = Feed
  type MethodState AddFeedAcid = Master

instance UpdateEvent AddFeedAcid

-- addPersonAcid

data AddPersonAcid = AddPersonAcid Person

$(deriveSafeCopy 0 'base ''AddPersonAcid)

instance Method AddPersonAcid where
  type MethodResult AddPersonAcid = Person
  type MethodState AddPersonAcid = Master

instance UpdateEvent AddPersonAcid

-- addMasterItemAcid

data AddMasterItemAcid = AddMasterItemAcid Item

$(deriveSafeCopy 0 'base ''AddMasterItemAcid)

instance Method AddMasterItemAcid where
  type MethodResult AddMasterItemAcid = Item
  type MethodState AddMasterItemAcid = Master

instance UpdateEvent AddMasterItemAcid

-- wipeMasterAcid

data WipeMasterAcid = WipeMasterAcid

$(deriveSafeCopy 0 'base ''WipeMasterAcid)

instance Method WipeMasterAcid where
  type MethodResult WipeMasterAcid = ()
  type MethodState WipeMasterAcid = Master

instance UpdateEvent WipeMasterAcid

-- Master

instance IsAcidic Master where
  acidEvents = [ QueryEvent  (\ GetStatsAcid         -> getStatsAcid)
               , QueryEvent  (\ GetCatsAcid          -> getCatsAcid)
               , QueryEvent  (\ GetFeedsAcid         -> getFeedsAcid)
               , QueryEvent  (\ GetShItemAcid        -> getShItemAcid)
               , QueryEvent  (\(GetCatAcid  k)       -> getCatAcid k)
               , QueryEvent  (\(GetFeedAcid k)       -> getFeedAcid k)
               , QueryEvent  (\(GetPersonAcid k)     -> getPersonAcid k)
               , QueryEvent  (\(GetItemShardAcid k)  -> getItemShardAcid k)
               , QueryEvent  (\(MkUniqueIDAcid i)    -> mkUniqueIDAcid i)
               , UpdateEvent (\(AddCatAcid  c)       -> addCatAcid c)
               , UpdateEvent (\(AddFeedAcid f)       -> addFeedAcid f)
               , UpdateEvent (\(AddPersonAcid p)     -> addPersonAcid p)
               , UpdateEvent (\(AddMasterItemAcid i) -> addMasterItemAcid i)
               , UpdateEvent (\ WipeMasterAcid       -> wipeMasterAcid)
               ]



-- getShardItemAcid

data GetShardItemAcid = GetShardItemAcid ItemID

$(deriveSafeCopy 0 'base ''GetShardItemAcid)

instance Method GetShardItemAcid where
  type MethodResult GetShardItemAcid = Maybe Item
  type MethodState GetShardItemAcid = ShardItem

instance QueryEvent GetShardItemAcid

-- addShardItemAcid

data AddShardItemAcid = AddShardItemAcid Item

$(deriveSafeCopy 0 'base ''AddShardItemAcid)

instance Method AddShardItemAcid where
  type MethodResult AddShardItemAcid = Item
  type MethodState AddShardItemAcid = ShardItem

instance UpdateEvent AddShardItemAcid


-- ItemShard

instance IsAcidic ShardItem where
  acidEvents = [ QueryEvent  (\(GetShardItemAcid k) -> getShardItemAcid k)
               , UpdateEvent (\(AddShardItemAcid i) -> addShardItemAcid i)
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

addItemConv :: (MonadIO m, ToItem i) => Handle -> i -> FeedID -> URL -> m Item
addItemConv h it fid u = do
  df <- liftIO getCurrentTime
  let (i, as, cs) = toItem it fid u df
  as' <- sequence $ addPerson h <$> as
  cs' <- sequence $ addPerson h <$> cs
  let i' = i { itemAuthors      = personID <$> as'
             , itemContributors = personID <$> cs'
             }
  mupdate h $ AddMasterItemAcid i'

addFeedConv :: (MonadIO m, ToFeed f) => Handle -> f -> CatID -> URL -> m Feed
addFeedConv h it cid u = do
  df <- liftIO getCurrentTime
  let (f, as, cs) = toFeed it cid u df
  as' <- sequence $ addPerson h <$> as
  cs' <- sequence $ addPerson h <$> cs
  let f' = f { feedAuthors      = personID <$> as'
             , feedContributors = personID <$> cs'
             }
  mupdate h $ AddFeedAcid f'

------------------------------------------------------------------------------
-- Main DB Functions
------------------------------------------------------------------------------

open :: MonadIO m => Maybe FilePath -> m Handle
open r = do
  let root = fromMaybe "data" r
  acid <- liftIO $ openLocalStateFrom (root </> "master") emptyDB
  s <- liftIO $ newMVar Map.empty
  return $ Handle DBState { rootDir = root
                          , master  = acid
                          , shards  = s
                          }

close :: MonadIO m => Handle -> m ()
close h = do
  liftIO $ closeAcidState $ master $ unHandle h
  closeShards h

checkpoint :: MonadIO m => Handle -> m ()
checkpoint = liftIO . createCheckpoint . master . unHandle

archive :: MonadIO m => Handle -> m ()
archive = liftIO . createArchive . master . unHandle

getStats :: MonadIO m => Handle -> m (Stats, [(ItemID, UTCTime)])
getStats h = do
  s <- mquery h GetStatsAcid
  liftIO $ bracket
    (takeMVar $ shards $ unHandle h)
    (putMVar $ shards $ unHandle h)
    (\ss -> return (s, (\(k,(t,_)) -> (ItemID k, t)) <$> Map.toList ss ))

getCat :: MonadIO m => Handle -> Int -> m (Maybe Cat)
getCat h c = mquery h $ GetCatAcid c

getFeed :: MonadIO m => Handle -> Int -> m (Maybe Feed)
getFeed h f = mquery h $ GetFeedAcid f

getPerson :: MonadIO m => Handle -> Int -> m (Maybe Person)
getPerson h p = mquery h $ GetPersonAcid p

getItem :: MonadIO m => Handle -> Int -> m (Maybe Item)
getItem h k = do
  s <- mquery h $ GetItemShardAcid $ ItemID k
  withShard h s $ \(a, _) ->
    liftIO $ query a $ GetShardItemAcid $ ItemID k

getCats :: MonadIO m => Handle -> m (Seq.Seq Cat)
getCats h = mquery h GetCatsAcid

getFeeds :: MonadIO m => Handle -> m (Seq.Seq Feed)
getFeeds h = mquery h GetFeedsAcid

addCat :: MonadIO m => Handle -> Cat -> m Cat
addCat h c = mupdate h $ AddCatAcid c

addFeed :: MonadIO m => Handle -> Feed -> m Feed
addFeed h f = mupdate h $ AddFeedAcid f

addPerson :: MonadIO m => Handle -> Person -> m Person
addPerson h c = mupdate h $ AddPersonAcid c

addItems :: MonadIO m => Handle -> [Item] -> m [Item]
addItems h is = do
  iss <- forM is $ \i -> do
    iid <- mquery h $ MkUniqueIDAcid i
    s   <- mquery h $ GetItemShardAcid iid
    return (i { itemID = iid }, s)
  let gss = groupBy (\(_,x) (_,y) -> x == y) $ sortBy (\(_,x) (_,y) -> compare x y) iss
  gss' <- forM gss $ \gs ->
    withShard h (snd $ head gs) $ \(a, _) ->
      forM gs $ \(i, _) -> do
        _ <- liftIO $ update a $ AddShardItemAcid i
        _ <- mupdate h $ AddMasterItemAcid i
        return i
  return $ concat gss'

wipeDB :: MonadIO m => Handle -> m ()
wipeDB h = do
  ss <- mquery h GetShItemAcid
  mupdate h WipeMasterAcid
  closeShards h
  let ss' = 0 : Set.toList ss
  forM_ ss' $ \s -> do
    let sf = shardPath h $ ItemID s
    ex <- liftIO $ doesDirectoryExist sf
    if ex then liftIO $ removeDirectoryRecursive sf
    else liftIO $ putStrLn $ "  Shard dir not found: " ++ sf
