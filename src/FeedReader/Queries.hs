{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies    #-}

module FeedReader.Queries
  ( GetStatsAcid (..)
  , GetCatsAcid (..)
  , GetFeedsAcid (..)
  , GetShItemAcid (..)
  , GetCatAcid (..)
  , GetFeedAcid (..)
  , GetPersonAcid (..)
  , GetItemShardAcid (..)
  , GetItemPageAcid (..)
  , MkUniqueIDAcid (..)
  , AddCatAcid (..)
  , AddFeedAcid (..)
  , AddPersonAcid (..)
  , AddMasterItemAcid (..)
  , WipeMasterAcid (..)
  , GetShardItemAcid (..)
  , GetShardItemsAcid (..)
  , GetStatsShardAcid (..)
  , AddShardItemAcid (..)
  ) where

import           Control.Monad.Reader (ask)
import           Control.Monad.State  (get, put)
import           Data.Acid
import           Data.Acid.Advanced
import           Data.SafeCopy
import           FeedReader.Types
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import qualified Data.Sequence         as Seq
import           Data.Hashable         (hash)
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)

findShard :: Int -> Master -> ItemID
findShard k m = ItemID $ case Set.lookupLE k (idxShard m) of
                           Just s -> s
                           _      -> 0

checkUniqueID :: Set.IntSet -> Int -> Int
checkUniqueID idx x = if Set.notMember x idx then x
                      else checkUniqueID idx (x + 1)

calcCatID :: Cat -> CatID
calcCatID = CatID . hash . catName

calcFeedID :: Feed -> FeedID
calcFeedID = FeedID . hash . feedURL

calcItemID :: Item -> ItemID
calcItemID = ItemID . fromInteger . round . utcTimeToPOSIXSeconds . itemUpdated

calcPersonID :: Person -> PersonID
calcPersonID p = PersonID $ hash $ personName p ++ personEmail p

------------------------------------------------------------------------------
-- Read Queries
------------------------------------------------------------------------------

getStatsAcid :: Query Master StatsMaster
getStatsAcid = do
  db <- ask
  return StatsMaster
           { countCats    = Map.size $ tblCat    db
           , countFeeds   = Map.size $ tblFeed   db
           , countPersons = Map.size $ tblPerson db
           , countItemsAll   = Set.size $ idxItem   db
           , countShards  = 1 + Set.size (idxShard db)
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
  return $ idxShard db

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
getItemShardAcid i = do
  db <- ask
  return $ findShard (unItemID i) db

mkUniqueIDAcid :: Item -> Query Master ItemID
mkUniqueIDAcid i = do
  db <- ask
  return $ ItemID $ checkUniqueID (idxItem db) $ unItemID $ calcItemID i

getItemPageAcid :: Int -> Int -> Query Master [(ItemID, ItemID)]
getItemPageAcid i l = do
  db <- ask
  let go n x r = if n == 0 then r
                 else case Set.lookupGT x (idxItem db) of
                        Nothing -> r
                        Just x' -> go (n - 1) x' $ (ItemID x', findShard x' db) : r
  return $ go l i []


-- Shard

getStatsShardAcid :: Query Shard StatsShard
getStatsShardAcid = do
  db <- ask
  return StatsShard
           { countItems = shSize db
           }

getShardItemAcid :: ItemID -> Query Shard (Maybe Item)
getShardItemAcid (ItemID k) = do
  db <- ask
  return $ Map.lookup k (tblItem db)

getShardItemsAcid :: [ItemID] -> Query Shard [Maybe Item]
getShardItemsAcid is = do
  db <- ask
  return $ (\(ItemID k) -> Map.lookup k (tblItem db)) <$> is

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
wipeMasterAcid = put emptyMaster

-- Shard

addShardItemAcid :: Item -> Update Shard Item
addShardItemAcid i = do
  db <- get
  put $ db { shSize = shSize db + 1
           , tblItem = Map.insert (unItemID $ itemID i) i $ tblItem db
           }
  return i

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
$(deriveSafeCopy 0 'base ''StatsMaster)
$(deriveSafeCopy 0 'base ''StatsShard)
$(deriveSafeCopy 0 'base ''Master)
$(deriveSafeCopy 0 'base ''Shard)

------------------------------------------------------------------------------
-- makeAcidic
------------------------------------------------------------------------------

-- getStatsAcid

data GetStatsAcid = GetStatsAcid

$(deriveSafeCopy 0 'base ''GetStatsAcid)

instance Method GetStatsAcid where
  type MethodResult GetStatsAcid = StatsMaster
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

-- getItemShardAcid

data GetItemShardAcid = GetItemShardAcid ItemID

$(deriveSafeCopy 0 'base ''GetItemShardAcid)

instance Method GetItemShardAcid where
  type MethodResult GetItemShardAcid = ItemID
  type MethodState GetItemShardAcid = Master

instance QueryEvent GetItemShardAcid

-- getItemPageAcid

data GetItemPageAcid = GetItemPageAcid Int Int

$(deriveSafeCopy 0 'base ''GetItemPageAcid)

instance Method GetItemPageAcid where
  type MethodResult GetItemPageAcid = [(ItemID, ItemID)]
  type MethodState GetItemPageAcid = Master

instance QueryEvent GetItemPageAcid


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
               , QueryEvent  (\(GetItemPageAcid k l) -> getItemPageAcid k l)
               , QueryEvent  (\(MkUniqueIDAcid i)    -> mkUniqueIDAcid i)
               , UpdateEvent (\(AddCatAcid  c)       -> addCatAcid c)
               , UpdateEvent (\(AddFeedAcid f)       -> addFeedAcid f)
               , UpdateEvent (\(AddPersonAcid p)     -> addPersonAcid p)
               , UpdateEvent (\(AddMasterItemAcid i) -> addMasterItemAcid i)
               , UpdateEvent (\ WipeMasterAcid       -> wipeMasterAcid)
               ]



-- getStatsShardAcid

data GetStatsShardAcid = GetStatsShardAcid

$(deriveSafeCopy 0 'base ''GetStatsShardAcid)

instance Method GetStatsShardAcid where
  type MethodResult GetStatsShardAcid = StatsShard
  type MethodState GetStatsShardAcid = Shard

instance QueryEvent GetStatsShardAcid

-- getShardItemAcid

data GetShardItemAcid = GetShardItemAcid ItemID

$(deriveSafeCopy 0 'base ''GetShardItemAcid)

instance Method GetShardItemAcid where
  type MethodResult GetShardItemAcid = Maybe Item
  type MethodState GetShardItemAcid = Shard

instance QueryEvent GetShardItemAcid

-- getShardItemsAcid

data GetShardItemsAcid = GetShardItemsAcid [ItemID]

$(deriveSafeCopy 0 'base ''GetShardItemsAcid)

instance Method GetShardItemsAcid where
  type MethodResult GetShardItemsAcid = [Maybe Item]
  type MethodState GetShardItemsAcid = Shard

instance QueryEvent GetShardItemsAcid

-- addShardItemAcid

data AddShardItemAcid = AddShardItemAcid Item

$(deriveSafeCopy 0 'base ''AddShardItemAcid)

instance Method AddShardItemAcid where
  type MethodResult AddShardItemAcid = Item
  type MethodState AddShardItemAcid = Shard

instance UpdateEvent AddShardItemAcid


-- ItemShard

instance IsAcidic Shard where
  acidEvents = [ QueryEvent  (\(GetShardItemAcid k)   -> getShardItemAcid k)
               , QueryEvent  (\(GetShardItemsAcid is) -> getShardItemsAcid is)
               , QueryEvent  (\GetStatsShardAcid      -> getStatsShardAcid)
               , UpdateEvent (\(AddShardItemAcid i)   -> addShardItemAcid i)
               ]
