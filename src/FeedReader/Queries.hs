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
  , SplitShardMasterAcid(..)
  , WipeMasterAcid (..)
  , GetShardItemAcid (..)
  , GetShardItemsAcid (..)
  , GetStatsShardAcid (..)
  , GetShardSplit (..)
  , SplitShardAcid (..)
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

findShard :: Int -> Master -> (ItemID, Int)
findShard k m = case Map.lookupLE k (idxShard m) of
                  Just (s, l) -> (ItemID s, l)
                  Nothing     -> (ItemID 0, 0)

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
-- Master Queries
------------------------------------------------------------------------------

getStatsAcid :: Query Master StatsMaster
getStatsAcid = do
  db <- ask
  let sz = Map.size (idxShard db)
  return StatsMaster
           { countCats     = Map.size $ tblCat    db
           , countFeeds    = Map.size $ tblFeed   db
           , countPersons  = Map.size $ tblPerson db
           , countItemsAll = Set.size $ idxItem   db
           , countShards   = if sz == 0 then 1 else sz
           }

getCatsAcid :: Query Master (Seq.Seq Cat)
getCatsAcid = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblCat db

getFeedsAcid :: Query Master (Seq.Seq Feed)
getFeedsAcid = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblFeed db

getShItemAcid :: Query Master (Map.IntMap Int)
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
  return $ fst $ findShard (unItemID i) db

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
                        Just x' -> go (n - 1) x' $ (ItemID x', fst $ findShard x' db) : r
  return $ go l i []

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
  let (ItemID sid, l) = findShard iid db
  put db { idxShard = Map.insert sid (l + 1) $ idxShard db
         , idxItem = Set.insert iid $ idxItem db
         , ixFeedItem = insertNested fid iid $ ixFeedItem db
         , ixCatItem  = case mbc of
             Just cid -> insertNested cid iid $ ixCatItem db
             _        -> ixCatItem db
         }
  return i

splitShardMasterAcid :: ItemID -> Int -> ItemID -> Int -> Update Master ()
splitShardMasterAcid (ItemID lid) lsz (ItemID rid) rsz = do
  db <- get
  let s = Map.insert lid lsz $ idxShard db
  put db { idxShard = Map.insert rid rsz s }

wipeMasterAcid :: Update Master ()
wipeMasterAcid = put emptyMaster

------------------------------------------------------------------------------
-- Shard Queries
------------------------------------------------------------------------------

getStatsShardAcid :: Query Shard StatsShard
getStatsShardAcid = do
  db <- ask
  return StatsShard
           { countItems = shSize db
           , shardID = ItemID $ fst $ Map.findMin $ tblItem db
           }

getShardItemAcid :: ItemID -> Query Shard (Maybe Item)
getShardItemAcid (ItemID k) = do
  db <- ask
  return $ Map.lookup k (tblItem db)

getShardItemsAcid :: [ItemID] -> Query Shard [Maybe Item]
getShardItemsAcid is = do
  db <- ask
  return $ (\(ItemID k) -> Map.lookup k (tblItem db)) <$> is

getShardSplit :: Query Shard (Map.IntMap Item, Map.IntMap Item, Int, Int)
getShardSplit = do
  db <- ask
  let as = Map.assocs $ tblItem db
  let (lsz, rsz) = shSize db `divMod` 2
  return (Map.fromList $ take lsz as, Map.fromList $ drop lsz as, lsz, rsz)

addShardItemAcid :: Item -> Update Shard Item
addShardItemAcid i = do
  db <- get
  put $ db { shSize = shSize db + 1
           , tblItem = Map.insert (unItemID $ itemID i) i $ tblItem db
           }
  return i

splitShardAcid :: ItemID -> Int -> Update Shard ()
splitShardAcid (ItemID rid) sz = do
  db <- get
  put $ db { shSize = sz
           , tblItem = fst $ Map.split rid $ tblItem db
           }


------------------------------------------------------------------------------
-- SafeCopy Instances
------------------------------------------------------------------------------

-- TODO: No TemplateHaskell

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

data GetStatsAcid = GetStatsAcid

$(deriveSafeCopy 0 'base ''GetStatsAcid)

instance Method GetStatsAcid where
  type MethodResult GetStatsAcid = StatsMaster
  type MethodState GetStatsAcid = Master

instance QueryEvent GetStatsAcid


data GetCatsAcid = GetCatsAcid

$(deriveSafeCopy 0 'base ''GetCatsAcid)

instance Method GetCatsAcid where
  type MethodResult GetCatsAcid = Seq.Seq Cat
  type MethodState GetCatsAcid = Master

instance QueryEvent GetCatsAcid


data GetFeedsAcid = GetFeedsAcid

$(deriveSafeCopy 0 'base ''GetFeedsAcid)

instance Method GetFeedsAcid where
  type MethodResult GetFeedsAcid = Seq.Seq Feed
  type MethodState GetFeedsAcid = Master

instance QueryEvent GetFeedsAcid


data GetShItemAcid = GetShItemAcid

$(deriveSafeCopy 0 'base ''GetShItemAcid)

instance Method GetShItemAcid where
  type MethodResult GetShItemAcid = Map.IntMap Int
  type MethodState GetShItemAcid = Master

instance QueryEvent GetShItemAcid


data GetCatAcid = GetCatAcid Int

$(deriveSafeCopy 0 'base ''GetCatAcid)

instance Method GetCatAcid where
  type MethodResult GetCatAcid = Maybe Cat
  type MethodState GetCatAcid = Master

instance QueryEvent GetCatAcid


data GetFeedAcid = GetFeedAcid Int

$(deriveSafeCopy 0 'base ''GetFeedAcid)

instance Method GetFeedAcid where
  type MethodResult GetFeedAcid = Maybe Feed
  type MethodState GetFeedAcid = Master

instance QueryEvent GetFeedAcid


data GetPersonAcid = GetPersonAcid Int

$(deriveSafeCopy 0 'base ''GetPersonAcid)

instance Method GetPersonAcid where
  type MethodResult GetPersonAcid = Maybe Person
  type MethodState GetPersonAcid = Master

instance QueryEvent GetPersonAcid


data GetItemShardAcid = GetItemShardAcid ItemID

$(deriveSafeCopy 0 'base ''GetItemShardAcid)

instance Method GetItemShardAcid where
  type MethodResult GetItemShardAcid = ItemID
  type MethodState GetItemShardAcid = Master

instance QueryEvent GetItemShardAcid


data GetItemPageAcid = GetItemPageAcid Int Int

$(deriveSafeCopy 0 'base ''GetItemPageAcid)

instance Method GetItemPageAcid where
  type MethodResult GetItemPageAcid = [(ItemID, ItemID)]
  type MethodState GetItemPageAcid = Master

instance QueryEvent GetItemPageAcid


data MkUniqueIDAcid = MkUniqueIDAcid Item

$(deriveSafeCopy 0 'base ''MkUniqueIDAcid)

instance Method MkUniqueIDAcid where
  type MethodResult MkUniqueIDAcid = ItemID
  type MethodState MkUniqueIDAcid = Master

instance QueryEvent MkUniqueIDAcid


data AddCatAcid = AddCatAcid Cat

$(deriveSafeCopy 0 'base ''AddCatAcid)

instance Method AddCatAcid where
  type MethodResult AddCatAcid = Cat
  type MethodState AddCatAcid = Master

instance UpdateEvent AddCatAcid


data AddFeedAcid = AddFeedAcid Feed

$(deriveSafeCopy 0 'base ''AddFeedAcid)

instance Method AddFeedAcid where
  type MethodResult AddFeedAcid = Feed
  type MethodState AddFeedAcid = Master

instance UpdateEvent AddFeedAcid


data AddPersonAcid = AddPersonAcid Person

$(deriveSafeCopy 0 'base ''AddPersonAcid)

instance Method AddPersonAcid where
  type MethodResult AddPersonAcid = Person
  type MethodState AddPersonAcid = Master

instance UpdateEvent AddPersonAcid


data AddMasterItemAcid = AddMasterItemAcid Item

$(deriveSafeCopy 0 'base ''AddMasterItemAcid)

instance Method AddMasterItemAcid where
  type MethodResult AddMasterItemAcid = Item
  type MethodState AddMasterItemAcid = Master

instance UpdateEvent AddMasterItemAcid


data SplitShardMasterAcid = SplitShardMasterAcid ItemID Int ItemID Int

$(deriveSafeCopy 0 'base ''SplitShardMasterAcid)

instance Method SplitShardMasterAcid where
  type MethodResult SplitShardMasterAcid = ()
  type MethodState SplitShardMasterAcid = Master

instance UpdateEvent SplitShardMasterAcid


data WipeMasterAcid = WipeMasterAcid

$(deriveSafeCopy 0 'base ''WipeMasterAcid)

instance Method WipeMasterAcid where
  type MethodResult WipeMasterAcid = ()
  type MethodState WipeMasterAcid = Master

instance UpdateEvent WipeMasterAcid

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
               , UpdateEvent (\(SplitShardMasterAcid lid lsz rid rsz) ->
                                  splitShardMasterAcid lid lsz rid rsz)
               , UpdateEvent (\ WipeMasterAcid       -> wipeMasterAcid)
               ]


data GetStatsShardAcid = GetStatsShardAcid

$(deriveSafeCopy 0 'base ''GetStatsShardAcid)

instance Method GetStatsShardAcid where
  type MethodResult GetStatsShardAcid = StatsShard
  type MethodState GetStatsShardAcid = Shard

instance QueryEvent GetStatsShardAcid


data GetShardItemAcid = GetShardItemAcid ItemID

$(deriveSafeCopy 0 'base ''GetShardItemAcid)

instance Method GetShardItemAcid where
  type MethodResult GetShardItemAcid = Maybe Item
  type MethodState GetShardItemAcid = Shard

instance QueryEvent GetShardItemAcid


data GetShardItemsAcid = GetShardItemsAcid [ItemID]

$(deriveSafeCopy 0 'base ''GetShardItemsAcid)

instance Method GetShardItemsAcid where
  type MethodResult GetShardItemsAcid = [Maybe Item]
  type MethodState GetShardItemsAcid = Shard

instance QueryEvent GetShardItemsAcid


data GetShardSplit = GetShardSplit

$(deriveSafeCopy 0 'base ''GetShardSplit)

instance Method GetShardSplit where
  type MethodResult GetShardSplit = (Map.IntMap Item, Map.IntMap Item, Int, Int)
  type MethodState GetShardSplit = Shard

instance QueryEvent GetShardSplit


data AddShardItemAcid = AddShardItemAcid Item

$(deriveSafeCopy 0 'base ''AddShardItemAcid)

instance Method AddShardItemAcid where
  type MethodResult AddShardItemAcid = Item
  type MethodState AddShardItemAcid = Shard

instance UpdateEvent AddShardItemAcid


data SplitShardAcid = SplitShardAcid ItemID Int

$(deriveSafeCopy 0 'base ''SplitShardAcid)

instance Method SplitShardAcid where
  type MethodResult SplitShardAcid = ()
  type MethodState SplitShardAcid = Shard

instance UpdateEvent SplitShardAcid


instance IsAcidic Shard where
  acidEvents = [ QueryEvent  (\(GetShardItemAcid k)   -> getShardItemAcid k)
               , QueryEvent  (\(GetShardItemsAcid is) -> getShardItemsAcid is)
               , QueryEvent  (\GetShardSplit          -> getShardSplit)
               , QueryEvent  (\GetStatsShardAcid      -> getStatsShardAcid)
               , UpdateEvent (\(AddShardItemAcid i)   -> addShardItemAcid i)
               , UpdateEvent (\(SplitShardAcid k sz)  -> splitShardAcid k sz)
               ]
