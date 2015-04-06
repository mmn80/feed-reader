{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies    #-}

module FeedReader.Queries
  ( GetStatsAcid (..)
  , GetPendingOpAcid (..)
  , GetCatsAcid (..)
  , GetFeedsAcid (..)
  , GetShardsAcid (..)
  , GetShardIdxAcid (..)
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
  , UpdatePendingOpAcid(..)
  , WipeMasterAcid (..)
  , GetShardItemAcid (..)
  , GetShardItemsAcid (..)
  , GetStatsShardAcid (..)
  , SplitShardAcid (..)
  , AddShardItemAcid (..)
  ) where

import           Control.Monad.Reader  (ask)
import           Control.Monad.State   (get, put)
import           Data.Acid
import           Data.Acid.Advanced
import           Data.Hashable         (hash)
import qualified Data.IntMap           as Map
import qualified Data.IntSet           as Set
import           Data.Maybe            (fromJust)
import           Data.SafeCopy
import qualified Data.Sequence         as Seq
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           FeedReader.Types

findShard :: Int -> Master -> (ShardID, ShardSize)
findShard k m = case Map.lookupLE k (idxItem m) of
                  Just (s, ShardIdx sz _) -> (ShardID s, sz)
                  Nothing                 -> (unsetShardID, 0)

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
  let sz = Map.size (idxItem db)
  let sza = sum $ shardSize <$> idxItem db
  return StatsMaster
           { statsPending  = pendingOp db
           , countCats     = Map.size $ tblCat    db
           , countFeeds    = Map.size $ tblFeed   db
           , countPersons  = Map.size $ tblPerson db
           , countItemsAll = sza
           , countShards   = if sz == 0 then 1 else sz
           }

getPendingOpAcid :: Query Master PendingOp
getPendingOpAcid = do
  db <- ask
  return $ pendingOp db

getCatsAcid :: Query Master (Seq.Seq Cat)
getCatsAcid = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblCat db

getFeedsAcid :: Query Master (Seq.Seq Feed)
getFeedsAcid = do
  db <- ask
  return $ Map.foldr (flip (Seq.|>)) Seq.empty $ tblFeed db

getShardIdxAcid :: Int -> Query Master (Maybe ShardIdx)
getShardIdxAcid k = do
  db <- ask
  return $ Map.lookup k $ idxItem db

getShardsAcid :: Query Master (Seq.Seq (ShardID, ShardSize))
getShardsAcid = do
  db <- ask
  let f k (ShardIdx l _) s = s Seq.|> (ShardID k, l)
  if null (idxItem db) then return $ Seq.singleton (unsetShardID, 0)
  else return $ Map.foldrWithKey f Seq.empty $ idxItem db

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

getItemShardAcid :: ItemID -> Query Master ShardID
getItemShardAcid i = do
  db <- ask
  return $ fst $ findShard (unItemID i) db

mkUniqueIDAcid :: Item -> Query Master ItemID
mkUniqueIDAcid i = do
  db <- ask
  let checkSet sid idx x = if Set.notMember x (shardKeys idx) then x
                           else checkSet sid idx (x + 1)
  let x0 = unItemID $ calcItemID i
  let stop sid x = x < unShardID sid
  return $ ItemID $ pageFoldPrimaryIdx (fst $ findShard x0 db)
                                       checkSet stop x0 $ idxItem db

getItemPageAcid :: Int -> Int -> Query Master [(ItemID, ShardID)]
getItemPageAcid i l = do
  db <- ask
  let go sid x (n, rs) m =
        if n == 0 then (0, rs)
        else case Set.lookupGT x m of
               Nothing -> (n, rs)
               Just x' -> go sid x' (n - 1, (ItemID x', sid) : rs) m
  let f sid (ShardIdx sz s) (n, rs) = go sid i (n, rs) s
  let stop k (n, _) = n == 0
  return $ snd $ pageFoldPrimaryIdx (fst $ findShard i db) f stop (l, []) $ idxItem db

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
  let (ShardID sid, _) = findShard iid db
  put db { pendingOp  = PendingAddItem (itemID i) i
         , idxItem    = insertPrimaryIdx sid iid $ idxItem db
         , ixFeedItem = insertNestedIdx fid iid $ ixFeedItem db
         , ixCatItem  = case mbc of
             Just cid -> insertNestedIdx cid iid $ ixCatItem db
             _        -> ixCatItem db
         }
  return i

splitShardMasterAcid :: ShardID -> Update Master ([ItemID], [ItemID],
                        ShardSize, ShardSize, ShardID)
splitShardMasterAcid (ShardID sid) = do
  db <- get
  let is = Set.toList $ shardKeys $ fromJust $ Map.lookup sid $ idxItem db
  let (lsz, r) = length is `divMod` 2
  let rsz = lsz + r
  let (ls, rs) = (take lsz is, drop lsz is)
  let rid = head rs
  let (lis, ris) = (Set.fromList ls, Set.fromList rs)
  put db { pendingOp = PendingSplitPhase0 (ShardID sid) (ShardID rid)
         , idxItem   = Map.insert rid (ShardIdx rsz ris) $
                       Map.insert sid (ShardIdx lsz lis) $ idxItem db }
  return (ItemID <$> ls, ItemID <$> rs, lsz, rsz, ShardID rid)

updatePendingOpAcid :: PendingOp -> Update Master ()
updatePendingOpAcid op = do
  db <- get
  put db { pendingOp = op }

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
           , shardID = ShardID $ fst $ Map.findMin $ tblItem db
           }

getShardItemAcid :: ItemID -> Query Shard (Maybe Item)
getShardItemAcid (ItemID k) = do
  db <- ask
  return $ Map.lookup k (tblItem db)

getShardItemsAcid :: [ItemID] -> Query Shard [Maybe Item]
getShardItemsAcid is = do
  db <- ask
  return $ (\(ItemID k) -> Map.lookup k (tblItem db)) <$> is

addShardItemAcid :: Item -> Update Shard Item
addShardItemAcid i = do
  db <- get
  put $ db { shSize = shSize db + 1
           , tblItem = Map.insert (unItemID $ itemID i) i $ tblItem db
           }
  return i

splitShardAcid :: ItemID -> ShardSize -> Update Shard ()
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
$(deriveSafeCopy 0 'base ''ShardID)
$(deriveSafeCopy 0 'base ''StatsMaster)
$(deriveSafeCopy 0 'base ''StatsShard)
$(deriveSafeCopy 0 'base ''ShardIdx)
$(deriveSafeCopy 0 'base ''PendingOp)
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


data GetPendingOpAcid = GetPendingOpAcid

$(deriveSafeCopy 0 'base ''GetPendingOpAcid)

instance Method GetPendingOpAcid where
  type MethodResult GetPendingOpAcid = PendingOp
  type MethodState GetPendingOpAcid = Master

instance QueryEvent GetPendingOpAcid


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


data GetShardsAcid = GetShardsAcid

$(deriveSafeCopy 0 'base ''GetShardsAcid)

instance Method GetShardsAcid where
  type MethodResult GetShardsAcid = Seq.Seq (ShardID, ShardSize)
  type MethodState GetShardsAcid = Master

instance QueryEvent GetShardsAcid


data GetShardIdxAcid = GetShardIdxAcid Int

$(deriveSafeCopy 0 'base ''GetShardIdxAcid)

instance Method GetShardIdxAcid where
  type MethodResult GetShardIdxAcid = Maybe ShardIdx
  type MethodState GetShardIdxAcid = Master

instance QueryEvent GetShardIdxAcid


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
  type MethodResult GetItemShardAcid = ShardID
  type MethodState GetItemShardAcid = Master

instance QueryEvent GetItemShardAcid


data GetItemPageAcid = GetItemPageAcid Int Int

$(deriveSafeCopy 0 'base ''GetItemPageAcid)

instance Method GetItemPageAcid where
  type MethodResult GetItemPageAcid = [(ItemID, ShardID)]
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


data SplitShardMasterAcid = SplitShardMasterAcid ShardID

$(deriveSafeCopy 0 'base ''SplitShardMasterAcid)

instance Method SplitShardMasterAcid where
  type MethodResult SplitShardMasterAcid = ([ItemID], [ItemID],
                        ShardSize, ShardSize, ShardID)
  type MethodState SplitShardMasterAcid = Master

instance UpdateEvent SplitShardMasterAcid


data UpdatePendingOpAcid = UpdatePendingOpAcid PendingOp

$(deriveSafeCopy 0 'base ''UpdatePendingOpAcid)

instance Method UpdatePendingOpAcid where
  type MethodResult UpdatePendingOpAcid = ()
  type MethodState UpdatePendingOpAcid = Master

instance UpdateEvent UpdatePendingOpAcid


data WipeMasterAcid = WipeMasterAcid

$(deriveSafeCopy 0 'base ''WipeMasterAcid)

instance Method WipeMasterAcid where
  type MethodResult WipeMasterAcid = ()
  type MethodState WipeMasterAcid = Master

instance UpdateEvent WipeMasterAcid

instance IsAcidic Master where
  acidEvents = [
      QueryEvent  (\ GetStatsAcid            -> getStatsAcid)
    , QueryEvent  (\ GetPendingOpAcid        -> getPendingOpAcid)
    , QueryEvent  (\ GetCatsAcid             -> getCatsAcid)
    , QueryEvent  (\ GetFeedsAcid            -> getFeedsAcid)
    , QueryEvent  (\ GetShardsAcid           -> getShardsAcid)
    , QueryEvent  (\(GetShardIdxAcid k)      -> getShardIdxAcid k)
    , QueryEvent  (\(GetCatAcid  k)          -> getCatAcid k)
    , QueryEvent  (\(GetFeedAcid k)          -> getFeedAcid k)
    , QueryEvent  (\(GetPersonAcid k)        -> getPersonAcid k)
    , QueryEvent  (\(GetItemShardAcid k)     -> getItemShardAcid k)
    , QueryEvent  (\(GetItemPageAcid k l)    -> getItemPageAcid k l)
    , QueryEvent  (\(MkUniqueIDAcid i)       -> mkUniqueIDAcid i)
    , UpdateEvent (\(AddCatAcid  c)          -> addCatAcid c)
    , UpdateEvent (\(AddFeedAcid f)          -> addFeedAcid f)
    , UpdateEvent (\(AddPersonAcid p)        -> addPersonAcid p)
    , UpdateEvent (\(AddMasterItemAcid i)    -> addMasterItemAcid i)
    , UpdateEvent (\(SplitShardMasterAcid k) -> splitShardMasterAcid k)
    , UpdateEvent (\(UpdatePendingOpAcid op) -> updatePendingOpAcid op)
    , UpdateEvent (\ WipeMasterAcid          -> wipeMasterAcid)
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
               , QueryEvent  (\GetStatsShardAcid      -> getStatsShardAcid)
               , UpdateEvent (\(AddShardItemAcid i)   -> addShardItemAcid i)
               , UpdateEvent (\(SplitShardAcid k sz)  -> splitShardAcid k sz)
               ]
