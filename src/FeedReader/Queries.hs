{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies    #-}

module FeedReader.Queries
  ( GetStatsAcid (..)
  , GetPendingOpAcid (..)
  , GetCatsAcid (..)
  , GetFeedsAcid (..)
  , GetPersonsAcid (..)
  , GetShardsAcid (..)
  , GetShardIdxAcid (..)
  , FindCatAcid (..)
  , FindFeedAcid (..)
  , FindPersonAcid (..)
  , FindItemShardAcid (..)
  , GetItemPageAcid (..)
  , MkUniqueIDAcid (..)
  , AddCatAcid (..)
  , AddFeedAcid (..)
  , AddPersonAcid (..)
  , AddMasterItemAcid (..)
  , SplitShardMasterAcid(..)
  , UpdatePendingOpAcid(..)
  , WipeMasterAcid (..)
  , FindShardItemAcid (..)
  , GetShardItemsAcid (..)
  , GetShardSizeAcid (..)
  , SplitShardAcid (..)
  , AddShardItemAcid (..)
  ) where

import           Control.Monad.Reader  (ask)
import           Control.Monad.State   (get, put)
import           Data.Acid
import           Data.Acid.Advanced
import           Data.SafeCopy
import qualified Data.Sequence         as Seq
import           FeedReader.Types

------------------------------------------------------------------------------
-- Master Queries
------------------------------------------------------------------------------

getStatsAcid :: Query Master StatsMaster
getStatsAcid = do
  db <- ask
  let sz = sizeShards $ idxItem db
  let sza = sizePrimary $ idxItem db
  return StatsMaster
           { statsPending  = pendingOp db
           , countCats     = tableSize $ tblCat    db
           , countFeeds    = tableSize $ tblFeed   db
           , countPersons  = tableSize $ tblPerson db
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
  return $ foldr (flip (Seq.|>)) Seq.empty $ tblCat db

getFeedsAcid :: Query Master (Seq.Seq Feed)
getFeedsAcid = do
  db <- ask
  return $ foldr (flip (Seq.|>)) Seq.empty $ tblFeed db

getPersonsAcid :: Query Master (Seq.Seq Person)
getPersonsAcid = do
  db <- ask
  return $ foldr (flip (Seq.|>)) Seq.empty $ tblPerson db

getShardIdxAcid :: ShardID -> Query Master (Maybe (ShardIdx ItemID))
getShardIdxAcid k = do
  db <- ask
  return $ findShard k $ idxItem db

getShardsAcid :: Query Master (Seq.Seq (ShardID, ShardSize))
getShardsAcid = do
  db <- ask
  return $ getShardsInfo $ idxItem db

findCatAcid :: Int -> Query Master (Maybe Cat)
findCatAcid k = do
  db <- ask
  return $ tableLookup k (tblCat db)

findFeedAcid :: Int -> Query Master (Maybe Feed)
findFeedAcid k = do
  db <- ask
  return $ tableLookup k (tblFeed db)

findPersonAcid :: Int -> Query Master (Maybe Person)
findPersonAcid k = do
  db <- ask
  return $ tableLookup k (tblPerson db)

findItemShardAcid :: Int -> Query Master ShardID
findItemShardAcid i = do
  db <- ask
  return $ fst $ findParentShard i $ idxItem db

mkUniqueIDAcid :: Item -> Query Master ItemID
mkUniqueIDAcid i = do
  db <- ask
  return $ findUnusedID (fromRecord i) $ idxItem db

getItemPageAcid :: Int -> Int -> Query Master [(ItemID, ShardID)]
getItemPageAcid i l = do
  db <- ask
  return $ nextPage i l $ idxItem db

addCatAcid :: Cat -> Update Master Cat
addCatAcid c = do
  db <- get
  let cid = fromRecord c
  let c' = c { catID = cid }
  put $ db { tblCat = tableInsert (toInt cid) c' $ tblCat db }
  return c'

addFeedAcid :: Feed -> Update Master Feed
addFeedAcid f = do
  db <- get
  let fid = fromRecord f
  let f' = f { feedID = fid }
  put $ db { tblFeed = tableInsert (toInt fid) f' $ tblFeed db }
  return f'

addPersonAcid :: Person -> Update Master Person
addPersonAcid p = do
  db <- get
  let pid = fromRecord p
  let p' = p { personID = pid }
  put $ db { tblPerson = tableInsert (toInt pid) p' $ tblPerson db }
  return p'

addMasterItemAcid :: Item -> Update Master Item
addMasterItemAcid i = do
  db <- get
  let iid = itemID i
  let fid = itemFeedID i
  let mbc = feedCatID <$> tableLookup (toInt fid) (tblFeed db)
  let (sid, _) = findParentShard (toInt iid) $ idxItem db
  put db { pendingOp  = PendingAddItem (itemID i) i
         , idxItem    = insertPrimary sid iid $ idxItem db
         , ixFeedItem = insertNested fid iid $ ixFeedItem db
         , ixCatItem  = case mbc of
             Just cid -> insertNested cid iid $ ixCatItem db
             _        -> ixCatItem db
         }
  return i

splitShardMasterAcid :: ShardID -> Update Master ([ItemID], [ItemID],
                        ShardSize, ShardSize, ShardID)
splitShardMasterAcid sid = do
  db <- get
  let (idx, (ls, rs, lsz, rsz, rid)) = splitShard sid $ idxItem db
  put db { pendingOp = PendingSplitPhase0 sid rid
         , idxItem   = idx }
  return (ls, rs, lsz, rsz, rid)

updatePendingOpAcid :: PendingOp -> Update Master ()
updatePendingOpAcid op = do
  db <- get
  put db { pendingOp = op }

wipeMasterAcid :: Update Master ()
wipeMasterAcid = put emptyMaster

------------------------------------------------------------------------------
-- Shard Queries
------------------------------------------------------------------------------

getShardSizeAcid :: Query Shard ShardSize
getShardSizeAcid = do
  db <- ask
  return $ shSize db

findShardItemAcid :: Int -> Query Shard (Maybe Item)
findShardItemAcid k = do
  db <- ask
  return $ tableLookup k (tblItem db)

getShardItemsAcid :: [ItemID] -> Query Shard [Maybe Item]
getShardItemsAcid is = do
  db <- ask
  return $ (\k -> tableLookup (toInt k) (tblItem db)) <$> is

addShardItemAcid :: Item -> Update Shard Item
addShardItemAcid i = do
  db <- get
  put $ db { shSize = shSize db + 1
           , tblItem = tableInsert (toInt $ itemID i) i $ tblItem db
           }
  return i

splitShardAcid :: ItemID -> ShardSize -> Update Shard ()
splitShardAcid rid sz = do
  db <- get
  put $ db { shSize = sz
           , tblItem = fst $ tableSplit (toInt rid) $ tblItem db
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
$(deriveSafeCopy 0 'base ''ShardSet)
$(deriveSafeCopy 0 'base ''ShardIdx)
$(deriveSafeCopy 0 'base ''PrimaryIdx)
$(deriveSafeCopy 0 'base ''NestedIdx)
$(deriveSafeCopy 0 'base ''Table)
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


data GetPersonsAcid = GetPersonsAcid

$(deriveSafeCopy 0 'base ''GetPersonsAcid)

instance Method GetPersonsAcid where
  type MethodResult GetPersonsAcid = Seq.Seq Person
  type MethodState GetPersonsAcid = Master

instance QueryEvent GetPersonsAcid


data GetShardsAcid = GetShardsAcid

$(deriveSafeCopy 0 'base ''GetShardsAcid)

instance Method GetShardsAcid where
  type MethodResult GetShardsAcid = Seq.Seq (ShardID, ShardSize)
  type MethodState GetShardsAcid = Master

instance QueryEvent GetShardsAcid


data GetShardIdxAcid = GetShardIdxAcid ShardID

$(deriveSafeCopy 0 'base ''GetShardIdxAcid)

instance Method GetShardIdxAcid where
  type MethodResult GetShardIdxAcid = Maybe (ShardIdx ItemID)
  type MethodState GetShardIdxAcid = Master

instance QueryEvent GetShardIdxAcid


data FindCatAcid = FindCatAcid Int

$(deriveSafeCopy 0 'base ''FindCatAcid)

instance Method FindCatAcid where
  type MethodResult FindCatAcid = Maybe Cat
  type MethodState FindCatAcid = Master

instance QueryEvent FindCatAcid


data FindFeedAcid = FindFeedAcid Int

$(deriveSafeCopy 0 'base ''FindFeedAcid)

instance Method FindFeedAcid where
  type MethodResult FindFeedAcid = Maybe Feed
  type MethodState FindFeedAcid = Master

instance QueryEvent FindFeedAcid


data FindPersonAcid = FindPersonAcid Int

$(deriveSafeCopy 0 'base ''FindPersonAcid)

instance Method FindPersonAcid where
  type MethodResult FindPersonAcid = Maybe Person
  type MethodState FindPersonAcid = Master

instance QueryEvent FindPersonAcid


data FindItemShardAcid = FindItemShardAcid Int

$(deriveSafeCopy 0 'base ''FindItemShardAcid)

instance Method FindItemShardAcid where
  type MethodResult FindItemShardAcid = ShardID
  type MethodState FindItemShardAcid = Master

instance QueryEvent FindItemShardAcid


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
    , QueryEvent  (\ GetPersonsAcid          -> getPersonsAcid)
    , QueryEvent  (\ GetShardsAcid           -> getShardsAcid)
    , QueryEvent  (\(GetShardIdxAcid k)      -> getShardIdxAcid k)
    , QueryEvent  (\(FindCatAcid k)          -> findCatAcid k)
    , QueryEvent  (\(FindFeedAcid k)         -> findFeedAcid k)
    , QueryEvent  (\(FindPersonAcid k)       -> findPersonAcid k)
    , QueryEvent  (\(FindItemShardAcid k)    -> findItemShardAcid k)
    , QueryEvent  (\(GetItemPageAcid k l)    -> getItemPageAcid k l)
    , QueryEvent  (\(MkUniqueIDAcid i)       -> mkUniqueIDAcid i)
    , UpdateEvent (\(AddCatAcid c)           -> addCatAcid c)
    , UpdateEvent (\(AddFeedAcid f)          -> addFeedAcid f)
    , UpdateEvent (\(AddPersonAcid p)        -> addPersonAcid p)
    , UpdateEvent (\(AddMasterItemAcid i)    -> addMasterItemAcid i)
    , UpdateEvent (\(SplitShardMasterAcid k) -> splitShardMasterAcid k)
    , UpdateEvent (\(UpdatePendingOpAcid op) -> updatePendingOpAcid op)
    , UpdateEvent (\ WipeMasterAcid          -> wipeMasterAcid)
    ]


data GetShardSizeAcid = GetShardSizeAcid

$(deriveSafeCopy 0 'base ''GetShardSizeAcid)

instance Method GetShardSizeAcid where
  type MethodResult GetShardSizeAcid = ShardSize
  type MethodState GetShardSizeAcid = Shard

instance QueryEvent GetShardSizeAcid


data FindShardItemAcid = FindShardItemAcid Int

$(deriveSafeCopy 0 'base ''FindShardItemAcid)

instance Method FindShardItemAcid where
  type MethodResult FindShardItemAcid = Maybe Item
  type MethodState FindShardItemAcid = Shard

instance QueryEvent FindShardItemAcid


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
  acidEvents = [ QueryEvent  (\(FindShardItemAcid k)  -> findShardItemAcid k)
               , QueryEvent  (\(GetShardItemsAcid is) -> getShardItemsAcid is)
               , QueryEvent  (\ GetShardSizeAcid      -> getShardSizeAcid)
               , UpdateEvent (\(AddShardItemAcid i)   -> addShardItemAcid i)
               , UpdateEvent (\(SplitShardAcid k sz)  -> splitShardAcid k sz)
               ]
