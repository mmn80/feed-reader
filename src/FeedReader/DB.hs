{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.Data.DB
-- Copyright : (c) 2015 Călin Ardelean
-- License : BSD-style
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides DB access functions for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.DB
  ( module Exports
  , runRange
  , runRangeF
  , runLookup
  , runLookupUnique
  , runInsert
  , runUpdate
  , runDelete
  , runDeleteRange
  , getStats
  , DBStats (..)
  , runToItem
  , runToFeed
  , runUpdateItemStatus
  ) where

import           Control.Monad                (forM_)
import           Control.Monad.Trans          (MonadIO (liftIO))
import           Data.Time.Clock              (getCurrentTime)
import           Database.Muesli.Handle       as Exports
import           Database.Muesli.Query        as Exports
import           FeedReader.Types             as Exports
import           FeedReader.Convert           as Exports
import           Prelude                      hiding (filter, lookup)

runLookup :: (Document a, LogState l, MonadIO m) => Handle l -> Reference a ->
              m (Either TransactionAbort (Maybe a))
runLookup h k = runQuery h (lookup k)

runLookupUnique :: (Document a, ToKey (Unique b), LogState l, MonadIO m) =>
                   Handle l -> Property a -> Unique b ->
                   m (Either TransactionAbort (Maybe (Reference a, a)))
runLookupUnique h p k = runQuery h $ lookupUnique p k

runRange :: (Document a, ToKey (Sortable b), LogState l, MonadIO m) =>
             Handle l -> Int -> Property a -> Maybe (Sortable b) ->
             m (Either TransactionAbort [(Reference a, a)])
runRange h pg prop s = runQuery h $ range pg prop s Nothing

runRangeF :: (Document a, ToKey (Sortable c), LogState l, MonadIO m) =>
              Handle l -> Int -> Property a -> Maybe (Reference b) ->
              Property a -> Maybe (Sortable c) ->
              m (Either TransactionAbort [(Reference a, a)])
runRangeF h pg fprop k sprop s = runQuery h $
  rangeF pg fprop k sprop s Nothing

runInsert :: (Document a, LogState l, MonadIO m) =>
              Handle l -> a -> m (Either TransactionAbort (Reference a))
runInsert h a = runQuery h $ insert a

runUpdate :: (Document a, LogState l, MonadIO m) =>
              Handle l -> Reference a -> a -> m (Either TransactionAbort ())
runUpdate h aid a = runQuery h $ update aid a

runDelete :: (LogState l, MonadIO m) =>
              Handle l -> Reference a -> m (Either TransactionAbort ())
runDelete h did = runQuery h (delete did)

deleteRange :: (Document a, ToKey (Sortable b), MonadIO m) =>
               Int -> Property a -> Sortable b -> Transaction l m Int
deleteRange pg prop s = do
  ks <- rangeK pg prop (Just s) Nothing
  forM_ ks $ \k -> delete k
  return $ length ks

runDeleteRange :: (Document a, ToKey (Sortable b), LogState l, MonadIO m) =>
                   Handle l -> Int -> Property a -> Sortable b ->
                   m (Either TransactionAbort Int)
runDeleteRange h pg prop s = runQuery h (deleteRange pg prop s)

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: (LogState l, MonadIO m) =>
             Handle l -> m (Either TransactionAbort DBStats)
getStats h = runQuery h $ DBStats
  <$> size ("catName"     :: Property Cat)
  <*> size ("feedUpdated" :: Property Feed)
  <*> size ("personName"  :: Property Person)
  <*> size ("itemUpdated" :: Property Item)

runToItem :: (ToItem i, LogState l, MonadIO m) =>
              Handle l -> i -> Reference Feed ->
              m (Either TransactionAbort (Reference Item, Item))
runToItem h it fid =
  liftIO getCurrentTime >>= runQuery h . toItem it fid . DateTime

runToFeed :: (ToFeed f, LogState l, MonadIO m) =>
              Handle l -> f -> Reference Feed -> Feed ->
              m (Either TransactionAbort Feed)
runToFeed h it fid feed =
  liftIO getCurrentTime >>= runQuery h . toFeed it fid feed . DateTime

runUpdateItemStatus :: (LogState l, MonadIO m) => Handle l -> Reference Item ->
                        ItemStatusKey -> m (Either TransactionAbort ())
runUpdateItemStatus h k stk = runQuery h $
  lookup k >>= maybe (return ()) (\it -> do
    st <- itemStatusByKey stk
    update k it { itemStatus = st }
    return ())
