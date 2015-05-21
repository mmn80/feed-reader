{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

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
-- This module provides DB access functions for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.DB
  ( module Exports
  , TransactionAbort (..)
  , runRange
  , runFilter
  , runLookup
  , runLookupUnique
  , runInsert
  , runDelete
  , runDeleteRange
  , getStats
  , DBStats (..)
  , runToItem
  , runToFeed
  ) where

import           Control.Monad          (forM_)
import           Control.Monad.Trans    (MonadIO (liftIO))
import qualified Data.List              as L
import           Data.Maybe             (fromMaybe)
import           Data.Time.Clock        (getCurrentTime)
import           Database.Muesli.Handle as Exports
import           Database.Muesli.Query
import           FeedReader.Convert
import           FeedReader.Types       as Exports
import           Prelude                hiding (filter, lookup)

runLookup :: (Document a, MonadIO m) => Handle -> Reference a ->
             m (Either TransactionAbort (Maybe (Reference a, a)))
runLookup h k = runQuery h (lookup k)

runLookupUnique :: (Document a, ToDBWord (Unique b), MonadIO m) =>
                   Handle -> Property a -> Unique b ->
                   m (Either TransactionAbort (Maybe (Reference a, a)))
runLookupUnique h p k = runQuery h $
  lookupUnique p k >>= maybe (return Nothing) lookup

runRange :: (Document a, ToDBWord (Sortable b), MonadIO m) => Handle ->
            Maybe (Sortable b) -> Property a -> Int ->
            m (Either TransactionAbort [(Reference a, a)])
runRange h s prop pg = runQuery h $ range s Nothing prop pg

runFilter :: (Document a, ToDBWord (Sortable c), MonadIO m) => Handle ->
             Maybe (Reference b) -> Maybe (Sortable c) -> Property a ->
             Property a -> Int -> m (Either TransactionAbort [(Reference a, a)])
runFilter h k s fprop sprop pg = runQuery h $
  filter k s Nothing fprop sprop pg

runInsert :: (Document a, MonadIO m) => Handle -> a ->
             m (Either TransactionAbort (Reference a))
runInsert h a = runQuery h $ insert a

runDelete :: MonadIO m => Handle -> Reference a -> m (Either TransactionAbort ())
runDelete h did = runQuery h (delete did)

deleteRange :: (Document a, ToDBWord (Sortable b), MonadIO m) =>
               Sortable b -> Property a -> Int -> Transaction m Int
deleteRange did prop pg = do
  ks <- rangeK (Just did) Nothing prop pg
  forM_ ks $ \k -> delete k
  return $ length ks

runDeleteRange :: (Document a, ToDBWord (Sortable b), MonadIO m) =>
                  Handle -> Sortable b -> Property a -> Int ->
                  m (Either TransactionAbort Int)
runDeleteRange h did prop pg = runQuery h (deleteRange did prop pg)

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: MonadIO m => Handle -> m (Either TransactionAbort DBStats)
getStats h = runQuery h $ DBStats
  <$> size ("catName"     :: Property Cat)
  <*> size ("feedUpdated" :: Property Feed)
  <*> size ("personName"  :: Property Person)
  <*> size ("itemUpdated" :: Property Item)

runToItem :: (MonadIO m, ToItem i) => Handle -> i -> Reference Feed ->
             m (Either TransactionAbort (Reference Item, Item))
runToItem h it fid =
  liftIO getCurrentTime >>= runQuery h . toItem it fid

runToFeed :: (MonadIO m, ToFeed f) => Handle -> f -> Reference Cat -> URL ->
             m (Either TransactionAbort (Reference Feed, Feed))
runToFeed h it cid u =
  liftIO getCurrentTime >>= runQuery h . toFeed it cid u
