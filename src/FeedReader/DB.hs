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

import           Control.Monad                (forM_)
import           Control.Monad.Trans          (MonadIO (liftIO))
import qualified Data.List                    as L
import           Data.Maybe                   (fromMaybe)
import           Data.Time.Clock              (getCurrentTime)
import           Database.Muesli.Backend.File as Exports
import           Database.Muesli.Handle       as Exports
import           Database.Muesli.Query        as Exports
import           FeedReader.Convert
import           FeedReader.Types             as Exports
import           Prelude                      hiding (filter, lookup)

runLookup :: (Document a, LogState l, DataHandle d, MonadIO m) => Handle l d ->
              Reference a -> m (Either TransactionAbort (Maybe (Reference a, a)))
runLookup h k = runQuery h (lookup k)

runLookupUnique :: (Document a, ToKey (Unique b), LogState l, DataHandle d, MonadIO m) =>
                   Handle l d -> Property a -> Unique b ->
                   m (Either TransactionAbort (Maybe (Reference a, a)))
runLookupUnique h p k = runQuery h $
  lookupUnique p k >>= maybe (return Nothing) lookup

runRange :: (Document a, ToKey (Sortable b), LogState l, DataHandle d, MonadIO m) =>
             Handle l d -> Maybe (Sortable b) -> Property a -> Int ->
             m (Either TransactionAbort [(Reference a, a)])
runRange h s prop pg = runQuery h $ range s Nothing prop pg

runFilter :: (Document a, ToKey (Sortable c), LogState l, DataHandle d, MonadIO m) =>
              Handle l d -> Maybe (Reference b) -> Maybe (Sortable c) -> Property a ->
              Property a -> Int -> m (Either TransactionAbort [(Reference a, a)])
runFilter h k s fprop sprop pg = runQuery h $
  filter k s Nothing fprop sprop pg

runInsert :: (Document a, LogState l, DataHandle d, MonadIO m) =>
              Handle l d -> a -> m (Either TransactionAbort (Reference a))
runInsert h a = runQuery h $ insert a

runDelete :: (LogState l, DataHandle d, MonadIO m) =>
              Handle l d -> Reference a -> m (Either TransactionAbort ())
runDelete h did = runQuery h (delete did)

deleteRange :: (Document a, ToKey (Sortable b), MonadIO m) =>
               Sortable b -> Property a -> Int -> Transaction l d m Int
deleteRange did prop pg = do
  ks <- rangeK (Just did) Nothing prop pg
  forM_ ks $ \k -> delete k
  return $ length ks

runDeleteRange :: (Document a, ToKey (Sortable b), LogState l, DataHandle d,
                   MonadIO m) => Handle l d -> Sortable b -> Property a -> Int ->
                   m (Either TransactionAbort Int)
runDeleteRange h did prop pg = runQuery h (deleteRange did prop pg)

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: (LogState l, DataHandle d, MonadIO m) =>
             Handle l d -> m (Either TransactionAbort DBStats)
getStats h = runQuery h $ DBStats
  <$> size ("catName"     :: Property Cat)
  <*> size ("feedUpdated" :: Property Feed)
  <*> size ("personName"  :: Property Person)
  <*> size ("itemUpdated" :: Property Item)

runToItem :: (ToItem i, LogState l, DataHandle d, MonadIO m) =>
              Handle l d -> i -> Reference Feed ->
              m (Either TransactionAbort (Reference Item, Item))
runToItem h it fid =
  liftIO getCurrentTime >>= runQuery h . toItem it fid

runToFeed :: (ToFeed f, LogState l, DataHandle d, MonadIO m) =>
              Handle l d -> f -> Reference Cat -> URL ->
              m (Either TransactionAbort (Reference Feed, Feed))
runToFeed h it cid u =
  liftIO getCurrentTime >>= runQuery h . toFeed it cid u
