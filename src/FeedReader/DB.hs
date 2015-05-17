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
  ( module FeedReader.Types
  , module FeedReader.DocDB
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

import           Control.Monad       (forM_)
import           Control.Monad.Trans (MonadIO (liftIO))
import qualified Data.List           as L
import           Data.Maybe          (fromMaybe)
import           Data.Time.Clock     (getCurrentTime)
import           FeedReader.Convert
import           FeedReader.DocDB
import           FeedReader.Types
import           Prelude             hiding (filter, lookup)

runLookup :: (Document a, MonadIO m) => Handle -> DocID a ->
             m (Either TransactionAbort (Maybe (DocID a, a)))
runLookup h k = runTransaction h (lookup k)

runLookupUnique :: (Document a, MonadIO m) => Handle -> Property a -> IntVal b ->
                   m (Either TransactionAbort (Maybe (DocID a, a)))
runLookupUnique h p k = runTransaction h $
  lookupUnique p k >>= maybe (return Nothing) lookup

runRange :: (Document a, MonadIO m) => Handle -> Maybe (IntVal b) ->
            Property a -> Int -> m (Either TransactionAbort [(DocID a, a)])
runRange h s prop pg = runTransaction h $ range s Nothing prop pg

runFilter :: (Document a, MonadIO m) => Handle -> Maybe (DocID b) -> Maybe (IntVal c) ->
             Property a -> Property a -> Int ->
             m (Either TransactionAbort [(DocID a, a)])
runFilter h k s fprop sprop pg = runTransaction h $
  filter k s Nothing fprop sprop pg

runInsert :: (Document a, MonadIO m) => Handle -> a ->
             m (Either TransactionAbort (DocID a))
runInsert h a = runTransaction h $ insert a

runDelete :: MonadIO m => Handle -> DocID a -> m (Either TransactionAbort ())
runDelete h did = runTransaction h (delete did)

deleteRange :: (Document a, MonadIO m) => IntVal b -> Property a -> Int ->
               Transaction m Int
deleteRange did prop pg = do
  ks <- rangeK (Just did) Nothing prop pg
  forM_ ks $ \k -> delete k
  return $ length ks

runDeleteRange :: (Document a, MonadIO m) => Handle -> IntVal b -> Property a ->
                  Int -> m (Either TransactionAbort Int)
runDeleteRange h did prop pg = runTransaction h (deleteRange did prop pg)

data DBStats = DBStats
  { countCats    :: Int
  , countFeeds   :: Int
  , countPersons :: Int
  , countItems   :: Int
  } deriving (Show)

getStats :: MonadIO m => Handle -> m (Either TransactionAbort DBStats)
getStats h = runTransaction h $ DBStats
  <$> size ("catName"     :: Property Cat)
  <*> size ("feedUpdated" :: Property Feed)
  <*> size ("personName"  :: Property Person)
  <*> size ("itemUpdated" :: Property Item)

runToItem :: (MonadIO m, ToItem i) => Handle -> i -> DocID Feed ->
             m (Either TransactionAbort (DocID Item, Item))
runToItem h it fid =
  liftIO getCurrentTime >>= runTransaction h . toItem it fid

runToFeed :: (MonadIO m, ToFeed f) => Handle -> f -> DocID Cat -> URL ->
             m (Either TransactionAbort (DocID Feed, Feed))
runToFeed h it cid u =
  liftIO getCurrentTime >>= runTransaction h . toFeed it cid u
