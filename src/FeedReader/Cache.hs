{-# LANGUAGE BangPatterns #-}

-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.DocDB
-- Copyright : (C) 2015 Călin Ardelean,
-- License : BSD-style (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides a LRU cache implementation.
----------------------------------------------------------------------------

module FeedReader.Cache
  ( LRUCache
  , empty
  , insert
  , lookup
  , trim
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Int        (Int64)
import           Data.IntPSQ     (IntPSQ)
import qualified Data.IntPSQ     as PQ
import           Data.Maybe      (fromJust)
import           Data.Time.Clock (UTCTime, NominalDiffTime, diffUTCTime)
import           Prelude         hiding (lookup)

data LRUCache = LRUCache
  { cMinCap :: !Int
  , cMaxCap :: !Int
  , cMaxAge :: !NominalDiffTime
  , cSize   :: !Int
  , cQueue  :: !(IntPSQ UTCTime ByteString)
  } deriving (Eq)

empty :: Int -> Int -> NominalDiffTime -> LRUCache
empty minc maxc age = LRUCache { cMinCap = minc
                               , cMaxCap = maxc
                               , cMaxAge = age
                               , cSize   = 0
                               , cQueue  = PQ.empty
                               }

trim :: UTCTime -> LRUCache -> LRUCache
trim now c =
  if cSize c < cMinCap c then c
  else case PQ.findMin (cQueue c) of
    Nothing        -> c
    Just (k, p, v) -> if (cSize c < cMaxCap c) && (diffUTCTime now p < cMaxAge c)
                      then c
                      else trim now $! c { cSize  = cSize c - BS.length v
                                         , cQueue = PQ.deleteMin (cQueue c)
                                         }

insert :: UTCTime -> Int -> ByteString -> LRUCache -> LRUCache
insert now k v c = trim now $! c { cSize  = cSize c + BS.length v -
                                              if null mbv then 0
                                              else BS.length . snd $ fromJust mbv
                                 , cQueue = q
                                 }
  where (mbv, q) = PQ.insertView k now v (cQueue c)

lookup :: UTCTime -> Int -> LRUCache -> Maybe (ByteString, LRUCache)
lookup now k c =
  case PQ.alter f k (cQueue c) of
    (Nothing, _) -> Nothing
    (Just v,  q) -> Just (v, c')
      where !c' = trim now $ c { cQueue = q }
  where f Nothing       = (Nothing, Nothing)
        f (Just (_, v)) = (Just v,  Just (now, v))
