-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.Data.DB
-- Copyright : (c) 2015-18 Călin Ardelean,
-- License : BSD-style (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <mmn80cpu@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides util functions for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.Utils
  ( text2DateTime
  , diffMs
  ) where

import           Control.Applicative   ((<|>))
import           Data.Maybe            (fromMaybe)
import           Data.Time.Clock       (UTCTime, diffUTCTime)
import           Data.Time.Format      (defaultTimeLocale, iso8601DateFormat,
                                        parseTimeM, rfc822DateFormat)
import qualified Data.Text             as T
import           Database.Muesli.Types (DateTime)

text2DateTime :: T.Text -> DateTime -> DateTime
text2DateTime t df = fromMaybe df $ iso <|> iso' <|> rfc
  where
    iso  = tryParse . iso8601DateFormat $ Just "%H:%M:%S"
    iso' = tryParse $ iso8601DateFormat Nothing
    rfc  = tryParse rfc822DateFormat
    tryParse f = parseTimeM True defaultTimeLocale f $ T.unpack t

diffMs :: UTCTime -> UTCTime -> Float
diffMs t0 t1 = 1000 * fromRational (toRational $ diffUTCTime t1 t0)
