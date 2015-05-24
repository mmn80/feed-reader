{-# LANGUAGE TupleSections #-}

-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.Import
-- Copyright : (c) 2015 Călin Ardelean
-- License : BSD-style
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides import and conversion functions for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.Import
  ( downloadFeed
  , updateFeed
  ) where

import           Control.Exception         (throw, try)
import           Control.Monad             (liftM)
import           Control.Monad.Trans       (MonadIO (liftIO))
import           Data.ByteString           (ByteString)
import           Data.ByteString.Char8     (unpack)
import           Database.Muesli.Query     (TransactionAbort (..))
import           FeedReader.DB
import           Network.HTTP.Types.Status (Status (..), statusIsSuccessful,
                                            statusMessage)
import           Pipes
import           Pipes.HTTP
import qualified Pipes.Prelude             as P
import qualified Text.Atom.Feed            as A
import           Text.Feed.Import          (readAtom, readRSS1, readRSS2)
import qualified Text.Feed.Types           as F
import qualified Text.RSS.Syntax           as R
import qualified Text.RSS1.Syntax          as R1
import           Text.XML.Light            as XML

downloadFeed :: MonadIO m => URL -> m (Either String F.Feed)
downloadFeed url = do
  res <- liftIO . try $ do
    req <- parseUrl url
    withManager tlsManagerSettings $ \m ->
      withHTTP req m $ \resp ->
        let st = responseStatus resp in
        if statusIsSuccessful st
        then liftM mconcat (P.toListM $ responseBody resp)
        else throw $ StatusCodeException st (responseHeaders resp) mempty
  return $ case res of
    Left  err -> Left $ case err of
      StatusCodeException (Status c m) _ _ ->
        "HTTP Status " ++ show c ++ ": " ++ unpack m
      InvalidUrlException s t ->
        "Invalid URL: " ++ s ++ ". " ++ t ++ ""
      FailedConnectionException2 _ _ _ ex ->
        "Connection Error: " ++ show ex
      ResponseTimeout ->
        "Timeout Error"
      _ -> show err
    Right bs  ->
      case parseFeed bs of
        Nothing -> Left "Feed Parsing Error. Use an RSS/Atom validator for more info."
        Just f  -> Right f

parseFeed :: ByteString -> Maybe F.Feed
parseFeed bs =
  case XML.parseXMLDoc bs of
    Nothing -> Nothing
    Just e ->
      readAtom e `mplus`
      readRSS2 e `mplus`
      readRSS1 e `mplus`
      Nothing

updateFeed :: (LogState l, DataHandle d, MonadIO m) =>
               Handle l d -> F.Feed -> Reference Cat -> URL -> m (Either
               TransactionAbort (Reference Feed, Feed, [(Reference Item, Item)]))
updateFeed h ff c u =
  case ff of
    F.AtomFeed af -> runToFeed h af c u
    F.RSSFeed rss -> runToFeed h (R.rssChannel rss) c u
    F.RSS1Feed rf -> runToFeed h rf c u
  >>=
  either (return . Left) (\(fid, f) -> liftM (liftM (fid, f,) . sequence) $
    case ff of
      F.AtomFeed af -> addItems fid $ A.feedEntries af
      F.RSSFeed rss -> addItems fid . R.rssItems $ R.rssChannel rss
      F.RSS1Feed rf -> addItems fid $ R1.feedItems rf)
  where addItems fid is = P.toListM $ for (each is) $ \i ->
          lift (runToItem h i fid) >>= yield
