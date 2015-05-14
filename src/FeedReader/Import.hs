-----------------------------------------------------------------------------
-- |
-- Module : FeedReader.Import
-- Copyright : (C) 2015 Călin Ardelean,
-- License : BSD-style (see the file LICENSE)
--
-- Maintainer : Călin Ardelean <calinucs@gmail.com>
-- Stability : experimental
-- Portability : portable
--
-- This module provides import and conversion functions for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.Import
  (downloadFeed) where

import           Control.Exception         (throw, try)
import           Control.Monad.Trans       (MonadIO (liftIO))
import           Data.ByteString           (ByteString)
import           Data.ByteString.Char8     (unpack)
import           FeedReader.DB
import           Network.HTTP.Types.Status (Status (..), statusIsSuccessful,
                                            statusMessage)
import           Pipes
import           Pipes.HTTP
import           Pipes.Prelude             (toListM)
import           Text.Feed.Import          (readAtom, readRSS1, readRSS2)
import qualified Text.Feed.Types           as F
import           Text.XML.Light            as XML

downloadFeed :: MonadIO m => URL -> m (Either String F.Feed)
downloadFeed url = do
  res <- liftIO . try $ do
    req <- parseUrl url
    withManager tlsManagerSettings $ \m ->
      withHTTP req m $ \resp ->
        let st = responseStatus resp in
        if statusIsSuccessful st then do
          bs <- toListM $ responseBody resp
          return $ mconcat bs
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
