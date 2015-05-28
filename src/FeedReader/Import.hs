{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

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
  , importOPML
  ) where

import           Control.Exception         (throw, try)
import           Control.Monad             (forM, liftM)
import           Data.ByteString           (ByteString)
import           Data.ByteString.Char8     (unpack)
import           Data.List                 (find)
import           Data.Maybe                (fromMaybe)
import           Data.Time.Clock.POSIX     (posixSecondsToUTCTime)
import           FeedReader.DB
import           Network.HTTP.Types.Status (Status (..), statusIsSuccessful)
import           Pipes
import           Pipes.HTTP
import qualified Pipes.Prelude             as P
import           Prelude                   hiding (filter, lookup)
import qualified Text.Atom.Feed            as A
import           Text.Feed.Import          (readAtom, readRSS1, readRSS2)
import qualified Text.Feed.Types           as F
import           Text.OPML.Import          (elementToOPML)
import           Text.OPML.Syntax          (Outline (..), opmlBody)
import qualified Text.RSS.Syntax           as R
import qualified Text.RSS1.Syntax          as R1
import qualified Text.XML.Light            as XML
import           Text.XML.Light.Input      (parseXMLDoc)

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
    Just e  ->
      readAtom e `mplus`
      readRSS2 e `mplus`
      readRSS1 e `mplus`
      Nothing

writeFeed :: (LogState l, MonadIO m) => Handle l -> F.Feed -> Reference Feed ->
              Feed -> m (Either TransactionAbort (Maybe Feed,
              [(Reference Item, Item)]))
writeFeed h f fid feed =
  case f of
    F.AtomFeed af -> runToFeed h af fid feed
    F.RSSFeed rss -> runToFeed h (R.rssChannel rss) fid feed
    F.RSS1Feed rf -> runToFeed h rf fid feed
  >>=
  either (return . Left) (\feed' -> liftM (liftM (Just feed',) . sequence) $
    case f of
      F.AtomFeed af -> addItems $ A.feedEntries af
      F.RSSFeed rss -> addItems . R.rssItems $ R.rssChannel rss
      F.RSS1Feed rf -> addItems $ R1.feedItems rf)
  where addItems is = P.toListM $ for (each is) $ \i ->
          lift (runToItem h i fid) >>= yield

updateFeed :: (LogState l, MonadIO m) => Handle l -> Reference Feed ->
               m (Either TransactionAbort (Maybe Feed, [(Reference Item, Item)]))
updateFeed h fid =
  runLookup h fid >>=
  either (return . Left) (
    maybe (return $ Right (Nothing, [])) (\feed ->
      downloadFeed (unUnique $ feedURL feed) >>=
      either (\err -> let f' = feed { feedLastError = Just err } in
                      runUpdate h fid f' >>=
                      either (return . Left)
                             (return . const (Right (Just f', []))) )
             (\f   -> writeFeed h f fid feed )
    )
  )

nullDate :: DateTime
nullDate = DateTime $ posixSecondsToUTCTime 0

importOPML :: (LogState l, MonadIO m) => Handle l -> FilePath ->
               m (Either String [(Reference Feed, Feed)])
importOPML h p = do
  str <- liftIO $ readFile p
  case parseXMLDoc str of
    Nothing -> return $ Left "XML parsing error."
    Just el ->
      case elementToOPML el of
        Nothing   -> return $ Left "OPML parsing error."
        Just opml -> do
          res <- runQuery h . opmlToDb Nothing $ opmlBody opml
          return $ either (Left . show) Right res

opmlToDb :: (LogState l, MonadIO m) => Maybe (Reference Cat) -> [Outline] ->
            Transaction l m [(Reference Feed, Feed)]
opmlToDb pcat os = do
  cs <- filter "catName" pcat "catName"
  rss <- forM os $ \o -> case parseOutline o of
    Left str -> if null str then return [] else do
      cid <- case find ((== opmlText o) . unSortable . catName . snd) cs of
        Nothing       -> insert $ Cat (Sortable str) pcat
        Just (cid, _) -> return cid
      opmlToDb (Just cid) $ opmlOutlineChildren o
    Right (ti, xmlUrl, htmlUrl) -> do
      mb <- lookupUnique "feedURL" (Unique xmlUrl)
      case mb of
        Nothing -> do
          let f' = Feed { feedCat          = pcat
                        , feedURL          = Unique xmlUrl
                        , feedHTTPAuth     = Nothing
                        , feedWebURL       = fromMaybe "" htmlUrl
                        , feedTitle        = Sortable $ Text ti
                        , feedDescription  = Text ""
                        , feedLanguage     = ""
                        , feedAuthors      = []
                        , feedContributors = []
                        , feedRights       = Text ""
                        , feedImage        = Nothing
                        , feedUpdated      = Sortable nullDate
                        , feedUnsubscribed = False
                        , feedLastError    = Nothing
                        }
          fid <- insert f'
          return [(fid, f')]
        Just (fid, f) -> return [(fid, f)]

  return $ concat rss
  where parseOutline o =
          let attrs = opmlOutlineAttrs o in
          case find ((== "xmlUrl") . XML.qName . XML.attrKey) attrs of
            Nothing   -> Left $ opmlText o
            Just attr -> Right (opmlText o, XML.attrVal attr, XML.attrVal <$> h)
              where h = find ((== "htmlUrl") . XML.qName . XML.attrKey) attrs
