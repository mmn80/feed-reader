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
    Just e ->
      readAtom e `mplus`
      readRSS2 e `mplus`
      readRSS1 e `mplus`
      Nothing

updateFeed :: (LogState l, MonadIO m) =>
               Handle l -> F.Feed -> Maybe (Reference Cat) -> URL -> m (Either
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

nullDate :: DateTime
nullDate = DateTime $ posixSecondsToUTCTime 0

importOPML :: (LogState l, MonadIO m) => Handle l -> FilePath ->
               m (Either String [(Reference Feed, Feed)])
importOPML h path = do
  str <- liftIO $ readFile path
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
-- TODO: filter -> (filter, filterRange)
  cs <- filter pcat (Nothing :: Maybe (Sortable Int)) Nothing "catName" "catName" 10000
  rss <- forM os $ \o -> case parseOutline o of
    Left str -> if null str then return [] else do
      cid <- case find ((== opmlText o) . unSortable . catName . snd) cs of
        Nothing       -> insert $ Cat (Sortable str) pcat
        Just (cid, _) -> return cid
      opmlToDb (Just cid) $ opmlOutlineChildren o
    Right (ti, xmlUrl, htmlUrl) -> do
      mbfid <- lookupUnique "feedURL" (Unique xmlUrl)
      case mbfid of
        Nothing -> do
          let f' = Feed { feedCat          = pcat
                        , feedURL          = Unique xmlUrl
                        , feedWebURL       = fromMaybe "" htmlUrl
                        , feedTitle        = Sortable $ Text ti
                        , feedDescription  = Text ""
                        , feedLanguage     = ""
                        , feedAuthors      = []
                        , feedContributors = []
                        , feedRights       = Text ""
                        , feedImage        = Nothing
                        , feedUpdated      = Sortable nullDate
                        }
          fid <- insert f'
          return [(fid, f')]
        Just fid -> maybe [] pure <$> lookup fid

  return $ concat rss
  where parseOutline o =
          let attrs = opmlOutlineAttrs o in
          case find ((== "xmlUrl") . XML.qName . XML.attrKey) attrs of
            Nothing   -> Left $ opmlText o
            Just attr -> Right (opmlText o, XML.attrVal attr, XML.attrVal <$> h)
              where h = find ((== "htmlUrl") . XML.qName . XML.attrKey) attrs
