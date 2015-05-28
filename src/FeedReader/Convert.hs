{-# LANGUAGE LambdaCase        #-}
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
-- This module provides DB conversion/import instances for Feed Reader.
----------------------------------------------------------------------------

module FeedReader.Convert
  ( ToFeed (..)
  , ToPerson (..)
  , ToItem (..)
  , itemStatusByKey
  , itemStatusToKey
  ) where

import           Control.Applicative   ((<|>))
import           Control.Monad         (liftM)
import           Control.Monad.Trans   (MonadIO)
import           Data.List             (find, isPrefixOf)
import           Data.Maybe            (fromMaybe, listToMaybe)
import           Database.Muesli.Query hiding (filter)
import           FeedReader.Types
import           FeedReader.Utils      (text2DateTime)
import           Prelude               hiding (lookup)
import qualified Text.Atom.Feed        as A
import qualified Text.DublinCore.Types as DC
import qualified Text.RSS.Syntax       as R
import qualified Text.RSS1.Syntax      as R1

class ToFeed f where
  toFeed :: MonadIO m => f -> Reference Feed -> Feed -> DateTime ->
            Transaction l m Feed

class ToPerson p where
  toPerson :: MonadIO m => p -> Transaction l m (Reference Person, Person)

class ToItem i where
  toItem :: (LogState l, MonadIO m) => i -> Reference Feed -> DateTime ->
            Transaction l m (Reference Item, Item)

itemStatusByKey :: MonadIO m => ItemStatusKey ->
                   Transaction l m (Reference ItemStatus)
itemStatusByKey k =
  lookupUniqueK "statusKey" (Unique k) >>=
  maybe (insert $ ItemStatus (Unique k)) return

itemStatusToKey :: (LogState l, MonadIO m) => Reference ItemStatus ->
                    Transaction l m ItemStatusKey
itemStatusToKey k =
  liftM (maybe StatusNew (unUnique . statusKey)) (lookup k)

------------------------------------------------------------------------------
-- Conversion transactions for Atom
------------------------------------------------------------------------------

content2DB :: A.TextContent -> Content
content2DB = \case
  A.TextString  s -> Text s
  A.HTMLString  s -> HTML s
  A.XHTMLString e -> XHTML $ show e

tryContent2DB :: Maybe A.TextContent -> Content
tryContent2DB c = content2DB $ fromMaybe (A.TextString "") c

eContent2DB :: A.EntryContent -> Content
eContent2DB = \case
  A.TextContent       s -> Text s
  A.HTMLContent       s -> HTML s
  A.XHTMLContent      e -> XHTML $ show e
  A.MixedContent   j cs -> Text $ foldl (flip shows) (fromMaybe "" j) cs
  A.ExternalContent j u -> Text . maybe (showString "")
    (\s -> showString "MediaType: " . showString s . showString "\n") j
    . showString "URL: " $ u

tryEContent2DB :: Maybe A.EntryContent -> Content
tryEContent2DB c = eContent2DB $ fromMaybe (A.TextContent "") c

imageFromURL :: URL -> Image
imageFromURL u = Image
  { imageURL         = u
  , imageTitle       = ""
  , imageDescription = ""
  , imageLink        = u
  , imageWidth       = 0
  , imageHeight      = 0
  }

instance ToPerson A.Person where
  toPerson p = do
    let name = Sortable $ A.personName p
    let p' = Person { personName  = Unique name
                    , personURL   = fromMaybe "" $ A.personURI p
                    , personEmail = fromMaybe "" $ A.personEmail p
                    }
    pid <- updateUnique "personName" (Unique name) p'
    return (pid, p')

toStr :: Maybe (Either String String) -> String
toStr Nothing = ""
toStr (Just (Left x)) = x
toStr (Just (Right x)) = x

isSelf :: A.Link -> Bool
isSelf lr = toStr (A.linkRel lr) == "alternate" && isHTMLType (A.linkType lr)

isHTMLType :: Maybe String -> Bool
isHTMLType (Just str) = "lmth" `isPrefixOf` reverse str
isHTMLType _ = True

instance ToFeed A.Feed where
  toFeed f fid feed df = do
    as <- mapM toPerson $ A.feedAuthors f
    cs <- mapM toPerson $ A.feedContributors f
    let f' = feed { feedWebURL       = maybe "" A.linkHref . listToMaybe .
                                       filter isSelf $ A.feedLinks f
                  , feedTitle        = Sortable . content2DB $ A.feedTitle f
                  , feedDescription  = tryContent2DB $ A.feedSubtitle f
                  , feedLanguage     = ""
                  , feedAuthors      = fst <$> as
                  , feedContributors = fst <$> cs
                  , feedRights       = tryContent2DB $ A.feedRights f
                  , feedImage        = imageFromURL <$> (A.feedLogo f <|> A.feedIcon f)
                  , feedUpdated      = Sortable $ text2DateTime (A.feedUpdated f) df
                  , feedLastError    = Nothing
                  }
    update fid f'
    return f'

updateItem :: (LogState l, MonadIO m) => URL -> Item -> Reference ItemStatus ->
               Reference ItemStatus -> Transaction l m (Reference Item, Item)
updateItem url it stNew stUnr =
  lookupUnique "itemURL" (Unique url) >>=
  maybe (liftM (,it) (insert it)) (\(k, oit) ->
    let it' = it { itemStatus = stUpd $ itemStatus oit
                 , itemTags   = itemTags oit } in
    update k it' >> return (k, it'))
  where stUpd st = if st == stNew then stUnr else st

instance ToItem A.Entry where
  toItem i f df = do
    as <- mapM toPerson $ A.entryAuthors i
    cs <- mapM toPerson $ A.entryContributor i
    let date = text2DateTime (A.entryUpdated i) df
    let url = case A.entryLinks i of
                []    -> ""
                (l:_) -> A.linkHref l
    stNew <- itemStatusByKey StatusNew
    stUnr <- itemStatusByKey StatusUnread
    let it = Item { itemFeed         = f
                  , itemURL          = Unique url
                  , itemTitle        = content2DB $ A.entryTitle i
                  , itemSummary      = tryContent2DB $ A.entrySummary i
                  , itemTags         = Sortable . A.catTerm <$> A.entryCategories i
                  , itemAuthors      = fst <$> as
                  , itemContributors = fst <$> cs
                  , itemRights       = tryContent2DB $ A.entryRights i
                  , itemContent      = tryEContent2DB $ A.entryContent i
                  , itemPublished    = Sortable $ text2DateTime
                                         (fromMaybe "" $ A.entryPublished i) df
                  , itemUpdated      = Sortable date
                  , itemStatus       = stNew
                  }
    updateItem url it stNew stUnr

------------------------------------------------------------------------------
-- Conversion transactions for RSS
------------------------------------------------------------------------------

data RSSPerson = RSSPerson { rssPersonName :: String }

instance ToPerson RSSPerson where
  toPerson p = do
    let name = Sortable $ rssPersonName p
    let p' = Person { personName  = Unique name
                    , personURL   = ""
                    , personEmail = ""
                    }
    pid <- updateUnique "personName" (Unique name) p'
    return (pid, p')

rssImageToImage :: R.RSSImage -> Image
rssImageToImage i = Image
  { imageURL         = R.rssImageURL i
  , imageTitle       = R.rssImageTitle i
  , imageDescription = fromMaybe "" $ R.rssImageDesc i
  , imageLink        = R.rssImageLink i
  , imageWidth       = fromIntegral . fromMaybe 0 $ R.rssImageWidth i
  , imageHeight      = fromIntegral . fromMaybe 0 $ R.rssImageHeight i
  }

rssPersonToPerson :: Maybe String -> [RSSPerson]
rssPersonToPerson = maybe [] (pure . RSSPerson)

instance ToFeed R.RSSChannel where
  toFeed f fid feed df = do
    as <- mapM toPerson . rssPersonToPerson $ R.rssEditor f
    cs <- mapM toPerson . rssPersonToPerson $ R.rssWebMaster f
    let f' = feed { feedWebURL       = R.rssLink f
                  , feedTitle        = Sortable . Text $ R.rssTitle f
                  , feedDescription  = HTML $ R.rssDescription f
                  , feedLanguage     = fromMaybe "" $ R.rssLanguage f
                  , feedAuthors      = fst <$> as
                  , feedContributors = fst <$> cs
                  , feedRights       = Text . fromMaybe "" $ R.rssCopyright f
                  , feedImage        = rssImageToImage <$> R.rssImage f
                  , feedUpdated      = Sortable $ case R.rssLastUpdate f of
                                         Nothing -> df
                                         Just d  -> text2DateTime d df
                  , feedLastError    = Nothing
                  }
    update fid f'
    return f'

instance ToItem R.RSSItem where
  toItem i f df = do
    as <- mapM toPerson . rssPersonToPerson $ R.rssItemAuthor i
    let url = fromMaybe "" $ R.rssItemLink i
    let date = Sortable $ text2DateTime (fromMaybe "" $ R.rssItemPubDate i) df
    stNew <- itemStatusByKey StatusNew
    stUnr <- itemStatusByKey StatusUnread
    let it = Item { itemFeed         = f
                  , itemURL          = Unique url
                  , itemTitle        = Text . fromMaybe "" $ R.rssItemTitle i
                  , itemSummary      = Text ""
                  , itemTags         = Sortable . R.rssCategoryValue <$>
                                       R.rssItemCategories i
                  , itemAuthors      = fst <$> as
                  , itemContributors = []
                  , itemRights       = Text ""
                  , itemContent      = HTML . fromMaybe "" $ R.rssItemDescription i
                  , itemPublished    = date
                  , itemUpdated      = date
                  , itemStatus       = stNew
                  }
    updateItem url it stNew stUnr

------------------------------------------------------------------------------
-- Conversion transactions for RSS1
------------------------------------------------------------------------------

extractDcPersons :: [DC.DCItem] -> DC.DCInfo -> [RSSPerson]
extractDcPersons dcs con = map (RSSPerson . DC.dcText) $
                           filter (\d -> DC.dcElt d == con) dcs

extractDcInfo :: [DC.DCItem] -> DC.DCInfo -> String
extractDcInfo dcs con = maybe "" DC.dcText $ find (\d -> DC.dcElt d == con) dcs

rss1ImageToImage :: R1.Image -> Image
rss1ImageToImage i = Image
  { imageURL         = R1.imageURI i
  , imageTitle       = R1.imageTitle i
  , imageDescription = ""
  , imageLink        = R1.imageLink i
  , imageWidth       = 0
  , imageHeight      = 0
  }

instance ToFeed R1.Feed where
  toFeed f fid feed df = do
    let ch  = R1.feedChannel f
    let dcs = R1.channelDC ch
    as <- mapM toPerson $ extractDcPersons dcs DC.DC_Creator
    cs <- mapM toPerson $ extractDcPersons dcs DC.DC_Contributor
    let f' = feed { feedWebURL       = R1.channelURI ch
                  , feedTitle        = Sortable . Text $ R1.channelTitle ch
                  , feedDescription  = HTML $ R1.channelDesc ch
                  , feedLanguage     = extractDcInfo dcs DC.DC_Language
                  , feedAuthors      = fst <$> as
                  , feedContributors = fst <$> cs
                  , feedRights       = Text $ extractDcInfo dcs DC.DC_Rights
                  , feedImage        = rss1ImageToImage <$> R1.feedImage f
                  , feedUpdated      = Sortable $ text2DateTime
                                         (extractDcInfo dcs DC.DC_Date) df
                  , feedLastError    = Nothing
                  }
    update fid f'
    return f'

instance ToItem R1.Item where
  toItem i f df = do
    let dcs = R1.itemDC i
    as <- mapM toPerson $ extractDcPersons dcs DC.DC_Creator
    cs <- mapM toPerson $ extractDcPersons dcs DC.DC_Contributor
    let url = if null $ R1.itemLink i then R1.itemURI i else R1.itemLink i
    let date = Sortable $ text2DateTime (extractDcInfo dcs DC.DC_Date) df
    stNew <- itemStatusByKey StatusNew
    stUnr <- itemStatusByKey StatusUnread
    let it = Item { itemFeed         = f
                  , itemURL          = Unique url
                  , itemTitle        = Text $ R1.itemTitle i
                  , itemSummary      = HTML . fromMaybe "" $ R1.itemDesc i
                  , itemTags         = Sortable <$> R1.itemTopics i
                  , itemAuthors      = fst <$> as
                  , itemContributors = fst <$> cs
                  , itemRights       = Text $ extractDcInfo dcs DC.DC_Rights
                  , itemContent      = HTML . concat . concatMap (foldMap pure .
                                       R1.contentValue) $ R1.itemContent i
                  , itemPublished    = date
                  , itemUpdated      = date
                  , itemStatus       = stNew
                  }
    updateItem url it stNew stUnr
