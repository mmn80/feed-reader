{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module FeedReader.XML2DB
  (
    addItem
  , addFeed
  ) where

import           Control.Monad    (sequence)
import           Data.Acid
import           Data.Foldable    (fold)
import           Data.Maybe       (fromMaybe)
import           Data.Monoid      (First (..), getFirst, (<>))
import           Data.Time.Clock  (getCurrentTime)
import           Data.Time.Format (parseTimeM)
import           FeedReader.DB    as DB
import qualified Text.Atom.Feed   as A
import qualified Text.RSS.Syntax  as R
import qualified Text.RSS1.Syntax as R1

addItem :: DB.Feed2DB f p i => AcidState DB.FeedsDB -> i ->
                               DB.FeedID -> DB.URL -> IO DB.Item
addItem acid it fid u = do
  df <- getCurrentTime
  i  <- item2DB it fid u df $ \p -> do
    p' <- update acid $ DB.InsertPerson p
    return $ personID p'
  update acid $ DB.InsertItem i

addFeed :: DB.Feed2DB f p i => AcidState DB.FeedsDB -> f ->
                               DB.CatID -> DB.URL -> IO DB.Feed
addFeed acid ft cid u = do
  df <- getCurrentTime
  f  <- feed2DB ft cid u df $ \p -> do
    p' <- update acid $ DB.InsertPerson p
    return $ personID p'
  update acid $ DB.InsertFeed f


------------------------------------------------------------------------------
-- Feed2DB instance for Atom
------------------------------------------------------------------------------

content2DB = \case
  A.TextString  s -> DB.Text s
  A.HTMLString  s -> DB.HTML s
  A.XHTMLString e -> DB.XHTML $ show e

tryContent2DB c = content2DB $ fromMaybe (A.TextString "") c

eContent2DB = \case
  A.TextContent       s -> DB.Text s
  A.HTMLContent       s -> DB.HTML s
  A.XHTMLContent      e -> DB.XHTML $ show e
  A.MixedContent   j cs -> DB.Text $ fromMaybe "" j ++ foldMap show cs
  A.ExternalContent j u -> DB.Text $ fold $ fromMaybe [""] $
    sequence [Just "MediaType: ", j, Just "\n"] <> Just ["URL: ", u]

tryEContent2DB c = eContent2DB $ fromMaybe (A.TextContent "") c

instance DB.Feed2DB A.Feed A.Person A.Entry where
  person2DB p = DB.Person
    { personID    = DB.unsetPersonID
    , personName  = A.personName p
    , personURL   = fromMaybe "" $ A.personURI p
    , personEmail = fromMaybe "" $ A.personEmail p
    }

  feed2DB f c u df addPerson = do
    as <- sequence $ addPerson . person2DB <$> A.feedAuthors f
    cs <- sequence $ addPerson . person2DB <$> A.feedContributors f
    return DB.Feed
      { feedID           = DB.unsetFeedID
      , feedCatID        = c
      , feedURL          = u
      , feedTitle        = content2DB $ A.feedTitle f
      , feedDescription  = tryContent2DB $ A.feedSubtitle f
      , feedLanguage     = ""
      , feedAuthors      = as
      , feedContributors = cs
      , feedRights       = tryContent2DB $ A.feedRights f
      , feedImage        = DB.imageFromURL <$> getFirst
                             (First (A.feedLogo f) <> First (A.feedIcon f))
      , feedUpdated      = DB.text2UTCTime (A.feedUpdated f) df
      }

  item2DB i f u df addPerson = do
    as <- sequence $ addPerson . person2DB <$> A.entryAuthors i
    cs <- sequence $ addPerson . person2DB <$> A.entryContributor i
    return DB.Item
      { itemID           = DB.unsetItemID
      , itemFeedID       = f
      , itemURL          = u
      , itemTitle        = content2DB $ A.entryTitle i
      , itemSummary      = tryContent2DB $ A.entrySummary i
      , itemTags         = A.catTerm <$> A.entryCategories i
      , itemAuthors      = as
      , itemContributors = cs
      , itemRights       = tryContent2DB $ A.entryRights i
      , itemContent      = tryEContent2DB $ A.entryContent i
      , itemPublished    = DB.text2UTCTime (fromMaybe "" $ A.entryPublished i) df
      , itemUpdated      = date
      }
      where date = DB.text2UTCTime (A.entryUpdated i) df
