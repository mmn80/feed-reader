{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module FeedReader.XML2DB where

import           Data.Foldable    (fold)
import           Data.Maybe       (fromMaybe)
import           Data.Monoid      (First (..), getFirst, (<>))
import           Data.Time.Format (parseTimeM)
import           FeedReader.DB    as DB
import qualified Text.Atom.Feed   as A
import qualified Text.RSS.Syntax  as R
import qualified Text.RSS1.Syntax as R1

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

person2DB p = DB.Person
  { personID    = 0
  , personName  = A.personName p
  , personURL   = fromMaybe "" $ A.personURI p
  , personEmail = e
  }
  where e = fromMaybe "" $ A.personEmail p

instance DB.Feed2DB A.Feed A.Entry where
  feed2DB f c u df = DB.Feed
    { feedID           = 0
    , feedCat          = c
    , feedURL          = u
    , feedTitle        = content2DB $ A.feedTitle f
    , feedDescription  = tryContent2DB $ A.feedSubtitle f
    , feedLanguage     = ""
    , feedAuthors      = person2DB <$> A.feedAuthors f
    , feedContributors = person2DB <$> A.feedContributors f
    , feedRights       = tryContent2DB $ A.feedRights f
    , feedImage        = DB.imageFromURL <$> getFirst
                           (First (A.feedLogo f) <> First (A.feedIcon f))
    , feedUpdated      = DB.text2UTCTime (A.feedUpdated f) df
    }

  item2DB i f u df = DB.Item
    { itemID           = 0
    , itemFeed         = f
    , itemURL          = u
    , itemTitle        = content2DB $ A.entryTitle i
    , itemSummary      = tryContent2DB $ A.entrySummary i
    , itemTags         = A.catTerm <$> A.entryCategories i
    , itemAuthors      = person2DB <$> A.entryAuthors i
    , itemContributors = person2DB <$> A.entryContributor i
    , itemRights       = tryContent2DB $ A.entryRights i
    , itemContent      = tryEContent2DB $ A.entryContent i
    , itemPublished    = DB.text2UTCTime (fromMaybe "" $ A.entryPublished i) df
    , itemUpdated      = date
    }
    where date = DB.text2UTCTime (A.entryUpdated i) df
