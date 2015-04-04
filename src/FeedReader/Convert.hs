{-# LANGUAGE LambdaCase #-}

module FeedReader.Convert
  () where

import           Control.Applicative ((<|>))
import           Control.Monad       (sequence)
import           Data.Foldable       (fold)
import           Data.Maybe          (fromJust, fromMaybe)
import           Data.Monoid         ((<>))
import           FeedReader.Types
import qualified Text.Atom.Feed      as A
import qualified Text.RSS.Syntax     as R
import qualified Text.RSS1.Syntax    as R1

------------------------------------------------------------------------------
-- Feed2DB instance for Atom
------------------------------------------------------------------------------

content2DB = \case
  A.TextString  s -> Text s
  A.HTMLString  s -> HTML s
  A.XHTMLString e -> XHTML $ show e

tryContent2DB c = content2DB $ fromMaybe (A.TextString "") c

eContent2DB = \case
  A.TextContent       s -> Text s
  A.HTMLContent       s -> HTML s
  A.XHTMLContent      e -> XHTML $ show e
  A.MixedContent   j cs -> Text $ fromMaybe "" j ++ foldMap show cs
  A.ExternalContent j u -> Text $ (if null j then ""
                                      else "MediaType: " ++ fromJust j ++ "\n")
                                      ++ "URL: " ++ u

tryEContent2DB c = eContent2DB $ fromMaybe (A.TextContent "") c

instance ToPerson A.Person where
  toPerson p = Person
    { personID    = unsetPersonID
    , personName  = A.personName p
    , personURL   = fromMaybe "" $ A.personURI p
    , personEmail = fromMaybe "" $ A.personEmail p
    }

instance ToFeed A.Feed where
  toFeed f c u df =
    ( Feed
      { feedID           = unsetFeedID
      , feedCatID        = c
      , feedURL          = u
      , feedTitle        = content2DB $ A.feedTitle f
      , feedDescription  = tryContent2DB $ A.feedSubtitle f
      , feedLanguage     = ""
      , feedAuthors      = []
      , feedContributors = []
      , feedRights       = tryContent2DB $ A.feedRights f
      , feedImage        = imageFromURL <$> (A.feedLogo f <|> A.feedIcon f)
      , feedUpdated      = text2UTCTime (A.feedUpdated f) df
      }
    , toPerson <$> A.feedAuthors f
    , toPerson <$> A.feedContributors f
    )


instance ToItem A.Entry where
  toItem i f u df =
    ( Item
      { itemID           = unsetItemID
      , itemFeedID       = f
      , itemURL          = u
      , itemTitle        = content2DB $ A.entryTitle i
      , itemSummary      = tryContent2DB $ A.entrySummary i
      , itemTags         = A.catTerm <$> A.entryCategories i
      , itemAuthors      = []
      , itemContributors = []
      , itemRights       = tryContent2DB $ A.entryRights i
      , itemContent      = tryEContent2DB $ A.entryContent i
      , itemPublished    = text2UTCTime (fromMaybe "" $ A.entryPublished i) df
      , itemUpdated      = date
      }
    , toPerson <$> A.entryAuthors i
    , toPerson <$> A.entryContributor i
    )
    where date = text2UTCTime (A.entryUpdated i) df
