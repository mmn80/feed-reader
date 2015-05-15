{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module FeedReader.Convert
  () where

import           Control.Applicative ((<|>))
import           Data.Maybe          (fromJust, fromMaybe)
import           FeedReader.DocDB    (Transaction, intValUnique, updateUnique)
import           FeedReader.Types
import qualified Text.Atom.Feed      as A
import qualified Text.RSS.Syntax     as R
import qualified Text.RSS1.Syntax    as R1

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
  A.ExternalContent j u -> Text .
    (if null j then showString ""
     else showString "MediaType: " . showString (fromJust j) . showString "\n")
    . showString "URL: " $ u

tryEContent2DB :: Maybe A.EntryContent -> Content
tryEContent2DB c = eContent2DB $ fromMaybe (A.TextContent "") c

instance ToPerson A.Person where
  toPerson p = do
    let name = Indexable $ A.personName p
    let p' = Person { personName  = Unique name
                    , personURL   = fromMaybe "" $ A.personURI p
                    , personEmail = fromMaybe "" $ A.personEmail p
                    }
    pid <- updateUnique "personName" (intValUnique name) p'
    return (pid, p')

instance ToFeed A.Feed where
  toFeed f c u df = do
    as <- mapM toPerson $ A.feedAuthors f
    cs <- mapM toPerson $ A.feedContributors f
    let f' = Feed { feedCatID        = c
                  , feedURL          = Unique u
                  , feedTitle        = Indexable . content2DB $ A.feedTitle f
                  , feedDescription  = tryContent2DB $ A.feedSubtitle f
                  , feedLanguage     = ""
                  , feedAuthors      = fst <$> as
                  , feedContributors = fst <$> cs
                  , feedRights       = tryContent2DB $ A.feedRights f
                  , feedImage        = imageFromURL <$> (A.feedLogo f <|> A.feedIcon f)
                  , feedUpdated      = Indexable $ text2UTCTime (A.feedUpdated f) df
                  }
    fid <- updateUnique "feedURL" (intValUnique u) f'
    return (fid, f')

instance ToItem A.Entry where
  toItem i f u df = do
    as <- mapM toPerson $ A.entryAuthors i
    cs <- mapM toPerson $ A.entryContributor i
    let date = text2UTCTime (A.entryUpdated i) df
    let i' = Item { itemFeedID       = f
                  , itemURL          = Unique u
                  , itemTitle        = content2DB $ A.entryTitle i
                  , itemSummary      = tryContent2DB $ A.entrySummary i
                  , itemTags         = A.catTerm <$> A.entryCategories i
                  , itemAuthors      = fst <$> as
                  , itemContributors = fst <$> cs
                  , itemRights       = tryContent2DB $ A.entryRights i
                  , itemContent      = tryEContent2DB $ A.entryContent i
                  , itemPublished    = Indexable $ text2UTCTime
                                         (fromMaybe "" $ A.entryPublished i) df
                  , itemUpdated      = Indexable date
                  }
    iid <- updateUnique "itemURL" (intValUnique u) i'
    return (iid, i')
