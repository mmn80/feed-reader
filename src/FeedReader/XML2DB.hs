{-# LANGUAGE LambdaCase #-}

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

addItem :: ToItem i => AcidState DB.FeedsDB -> i ->
                       DB.FeedID -> DB.URL -> IO DB.Item
addItem acid it fid u = do
  df <- getCurrentTime
  let (i, as, cs) = DB.toItem it fid u df
  as' <- sequence $ addPerson acid <$> as
  cs' <- sequence $ addPerson acid <$> cs
  let i' = i { itemAuthors      = as'
             , itemContributors = cs'
             }
  update acid $ DB.InsertItem i'

addFeed :: ToFeed f => AcidState DB.FeedsDB -> f ->
                       DB.CatID -> DB.URL -> IO DB.Feed
addFeed acid it cid u = do
  df <- getCurrentTime
  let (f, as, cs) = DB.toFeed it cid u df
  as' <- sequence $ addPerson acid <$> as
  cs' <- sequence $ addPerson acid <$> cs
  let f' = f { feedAuthors      = as'
             , feedContributors = cs'
             }
  update acid $ DB.InsertFeed f'

addPerson acid p = do
  p' <- update acid $ DB.InsertPerson p
  return $ DB.personID p'


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

instance DB.ToPerson A.Person where
  toPerson p = DB.Person
    { personID    = DB.unsetPersonID
    , personName  = A.personName p
    , personURL   = fromMaybe "" $ A.personURI p
    , personEmail = fromMaybe "" $ A.personEmail p
    }

instance DB.ToFeed A.Feed where
  toFeed f c u df =
    ( DB.Feed
      { feedID           = DB.unsetFeedID
      , feedCatID        = c
      , feedURL          = u
      , feedTitle        = content2DB $ A.feedTitle f
      , feedDescription  = tryContent2DB $ A.feedSubtitle f
      , feedLanguage     = ""
      , feedAuthors      = []
      , feedContributors = []
      , feedRights       = tryContent2DB $ A.feedRights f
      , feedImage        = DB.imageFromURL <$> getFirst
                             (First (A.feedLogo f) <> First (A.feedIcon f))
      , feedUpdated      = DB.text2UTCTime (A.feedUpdated f) df
      }
    , DB.toPerson <$> A.feedAuthors f
    , DB.toPerson <$> A.feedContributors f
    )


instance DB.ToItem A.Entry where
  toItem i f u df =
    ( DB.Item
      { itemID           = DB.unsetItemID
      , itemFeedID       = f
      , itemURL          = u
      , itemTitle        = content2DB $ A.entryTitle i
      , itemSummary      = tryContent2DB $ A.entrySummary i
      , itemTags         = A.catTerm <$> A.entryCategories i
      , itemAuthors      = []
      , itemContributors = []
      , itemRights       = tryContent2DB $ A.entryRights i
      , itemContent      = tryEContent2DB $ A.entryContent i
      , itemPublished    = DB.text2UTCTime (fromMaybe "" $ A.entryPublished i) df
      , itemUpdated      = date
      }
    , DB.toPerson <$> A.entryAuthors i
    , DB.toPerson <$> A.entryContributor i
    )
    where date = DB.text2UTCTime (A.entryUpdated i) df
