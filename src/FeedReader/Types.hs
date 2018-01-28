{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

-----------------------------------------------------------------------------
-- |
-- Module      : Main
-- Copyright   : (c) 2015-18 Călin Ardelean
-- License     : BSD-style
--
-- Maintainer  : Călin Ardelean <mmn80cpu@gmail.com>
-- Stability   : experimental
-- Portability : portable
--
-- Core muesli-indexed types.
----------------------------------------------------------------------------

module FeedReader.Types
  ( Cat (..)
  , Feed (..)
  , Person (..)
  , Item (..)
  , ItemStatus (..)
  , ItemStatusKey (..)
  , Text (..)
  , pack
  , unpack
  , URL
  , Language
  , Tag
  , Content (..)
  , HTTPAuth (..)
  , Image (..)
  , Sortable (..)
  , Unique (..)
  ) where

import           Control.Arrow         (first)
import           Data.Hashable         (Hashable (..))
import           Data.Serialize        (Serialize (..))
import           Data.String           (IsString (..))
import qualified Data.Text             as T
import           Data.Text.Encoding    (decodeUtf8, encodeUtf8)
import           Database.Muesli.Types
import           GHC.Generics          (Generic)

newtype Text = Text { unText :: T.Text }
  deriving (Eq, Ord, Generic, Hashable)

instance Read Text where
  readsPrec p = map (first Text) . readsPrec p

instance Show Text where
  showsPrec p = showsPrec p . unText

instance IsString Text where
  fromString = Text . fromString

instance Serialize Text where
  put t = put . encodeUtf8 $ unText t
  get   = Text . decodeUtf8 <$> get

instance Indexable Text

pack :: String -> Text
pack = Text . T.pack

unpack :: Text -> String
unpack = T.unpack . unText

type URL      = Text
type Language = Text
type Tag      = Text

data Content  = Plain Text | HTML Text | XHTML Text
  deriving (Show, Generic, Serialize, Indexable)

data Cat = Cat
  { catName   :: Sortable Text
  , catParent :: Maybe (Reference Cat)
  } deriving (Show, Generic, Serialize)

instance Document Cat

data Person = Person
  { personName  :: Unique (Sortable Text)
  , personURL   :: URL
  , personEmail :: Text
  } deriving (Show, Generic, Serialize)

instance Document Person

data Image = Image
  { imageURL         :: URL
  , imageTitle       :: Text
  , imageDescription :: Text
  , imageLink        :: URL
  , imageWidth       :: Int
  , imageHeight      :: Int
  } deriving (Show, Generic, Serialize, Indexable)

data HTTPAuth = HTTPAuth
  { authUserName :: Text
  , authPassword :: Text
  } deriving (Show, Generic, Serialize, Indexable)

data Feed = Feed
  { feedCat          :: Maybe (Reference Cat)
  , feedURL          :: Unique URL
  , feedHTTPAuth     :: Maybe HTTPAuth
  , feedWebURL       :: URL
  , feedTitle        :: Sortable Content
  , feedDescription  :: Content
  , feedLanguage     :: Language
  , feedAuthors      :: [Reference Person]
  , feedContributors :: [Reference Person]
  , feedRights       :: Content
  , feedImage        :: Maybe Image
  , feedUpdated      :: Sortable DateTime
  , feedUnsubscribed :: Bool
  , feedLastError    :: Maybe Text
  } deriving (Show, Generic, Serialize)

-- TODO: Sortable -> Sorted, Reference -> Ref
-- TODO: filter on Sorted columns
-- TODO: File backend: process file locking
-- TODO: optimize getDocument (hash id => multiple caches)
-- TODO: index on disk
-- TODO: cursor (zipper) in & out of transactions
-- TODO: lenses/prisms/folds/traversals
-- TODO: free/cofree pairing interpreter
-- TODO: query serialization
-- TODO: Alternative instance for Transaction
-- TODO: retry and orElse like in STM
-- TODO: tests, benchmarks
-- TODO: tutorial module (move example from README there)
-- TODO: travis button in README
-- TODO: migration

instance Document Feed

data ItemStatusKey = StatusNew | StatusUnread | StatusRead | StatusStarred
  deriving (Show, Generic, Serialize, Indexable, Hashable)

data ItemStatus = ItemStatus
  { statusKey :: Unique ItemStatusKey
  } deriving (Show, Generic, Serialize)

instance Document ItemStatus

data Item = Item
  { itemFeed         :: Reference Feed
  , itemURL          :: Unique URL
  , itemTitle        :: Content
  , itemSummary      :: Content
  , itemTags         :: [Sortable Tag]
  , itemAuthors      :: [Reference Person]
  , itemContributors :: [Reference Person]
  , itemRights       :: Content
  , itemContent      :: Content
  , itemPublished    :: Sortable DateTime
  , itemUpdated      :: Sortable DateTime
  , itemStatus       :: Reference ItemStatus
  } deriving (Show, Generic, Serialize)

instance Document Item
