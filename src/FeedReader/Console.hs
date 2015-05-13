{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module Main (main) where

import           Control.Monad         (forM, forM_, replicateM, unless)
import           Data.List             (intercalate, isPrefixOf)
import           Data.Maybe            (fromJust)
import qualified Data.Sequence         as S
import           Data.Serialize        (Serialize (..), decode, encode)
import           Data.String           (IsString (..))
import           Data.Time.Clock       (UTCTime, getCurrentTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           FeedReader.DB         (DocID, Property)
import qualified FeedReader.DB         as DB
import           FeedReader.Types
import           Pipes
import qualified Pipes.Prelude         as P
import           Pipes.Safe
import           System.Random         (getStdGen, randomRIO, randomRs)

introMessage = do
  yield "Welcome to the Jungle!"
  yield "Type 'help' for a list of commands."
  cat

helpMessage = do
  yield "Commands:"
  yield "  help    : shows this helpful message"
  yield "  stats   : shows general stats"
  yield "  get t k : SELECT * FROM t WHERE ID = k"
  yield "          : t: 'cat', 'feed', 'person' or 'item'"
  yield "  range p t c s"
  yield "          : SELECT TOP p * FROM t WHERE c < s ORDER BY c DESC"
  yield "          : c: '*' will use a default column"
  yield "          : s: for date columns, start with 'D:' and replace ' ' with '_'"
  yield "          : s: for string columns, start with 'S:' (only first 4 chars matter)"
  yield "          : s: use '*' to start at the top"
  yield "  filter p t c k o s"
  yield "          : SELECT TOP p * FROM t WHERE c = k AND o < s ORDER BY o DESC"
  yield "  add n t : inserts n random records into the DB (t as above)"
  yield "  del k   : deletes document with ID = k"
  yield "  range_del p t c s"
  yield "          : deletes a range of documents starting at k"
  yield "          : params as in 'range'"
  yield "  gc      : performs GC"
  yield "  debug i c"
  yield "          : shows the internal state"
  yield "          : i: if '1' will show all indexes"
  yield "          : c: if '1' will show the contents of the cache"
  yield "  quit    : quits the program"

processCommand h = do
  args <- words <$> await
  case head args of
    "help"      -> helpMessage
    "stats"     -> checkArgs 0 args h cmdStats
    "get"       -> checkArgs 2 args h cmdGet
    "range"     -> checkArgs 4 args h cmdRange
    "filter"    -> checkArgs 6 args h cmdFilter
    "add"       -> checkArgs 2 args h cmdAdd
    "del"       -> checkArgs 1 args h cmdDel
    "range_del" -> checkArgs 4 args h cmdRangeDel
    "gc"        -> checkArgs 0 args h cmdGC
    "debug"     -> checkArgs 2 args h cmdDebug
    _           -> yield . showString "Command '" . showString (head args) $
                     "' not understood."
  processCommand h

checkArgs n args h f =
  if n == length args - 1 then f h args
  else yields' "Command '" $ showString (head args) . showString "' requires " .
         shows n . showString " arguments but " .
         shows (length args - 1) . showString " were supplied."

------------------------------------------------------------------------------
-- Random Utilities
------------------------------------------------------------------------------

randomString l r = do
  ln <- randomRIO (l, r)
  replicateM ln $ randomRIO (' ', '~')

randomStringIx l r = Indexable <$> randomString l r

randomContentIx l r = Indexable . Text <$> randomString l r

time2Int str = fromInteger . round . utcTimeToPOSIXSeconds . DB.text2UTCTime str

randomTime = do
  df <- getCurrentTime
  let l = time2Int "2000-01-01" df
  let r = time2Int "2020-01-01" df
  ti <- randomRIO (l, r)
  return . posixSecondsToUTCTime $ fromInteger ti

randomTimeIx = Indexable <$> randomTime

randomCat =
  Cat <$> randomStringIx 10 30

randomFeed c =
  Feed <$> pure c
          <*> randomString 100 300
          <*> randomContentIx 100 300
          <*> (Text <$> randomString 100 300)
          <*> randomString 2 10
          <*> pure []
          <*> pure []
          <*> (Text <$> randomString 10 50)
          <*> pure Nothing
          <*> randomTimeIx

randomPerson =
  Person <$> randomStringIx 10 30
         <*> randomString 100 300
         <*> randomString 10 30

randomItem f =
  Item <$> pure f
          <*> randomString 100 300
          <*> (Text <$> randomString 100 300)
          <*> (Text <$> randomString 100 300)
          <*> pure []
          <*> pure []
          <*> pure []
          <*> (Text <$> randomString 10 50)
          <*> (Text <$> randomString 200 300)
          <*> randomTimeIx
          <*> randomTimeIx

------------------------------------------------------------------------------
-- Command Functions
------------------------------------------------------------------------------

yields sf = yield $ sf ""
yields' s sf = yields $ showString s . sf

timed f = do
  t0 <- liftBase getCurrentTime
  f
  t1 <- liftBase getCurrentTime
  yields' "Command: " $ shows (DB.diffMs t0 t1) . showString " ms"

cmdStats h _ = timed $ do
  s <- liftBase $ DB.getStats h
  yields' "Category count: " $ shows (DB.countCats s)
  yields' "Feed count    : " $ shows (DB.countFeeds s)
  yields' "Person count  : " $ shows (DB.countPersons s)
  yields' "Entry count   : " $ shows (DB.countItems s)

type LookupRet a = IO (Maybe (DocID a, a))

cmdGet h args = timed $ do
  let t = args !! 1
  let kstr = args !! 2
  let k = read kstr :: Int
  let out = \case
              Just s -> each $ lines s
              _      -> yields' "No record found with ID = " $ showString kstr
  case t of
    "cat"    -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Cat)
                  >>= out . fmap (show . snd)
    "feed"   -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Feed)
                  >>= out . fmap (show . snd)
    "person" -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Person)
                  >>= out . fmap (show . snd)
    "item"   -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Item)
                  >>= out . fmap (show . snd)
    _        -> yield . shows t $ " is not a valid table name."

page h c p s k o now df = liftBase $
  if (k :: Int) == 0
  then DB.runRange h (parseVal s now) (fromString fprop) p
  else DB.runFilter h (fromIntegral k) (parseVal s now)
         (fromString fprop) (fromString oprop) p
  where fprop = if c == "*" then df else c
        oprop = if o == "*" then df else o

parseVal s now
  | "D:" `isPrefixOf` s = Just . DB.intVal $ text2UTCTime (drop 2 s) now
  | "S:" `isPrefixOf` s = Just . DB.intVal $ drop 2 s
  | s == "*"            = Nothing
  | otherwise           = Just . DB.intVal $ (read s :: Int)

cmdPage h args s k o = do
  let p = (read $ args !! 1) :: Int
  let t = args !! 2
  let c = args !! 3
  let formatsK k i = i . showString (replicate (k - length (i "")) ' ')
  let formats = formatsK 12
  let format = formats . showString
  let sCat (iid, i) = formats (shows iid) . showString (unIndexable $ catName i) $ ""
  let sFeed (iid, i) = formats (shows iid) .
                       formats (shows $ feedCatID i) .
                       shows (feedUpdated i) $ ""
  let sPerson (iid, i) = formats (shows iid) . showString (unIndexable $ personName i) $ ""
  let sItem (iid, i) = formats (shows iid) .
                       formats (shows $ itemFeedID i) .
                       formatsK 25 (shows $ itemUpdated i) .
                       shows (itemPublished i) $ ""
  now <- liftBase getCurrentTime
  case t of
    "cat"    -> do
      as <- page h c p s k o now "catName"
      yields $ format "ID" . showString "Name"
      each $ sCat <$> as
    "feed"   -> do
      as <- page h c p s k o now "feedUpdated"
      yields $ format "ID" . format "CatID" . showString "Updated"
      each $ sFeed <$> as
    "person" -> do
      as <- page h c p s k o now "personName"
      yields $ format "ID" . showString "Name"
      each $ sPerson <$> as
    "item"   -> do
      as <- page h c p s k o now "itemUpdated"
      yields $ format "ID" . format "FeedID" .
        formatsK 25 (showString "Updated") . showString "Published"
      each $ sItem <$> as
    _        -> yield . shows t $ " is not a valid table name."

cmdRange h args = timed $ do
  let s = args !! 4
  cmdPage h args s (0 :: Int) ""

cmdFilter h args = timed $ do
  let k = (read $ args !! 4) :: Int
  let o = args !! 5
  let s = args !! 6
  cmdPage h args s k o

clean :: [Maybe (DocID a)] -> [String]
clean = map (show . fromJust) . filter (not . null)

showIDs mbs = do
  let ids = clean mbs
  let l = length ids
  let (prefix, suffix) = if l > 10
                         then (showString "First 10 IDs: ", showString "...")
                         else (showString "IDs: ", showString ".")
  yields $ shows l . showString " records generated."
  yields $ prefix . showString (intercalate ", " (take 10 ids)) . suffix

cmdAdd h args = timed $ do
  let n = (read $ args !! 1) :: Int
  let t = args !! 2
  g <- liftBase getStdGen
  case t of
    "cat"    -> liftBase (P.toListM $ P.replicateM n
                  (randomCat >>= DB.runInsert h)) >>=
                showIDs
    "person" -> liftBase (P.toListM $ P.replicateM n
                  (randomPerson >>= DB.runInsert h)) >>=
                showIDs
    "feed" -> do
      cs' <- liftBase $ DB.runRange h Nothing "catName" 100
      let cs = S.fromList cs'
      let rs = take n $ randomRs (0, S.length cs - 1) g
      fs <- liftBase . P.toListM . for (each rs) $ \r ->
        (lift . randomFeed . fst . S.index cs) r >>=
        lift . DB.runInsert h >>=
        yield
      showIDs fs
    "item" -> do
      fs' <- liftBase $ DB.runRange h Nothing "feedUpdated" 1000
      let fs = S.fromList fs'
      let rfids = (fst . S.index fs) <$> take n (randomRs (0, S.length fs - 1) g)
      is <- liftBase . P.toListM . for (each rfids) $ \fid ->
        (lift . randomItem) fid >>=
        lift . DB.runInsert h >>=
        yield
      showIDs is
      return ()
    _      -> yield . shows t $ " is not a valid table name."

cmdDel h args = timed $ do
  let k = (read $ args !! 1) :: Int
  liftBase . DB.runDelete h $ fromIntegral k
  yield "Record deleted."

df c d = fromString $ if c == "*" then d else c

cmdRangeDel h args = timed $ do
  let t = args !! 2
  let p = (read $ args !! 1) :: Int
  let c = args !! 3
  let s = args !! 4
  now <- liftBase getCurrentTime
  let mb = parseVal s now
  sz <- if null mb
        then do
          yield "Explicit value required."
          return 0
        else let k = fromJust mb in case t of
          "cat"    -> DB.runDeleteRange h k (df c "catName"     :: Property Cat   ) p
          "feed"   -> DB.runDeleteRange h k (df c "feedUpdated" :: Property Feed  ) p
          "person" -> DB.runDeleteRange h k (df c "personName"  :: Property Person) p
          "item"   -> DB.runDeleteRange h k (df c "itemUpdated" :: Property Item  ) p
          _        -> do
                        yield . shows t $ " is not a valid table name."
                        return 0
  yields $ shows sz . showString " records deleted."

cmdGC h _ = timed $ do
  liftBase $ DB.performGC h
  yield "Garbage collection job scheduled."

cmdDebug h args = timed $ do
  let i = args !! 1 == "1"
  let c = args !! 2 == "1"
  liftBase (DB.debug h i c) >>= each . lines

pipeLine h =
      P.stdinLn
  >-> P.takeWhile (/= "quit")
  >-> processCommand h
  >-> introMessage
  >-> P.map (showString "  ")
  >-> P.stdoutLn

main :: IO ()
main = runSafeT . runEffect $ bracket
    (do t0 <- getCurrentTime
        putStrLn "  Opening DB..."
        h <- DB.open (Just "feeds.log") (Just "feeds.dat")
        t1 <- getCurrentTime
        putStrLn . showString "  DB opened in " . shows (DB.diffMs t0 t1) $ " ms."
        return h )
    (\h -> do
        putStrLn "  Closing DB..."
        DB.close h
        putStrLn "  Goodbye." )
    pipeLine
