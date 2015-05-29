{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

module Main (main) where

import           Control.Monad         (liftM, replicateM, unless)
import           Data.List             (intercalate, isPrefixOf)
import qualified Data.Sequence         as S
import           Data.String           (fromString)
import           Data.Time.Clock       (getCurrentTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           FeedReader.DB         (Property, Reference)
import qualified FeedReader.DB         as DB
import           FeedReader.Import     (downloadFeed, importOPML, updateFeed)
import           FeedReader.Types
import           FeedReader.Utils      (diffMs, text2DateTime)
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
  yield "  getk t k: SELECT * FROM t WHERE UniqueKey = k"
  yield "          : t: 'feed', 'person' or 'item'"
  yield "          : UniqueKey: 'URL', 'Name' or 'URL'"
  yield "  range p t c s"
  yield "          : SELECT TOP p * FROM t WHERE c < s ORDER BY c DESC"
  yield "          : c: '*' will use a default column"
  yield "          : s: for date columns, start with 'D:' and replace ' ' with '_'"
  yield "          : s: for string columns, start with 'S:' (only first 4 chars matter)"
  yield "          : s: use '*' to start at the top"
  yield "  filter p t c k o s"
  yield "          : SELECT TOP p * FROM t WHERE c = k AND o < s ORDER BY o DESC"
  yield "  add n t : inserts n random records into the DB (t as above)"
  yield "  cat n p : inserts a category"
  yield "          : n: name"
  yield "          : p: parent ID"
  yield "  del k   : deletes document with ID = k"
  yield "  range_del p t c s"
  yield "          : deletes a range of documents starting at k"
  yield "          : params as in 'range'"
  yield "  gc      : performs GC"
  yield "  debug i c"
  yield "          : shows the internal state"
  yield "          : i: if '1' will show all indexes"
  yield "          : c: if '1' will show the contents of the cache"
  yield "  curl u  : downloads and parses feed at URL = u"
  yield "  opml p  : imports OPML file at path p"
  yield "  feed k  : downloads, parses and updates feed with ID = k"
  yield "  status i s"
  yield "          : updates the status of item with ID = i to s"
  yield "          : s: one of New, Unread, Read"
  yield "  quit    : quits the program"

processCommand h = do
  args <- words <$> await
  unless (null args) $
    case head args of
      "help"      -> helpMessage
      "stats"     -> doAbortable 0 args h cmdStats
      "get"       -> doAbortable 2 args h cmdGet
      "getk"      -> doAbortable 2 args h cmdGetk
      "range"     -> doAbortable 4 args h cmdRange
      "filter"    -> doAbortable 6 args h cmdFilter
      "add"       -> doAbortable 2 args h cmdAdd
      "cat"       -> doAbortable 2 args h cmdAddCat
      "del"       -> doAbortable 1 args h cmdDel
      "range_del" -> doAbortable 4 args h cmdRangeDel
      "gc"        -> doAbortable 0 args h cmdGC
      "debug"     -> doAbortable 2 args h cmdDebug
      "curl"      -> doAbortable 1 args h cmdCurl
      "opml"      -> doAbortable 1 args h cmdOPML
      "feed"      -> doAbortable 1 args h cmdFeed
      "status"    -> doAbortable 2 args h cmdItemStatus
      _           -> yield . showString "Command '" . showString (head args) $
                       "' not understood."
  processCommand h

doAbortable n args h f =
  if n == length args - 1
  then f h args `catches` [ Handler (each . lines . transErr)
                          , Handler (each . lines . dbErr) ]
  else yields' "Command '" $ showString (head args) . showString "' requires " .
         shows n . showString " arguments but " .
         shows (length args - 1) . showString " were supplied."
  where transErr (DB.AbortUnique s)   = s
        transErr (DB.AbortConflict s) = s
        transErr (DB.AbortDelete s)   = s

        dbErr (DB.LogParseError s) = s
        dbErr (DB.DataParseError pos sz s) = showString s . showString
          "\nPosition: " . shows pos . showString "\nSize: " $ shows sz ""
        dbErr (DB.IdAllocationError s) = s
        dbErr (DB.DataAllocationError sz mbsz s) = showString s . showString
          "\nSize requested: " . shows sz . showString
          "\nLargest gap: " . maybe (showString "no gaps") shows mbsz $ ""

------------------------------------------------------------------------------
-- Random Utilities
------------------------------------------------------------------------------

randomString l r = do
  ln <- randomRIO (l, r)
  replicateM ln $ randomRIO (' ', '~')

randomStringIx l r = Sortable <$> randomString l r

randomContentIx l r = Sortable . Text <$> randomString l r

time2Int str = fromInteger . round . utcTimeToPOSIXSeconds .
               DB.unDateTime . text2DateTime str

randomTime = do
  now <- DB.DateTime <$> getCurrentTime
  let l = time2Int "2000-01-01" now
  let r = time2Int "2020-01-01" now
  ti <- randomRIO (l, r)
  return . DB.DateTime . posixSecondsToUTCTime $ fromInteger ti

randomTimeIx = Sortable <$> randomTime

randomCat =
  Cat <$> randomStringIx 10 30
      <*> pure Nothing

randomFeed c =
   Feed <$> pure (Just c)
        <*> (Unique <$> randomString 100 300)
        <*> pure Nothing
        <*> randomString 100 300
        <*> randomContentIx 100 300
        <*> (Text <$> randomString 100 300)
        <*> randomString 2 10
        <*> pure []
        <*> pure []
        <*> (Text <$> randomString 10 50)
        <*> pure Nothing
        <*> randomTimeIx
        <*> pure False
        <*> pure Nothing

randomPerson =
  Person <$> (Unique <$> randomStringIx 10 30)
         <*> randomString 100 300
         <*> randomString 10 30

randomItem f s =
     Item <$> pure f
          <*> (Unique <$> randomString 100 300)
          <*> (Text <$> randomString 100 300)
          <*> (Text <$> randomString 100 300)
          <*> pure []
          <*> pure []
          <*> pure []
          <*> (Text <$> randomString 10 50)
          <*> (Text <$> randomString 200 300)
          <*> randomTimeIx
          <*> randomTimeIx
          <*> pure s

------------------------------------------------------------------------------
-- Command Functions
------------------------------------------------------------------------------

yields sf = yield $ sf ""
yields' s sf = yields $ showString s . sf

timed f = do
  t0 <- liftBase getCurrentTime
  f
  t1 <- liftBase getCurrentTime
  yields' "Command: " $ showsTimeDiff t0 t1

timeOf f = do
  t0 <- liftBase getCurrentTime
  r  <- f
  t1 <- liftBase getCurrentTime
  return (r, diffMs t0 t1)

showsTimeDiff t0 t1 = showsTimeDiff' $ diffMs t0 t1

showsTimeDiff' d =
  if d >= 1 then shows d . showString " ms"
  else shows (d * 1000) . showString " us"

handleAbort cmd = liftBase cmd >>= either throwM return

showTime dt = yields' "Command: " $ showsTimeDiff' dt

cmdStats h _ = timed $ do
  s <- handleAbort (DB.getStats h)
  yields' "Category count: " $ shows (DB.countCats s)
  yields' "Feed count    : " $ shows (DB.countFeeds s)
  yields' "Person count  : " $ shows (DB.countPersons s)
  yields' "Entry count   : " $ shows (DB.countItems s)

type LookupRet a = IO (Either DB.TransactionAbort (Maybe a))

cmdGet h args = do
  let t = args !! 1
  let kstr = args !! 2
  let k = read kstr :: Int
  let out = maybe (yields' "No record found with Id = " $ showString kstr)
                  (each . lines)
  case t of
    "cat"    -> do
       (r, dt) <- timeOf $ handleAbort
                    (DB.runLookup h (fromIntegral k) :: LookupRet Cat)
       out $ fmap show r
       showTime dt
    "feed"   -> do
       (r, dt) <- timeOf $ handleAbort
                    (DB.runLookup h (fromIntegral k) :: LookupRet Feed)
       out $ fmap show r
       showTime dt
    "person" -> do
       (r, dt) <- timeOf $ handleAbort
                    (DB.runLookup h (fromIntegral k) :: LookupRet Person)
       out $ fmap show r
       showTime dt
    "item"   -> do
       (r, dt) <- timeOf $ handleAbort
                    (DB.runLookup h (fromIntegral k) :: LookupRet Item)
       out $ fmap show r
       showTime dt
    _        -> yield . shows t $ " is not a valid table name."

type LookupUnqRet a = IO (Either DB.TransactionAbort (Maybe (Reference a, a)))

cmdGetk h args = timed $ do
  let t = args !! 1
  let k = args !! 2
  let out = maybe (yields' "No record found with Key = " $ showString k)
                  (each . lines)
  case t of
    "feed"   ->
      handleAbort (DB.runUnique h "feedURL" (DB.Unique k)
               :: LookupUnqRet Feed) >>= out . fmap (show . snd)
    "person" ->
      handleAbort (DB.runUnique h "personName" (DB.Unique k)
               :: LookupUnqRet Person) >>= out . fmap (show . snd)
    "item"   ->
      handleAbort (DB.runUnique h "itemURL" (DB.Unique k)
               :: LookupUnqRet Item) >>= out . fmap (show . snd)
    _        -> yield . shows t $ " is not a valid table name."

page h c p s mk o now dft = handleAbort $
  maybe (DB.runRange h p (fromString fprop) (parseVal s now))
  (\k -> DB.runFilterRange h p (fromString fprop) (nk k)
                          (fromString oprop)(parseVal s now))
  mk
  where fprop = if c == "*" then dft else c
        oprop = if o == "*" then dft else o
        nk k  = if k == 0 then Nothing else Just $ fromIntegral (k :: Int)

parseVal s now
  | "D:" `isPrefixOf` s = Just . DB.Sortable . DB.toKey . DB.Sortable $
                            text2DateTime (drop 2 s) now
  | "S:" `isPrefixOf` s = Just . DB.Sortable . DB.toKey . DB.Sortable $ drop 2 s
  | s == "*"            = Nothing
  | otherwise           = Just . DB.Sortable . DB.toKey . DB.Sortable $
                            (read s :: Int)

cmdPage h args s mk o = do
  let p = (read $ args !! 1) :: Int
  let t = args !! 2
  let c = args !! 3
  let sCat (iid, i) = formats (shows iid) . showString (unSortable $ catName i) $ ""
  let sFeed (iid, i) = formats (shows iid) . formats (showsFeedCat i) .
                       shows (feedUpdated i) $ ""
  let sPerson (iid, i) = formats (shows iid) . showString (unSortable .
                         unUnique $ personName i) $ ""
  now <- DB.DateTime <$> liftBase getCurrentTime
  case t of
    "cat"    -> do
      (as, dt) <- timeOf $ page h c p s mk o now "catName"
      yields $ format "Id" . showString "Name"
      each $ sCat <$> as
      showTime dt
    "feed"   -> do
      (as, dt) <- timeOf $ page h c p s mk o now "feedUpdated"
      yields $ format "Id" . format "Category" . showString "Updated"
      each $ sFeed <$> as
      showTime dt
    "person" -> do
      (as, dt) <- timeOf $ page h c p s mk o now "personName"
      yields $ format "Id" . showString "Name"
      each $ sPerson <$> as
      showTime dt
    "item"   -> do
      (as, dt) <- timeOf $ page h c p s mk o now "itemUpdated"
      showItems h as
      showTime dt
    _        -> yield . shows t $ " is not a valid table name."

formatsK k i = let str = take (k - 1) $ i "" in
               showString str .
               showString (replicate (k - length str) ' ')
formats = formatsK 12
format = formats . showString

content2Str (Text str)  = take 27 str ++ "..."
content2Str (HTML str)  = take 27 str ++ "..."
content2Str (XHTML str) = take 27 str ++ "..."

showStatus StatusNew     = "(?)"
showStatus StatusUnread  = " ?"
showStatus StatusRead    = " ~"
showStatus StatusStarred = "{★}"

showItems h as = do
  yields $ format "Id" . format "Feed" .
    formatsK 20 (showString "Updated") . showString "Sta Title"
  as' <- traverse (\(iid, i) -> do
    st <- handleAbort . DB.runQuery h . DB.itemStatusToKey $ itemStatus i
    return (iid, i, st)) as
  each $ sItem <$> as'
  where sItem (iid, i, st) = formats (shows iid) .
                             formats (shows $ itemFeed i) .
                             formatsK 20 (shows $ itemUpdated i) .
                             formatsK 4 (showString $ showStatus st) $
                             content2Str (itemTitle i)

cmdRange h args = do
  let s = args !! 4
  cmdPage h args s Nothing ""

cmdFilter h args = do
  let k = (read $ args !! 4) :: Int
  let o = args !! 5
  let s = args !! 6
  cmdPage h args s (Just k) o

showIDs is = do
  let ids = map show is
  let l = length ids
  let (prefix, suffix) = if l > 10
                         then (showString "First 10 Ids: ", showString "...")
                         else (showString "Ids: ", showString ".")
  yields $ shows l . showString " records generated."
  yields $ prefix . showString (intercalate ", " (take 10 ids)) . suffix

nothing = Nothing :: Maybe (Sortable DB.IxKey)

cmdAdd h args = timed $ do
  let n = (read $ args !! 1) :: Int
  let t = args !! 2
  g <- liftBase getStdGen
  case t of
    "cat"    -> handleAbort (liftM sequence . P.toListM . P.replicateM n $
                  randomCat >>= DB.runInsert h)
                >>= showIDs
    "person" -> handleAbort (liftM sequence . P.toListM . P.replicateM n $
                  randomPerson >>= DB.runInsert h)
                >>= showIDs
    "feed" -> do
      cs' <- handleAbort $ DB.runRange h maxBound "catName" nothing
      let cs = S.fromList cs'
      let rs = take n $ randomRs (0, S.length cs - 1) g
      fs <- handleAbort $ liftM sequence . P.toListM . for (each rs) $ \r ->
        (lift . randomFeed . fst . S.index cs) r >>=
        lift . DB.runInsert h >>=
        yield
      showIDs fs
    "item" -> do
      fs' <- handleAbort $ DB.runRange h maxBound "feedUpdated" nothing
      let fs = S.fromList fs'
      let rfids = (fst . S.index fs) <$> take n (randomRs (0, S.length fs - 1) g)
      stNew <- handleAbort . DB.runQuery h $ DB.itemStatusByKey StatusNew
      is <- handleAbort $ liftM sequence . P.toListM . for (each rfids) $ \fid ->
        lift (randomItem fid stNew) >>=
        lift . DB.runInsert h >>=
        yield
      showIDs is
      return ()
    _      -> yield . shows t $ " is not a valid table name."

cmdAddCat h args = timed $ do
  let n = args !! 1
  let p = (read $ args !! 2) :: Int
  let c = Cat (Sortable n) (if p == 0 then Nothing else Just $ fromIntegral p)
  cid <- handleAbort $ DB.runInsert h c
  yields' "Category " $ shows cid . showString " inserted."

cmdDel h args = timed $ do
  let k = (read $ args !! 1) :: Int
  handleAbort . DB.runDelete h $ fromIntegral k
  yield "Record deleted."

def c d = fromString $ if c == "*" then d else c

cmdRangeDel h args = timed $ do
  let t = args !! 2
  let p = (read $ args !! 1) :: Int
  let c = args !! 3
  let s = args !! 4
  now <- DB.DateTime <$> liftBase getCurrentTime
  sz <- maybe (yield "Explicit value required." >> return 0)
    (\k -> case t of
      "cat"    ->
        handleAbort (DB.runDeleteRange h p (def c "catName"     :: Property Cat   ) k)
      "feed"   ->
        handleAbort (DB.runDeleteRange h p (def c "feedUpdated" :: Property Feed  ) k)
      "person" ->
        handleAbort (DB.runDeleteRange h p (def c "personName"  :: Property Person) k)
      "item"   ->
        handleAbort (DB.runDeleteRange h p (def c "itemUpdated" :: Property Item  ) k)
      _        -> yield (shows t " is not a valid table name.") >> return 0)
    (parseVal s now)
  yields $ shows sz . showString " records deleted."

cmdGC h _ = timed $ do
  liftBase $ DB.performGC h
  yield "Garbage collection job scheduled."

cmdDebug h args = timed $ do
  let i = args !! 1 == "1"
  let c = args !! 2 == "1"
  liftBase (DB.debug h i c) >>= each . lines

cmdCurl _ args = timed $ do
  let url = args !! 1
  liftBase (show <$> downloadFeed url) >>= each . lines

cmdFeed h args = timed $ do
  let f = (read $ args !! 1) :: Int
  let fid = fromIntegral f
  (mbf, is) <- handleAbort $ updateFeed h fid
  maybe (yield "Feed not found.") (\feed -> do
    yield . showString "Feed " . shows fid $ " updated ok."
    yield ""
    each . lines $ show feed
    yield ""
    showItems h is ) mbf

cmdItemStatus h args = timed $ do
  let i = (read $ args !! 1) :: Int
  let s = args !! 2
  let iid = fromIntegral i
  case s of
    "New"     -> handleAbort $ DB.runUpdateItemStatus h iid StatusNew
    "Unread"  -> handleAbort $ DB.runUpdateItemStatus h iid StatusUnread
    "Read"    -> handleAbort $ DB.runUpdateItemStatus h iid StatusRead
    "Starred" -> handleAbort $ DB.runUpdateItemStatus h iid StatusStarred
    _         -> yield "Status unknown."

showsFeedCat f = maybe (showString "-") shows $ feedCat f

cmdOPML h args = timed $ do
  let path = args !! 1
  res <- importOPML h path
  either yield (\rs -> do
    yield $ shows (length rs) " feeds merged:"
    yields $ format "Id" . format "Category" . showString "URL"
    each $ map (\(fid, f) -> formats (shows fid) .
                             formats (showsFeedCat f) .
                             shows (feedURL f) $ "") rs )
    res

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
        h <- DB.open (Just "feeds.log") (Just "feeds.dat") Nothing Nothing
        t1 <- getCurrentTime
        putStrLn . showString "  DB opened in " $ showsTimeDiff t0 t1 ""
        return (h :: DB.Handle DB.FileLogState) )
    (\h -> do
        putStrLn "  Closing DB..."
        DB.close h
        putStrLn "  Goodbye." )
    pipeLine
