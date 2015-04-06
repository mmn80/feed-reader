{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import           Control.Monad         (forM, forM_, replicateM, unless)
import           Data.List             (intercalate)
import qualified Data.Sequence         as S
import           Data.Time.Clock       (getCurrentTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
import           FeedReader.DB         as DB
import           Pipes
import qualified Pipes.Prelude         as P
import           Pipes.Safe
import           System.Random         (getStdGen, randomRIO, randomRs)

introMessage = do
  yield "Welcome to the Jungle!"
  yield "Type 'help' for a list of commands."
  cat

helpMessage = do
  yield "Commands (use _ instead of space inside arguments):"
  yield "  help    : prints this helpful message"
  yield "  stats   : prints general stats"
  yield "  shards  : lists all shard IDs with their record counts"
  yield "  get t k : prints the item with ID == k"
  yield "          : t is the table, one of 'cat', 'feed', 'person' or 'item'"
  yield "  page p k: prints the next p entries with ID > k"
  yield "  gen t n : inserts n random records into the DB (t as above)"
  yield "  snap    : creates a checkpoint"
  yield "  archive : archives the unused log files"
  yield "  clean   : wipes clean the database"
  -- yield "  date d  : shows the result of parsing a RSS/Atom date field"
  -- yield "  rand l r: generates a random string of length in range l..r"
  yield "  quit    : quits the program"

processCommand h = do
  args <- words <$> await
  case head args of
    "help"    -> helpMessage
    "stats"   -> checkArgs 0 args h cmdStats
    "shards"  -> checkArgs 0 args h cmdShards
    "get"     -> checkArgs 2 args h cmdGet
    "page"    -> checkArgs 2 args h cmdPage
    "gen"     -> checkArgs 2 args h cmdGen
    "snap"    -> checkArgs 0 args h cmdSnap
    "archive" -> checkArgs 0 args h cmdArchive
    -- "date"  -> checkArgs 1 args h cmdDate
    -- "rand"  -> checkArgs 2 args h cmdRand
    "clean" -> checkArgs 0 args h cmdClean
    _       -> yield $ "Command '" ++ head args ++ "' not understood."
  processCommand h

checkArgs n args h f =
  if n == length args - 1 then f h args
  else yield $ "Command '" ++ head args ++ "' requires " ++ show n ++ " arguments\
               \ but " ++ show (length args) ++ " were supplied."

------------------------------------------------------------------------------
-- Random Utilities
------------------------------------------------------------------------------

randomString l r = do
  ln <- randomRIO (l, r)
  replicateM ln $ randomRIO (' ', '~')

randomTime = do
  df <- getCurrentTime
  let l = fromInteger $ round $ utcTimeToPOSIXSeconds $ DB.text2UTCTime "2000-01-01" df
  let r = fromInteger $ round $ utcTimeToPOSIXSeconds $ DB.text2UTCTime "2020-01-01" df
  ti <- randomRIO (l, r)
  return $ posixSecondsToUTCTime $ fromInteger ti

randomCat =
  DB.Cat <$> pure DB.unsetID <*> randomString 10 30

randomFeed c =
  DB.Feed <$> pure DB.unsetID
          <*> pure c
          <*> randomString 100 300
          <*> (DB.Text <$> randomString 100 300)
          <*> (DB.Text <$> randomString 100 300)
          <*> randomString 2 10
          <*> pure []
          <*> pure []
          <*> (DB.Text <$> randomString 10 50)
          <*> pure Nothing
          <*> randomTime

randomPerson =
  DB.Person <$> pure DB.unsetID
            <*> randomString 10 30
            <*> randomString 100 300
            <*> randomString 10 30

randomItem f =
  DB.Item <$> pure DB.unsetID
          <*> pure f
          <*> randomString 100 300
          <*> (DB.Text <$> randomString 100 300)
          <*> (DB.Text <$> randomString 100 300)
          <*> pure []
          <*> pure []
          <*> pure []
          <*> (DB.Text <$> randomString 10 50)
          <*> (DB.Text <$> randomString 200 300)
          <*> randomTime
          <*> randomTime

------------------------------------------------------------------------------
-- Command Functions
------------------------------------------------------------------------------

-- under '_' = ' '
-- under c   = c

-- cmdDate acid args = do
--   df <- liftBase getCurrentTime
--   yield $ "Using " ++ show df ++ " as default date."
--   let d = DB.text2UTCTime (under <$> args !! 1) df
--   yield $ "Date parsed as: " ++ show d
--
-- cmdRand acid args = do
--   let l = (read $ args !! 1) :: Int
--   let r = (read $ args !! 2) :: Int
--   str <- liftBase $ randomString l r
--   yield str

timed f = do
  t0 <- liftBase getCurrentTime
  f
  t1 <- liftBase getCurrentTime
  yield $ "Command: " ++ show (DB.diffMs t0 t1) ++ " ms"

format sz i = i ++ replicate (sz - length i) ' '

cmdStats h _ = timed $ do
  (s, ss) <- liftBase $ DB.getStats h
  yield $ "Pending       : " ++ show (DB.statsPending s)
  yield $ "Category count: " ++ show (DB.countCats s)
  yield $ "Feed count    : " ++ show (DB.countFeeds s)
  yield $ "Person count  : " ++ show (DB.countPersons s)
  yield $ "Entry count   : " ++ show (DB.countItemsAll s)
  yield $ "Shard count   : " ++ show (DB.countShards s)
  yield $ "Opened shards :" ++ if null ss then " 0" else ""
  unless (null ss) $ yield $ "  Shard ID"  ++ replicate (20 - 8) ' ' ++
                             "Entries" ++ replicate (10 - 7) ' ' ++
                             "Last accessed"
  forM_ ss $ \(sid, t, sz) ->
    yield $ "  " ++ format 20 (show sid) ++ format 10 (show sz) ++ show t

cmdShards h _ = timed $ do
  ss <- liftBase $ DB.getShardStats h
  yield $ "  Shard ID"  ++ replicate (20 - 8) ' ' ++ "Entries"
  forM_ ss $ \(sid, sz) ->
    yield $ "  " ++ format 20 (show sid) ++ format 10 (show sz)

cmdGet h args = timed $ do
  let t = args !! 1
  let k = (read $ args !! 2) :: Int
  let out = \case
              Just s -> each $ lines s
              _      -> yield $ "No record found with ID == " ++ show k
  case t of
    "cat"    -> liftBase (DB.findCat    h k) >>= out . fmap show
    "feed"   -> liftBase (DB.findFeed   h k) >>= out . fmap show
    "person" -> liftBase (DB.findPerson h k) >>= out . fmap show
    "item"   -> liftBase (DB.findItem   h k) >>= out . fmap show
    _        -> yield $ t ++ " is not a valid table name."

showShortHeader = "ID" ++ replicate (24 - 2) ' ' ++
                  "FeedID" ++ replicate (24 - 6) ' ' ++
                  "Published"

showShort i = format 24 (show $ itemID i) ++
              format 24 (show $ itemFeedID i) ++
              show (itemPublished i)

cmdPage h args = timed $ do
  let p = (read $ args !! 1) :: Int
  let k = (read $ args !! 2) :: Int
  is <- liftBase $ DB.getItemPage h k p
  yield showShortHeader
  each $ showShort <$> is

cmdGen h args = timed $ do
  let t = args !! 1
  let n = (read $ args !! 2) :: Int
  g <- liftBase getStdGen
  let showIDs ids = do
        let l = length ids
        let (prefix, suffix) = if l > 10
                               then ("First 10 IDs: ", "...")
                               else ("IDs: ", ".")
        yield $ show l ++ " records generated."
        yield $ prefix ++ intercalate ", " (take 10 ids) ++ suffix
  case t of
    "cat"  -> do
       cs <- replicateM n $ do
         a <- liftBase randomCat
         liftBase $ DB.addCat h a
       showIDs $ (show . DB.catID) <$> cs
    "person"  -> do
       ps <- replicateM n $ do
         a <- liftBase randomPerson
         liftBase $ DB.addPerson h a
       showIDs $ (show . DB.personID) <$> ps
    "feed" -> do
      cs <- liftBase $ DB.getCats h
      let rs = take n $ randomRs (0, S.length cs - 1) g
      fs <- forM rs $ \r -> do
        f <- liftBase $ randomFeed $ DB.catID $ S.index cs r
        liftBase $ DB.addFeed h f
      showIDs $ (show . DB.feedID) <$> fs
    "item" -> do
      fs <- liftBase $ DB.getFeeds h
      let rfids = (DB.feedID . S.index fs) <$> take n (randomRs (0, S.length fs - 1) g)
      is <- forM rfids $ liftBase . randomItem
      is' <- liftBase $ DB.addItems h is
      showIDs $ (show . DB.itemID) <$> is'
      return ()
    _      -> yield $ t ++ " is not a valid table name."


cmdSnap h _ = timed $ do
  liftBase $ DB.checkpoint h
  yield "Checkpoint created."

cmdArchive h _ = timed $ do
  liftBase $ DB.archive h
  yield "Archive created."

cmdClean h _ = timed $ do
  yield "Are you sure? (y/n)"
  r <- await
  case r of
    "y" -> do
             liftBase $ DB.wipeDB h
             yield "Database wiped clean. Have a nice day."
    _   -> yield "Crisis averted."

pipeLine h =
      P.stdinLn
  >-> P.takeWhile (/= "quit")
  >-> processCommand h
  >-> introMessage
  >-> P.map ("  " ++)
  >-> P.stdoutLn

main :: IO ()
main = runSafeT $ runEffect $ bracket
    (do t0 <- getCurrentTime
        putStrLn "  Opening master DB..."
        h <- DB.open Nothing
        t1 <- getCurrentTime
        putStrLn $ "  DB opened in " ++ show (DB.diffMs t0 t1) ++ " ms."
        return h )
    (\h -> do
        putStrLn "  Closing DB..."
        DB.close h
        putStrLn "  Goodbye." )
    pipeLine
