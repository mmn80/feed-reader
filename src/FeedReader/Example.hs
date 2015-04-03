{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import           Control.Monad         (forM, forM_, replicateM, replicateM_)
import           Control.Monad.State   (get, put)
import           Data.Acid
import qualified Data.Sequence         as S
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import           FeedReader.DB         as DB
import           Pipes
import qualified Pipes.Prelude         as P
import           Pipes.Safe
import           System.FilePath       ((</>))
import           System.Random         (getStdGen, randomRIO, randomRs)

introMessage = do
  yield "Welcome to the Jungle!"
  yield "Type 'help' for a list of commands."
  cat

helpMessage = do
  yield "Commands (use _ instead of space inside arguments):"
  yield "  help    : prints this helpful message"
  yield "  stats   : prints record counts"
  yield "  get t k : prints the item with ID == k"
  yield "          : t is the table, one of 'cat', 'feed', 'person' or 'item'"
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
    "get"     -> checkArgs 2 args h cmdGet
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
  DB.Cat <$> pure DB.unsetCatID <*> randomString 10 30

randomFeed c =
  DB.Feed <$> pure DB.unsetFeedID
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
  DB.Person <$> pure DB.unsetPersonID
            <*> randomString 10 30
            <*> randomString 100 300
            <*> randomString 10 30

randomItem f =
  DB.Item <$> pure DB.unsetItemID
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

cmdStats h args = timed $ do
  s <- liftBase $ DB.getStats h
  yield $ "Category count: " ++ show (DB.countCats s)
  yield $ "Feed count    : " ++ show (DB.countFeeds s)
  yield $ "Person count  : " ++ show (DB.countPersons s)
  yield $ "Entry count   : " ++ show (DB.countItems s)
  yield $ "Shard count   : " ++ show (DB.countShards s)

cmdGet h args = timed $ do
  let t = args !! 1
  let k = (read $ args !! 2) :: Int
  let out = \case
              Just s -> each $ lines s
              _      -> yield $ "No record found with ID == " ++ show k
  case t of
    "cat"    -> liftBase (DB.getCat    h k) >>= out . fmap show
    "feed"   -> liftBase (DB.getFeed   h k) >>= out . fmap show
    "person" -> liftBase (DB.getPerson h k) >>= out . fmap show
    "item"   -> liftBase (DB.getItem   h k) >>= out . fmap show
    _        -> yield $ t ++ " is not a valid table name."

cmdGen h args = timed $ do
  let t = args !! 1
  let n = (read $ args !! 2) :: Int
  g <- liftBase getStdGen
  case t of
    "cat"  ->
       replicateM_ n $ do
         a <- liftBase randomCat
         liftBase $ DB.addCat h a
    "person"  ->
       replicateM_ n $ do
         a <- liftBase randomPerson
         liftBase $ DB.addPerson h a
    "feed" -> do
      cs <- liftBase $ DB.getCats h
      let rs = take n $ randomRs (0, S.length cs - 1) g
      forM_ rs $ \r -> do
        f <- liftBase $ randomFeed $ DB.catID $ S.index cs r
        liftBase $ DB.addFeed h f
    "item" -> do
      fs <- liftBase $ DB.getFeeds h
      let rfids = (DB.feedID . S.index fs) <$> take n (randomRs (0, S.length fs - 1) g)
      is <- forM rfids $ liftBase . randomItem
      liftBase $ DB.addItems h is
      return ()
    _      -> yield $ t ++ " is not a valid table name."


cmdSnap h args = timed $ do
  liftBase $ DB.checkpoint h
  yield "Checkpoint created."

cmdArchive h args = timed $ do
  liftBase $ DB.archive h
  yield "Archive created."

cmdClean h args = timed $ do
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
