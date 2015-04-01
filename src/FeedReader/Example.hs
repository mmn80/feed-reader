{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}

module Main (main) where

import           Control.Monad         (replicateM, replicateM_, forM_)
import           Data.Acid
import qualified Data.Sequence         as S
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import           FeedReader.DB         as DB
import           Pipes
import qualified Pipes.Prelude         as P
import           Pipes.Safe
import           System.Random         (randomRIO, getStdGen, randomRs)

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
  yield "  snap    : creates an acid-state checkpoint"
  yield "  clean   : wipes clean the database"
  -- yield "  date d  : shows the result of parsing a RSS/Atom date field"
  -- yield "  rand l r: generates a random string of length in range l..r"
  yield "  quit    : quits the program"

processCommand acid = do
  args <- words <$> await
  case head args of
    "help"  -> helpMessage
    "stats" -> checkArgs 0 args acid cmdStats
    "get"   -> checkArgs 2 args acid cmdGet
    "gen"   -> checkArgs 2 args acid cmdGen
    "snap"  -> checkArgs 0 args acid cmdSnap
    -- "date"  -> checkArgs 1 args acid cmdDate
    -- "rand"  -> checkArgs 2 args acid cmdRand
    "clean" -> checkArgs 0 args acid cmdClean
    _       -> yield $ "Command '" ++ head args ++ "' not understood."
  processCommand acid

checkArgs n args acid f =
  if n == length args - 1 then f acid args
  else yield $ "Command '" ++ head args ++ "' requires " ++ show n ++ " arguments\
               \ but " ++ show (length args) ++ " were supplied."

-- under '_' = ' '
-- under c   = c

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
  let diff = (fromRational $ toRational $ diffUTCTime t1 t0) :: Float
  yield $ "Command: " ++ show (diff * 1000) ++ " ms"

cmdStats acid args = timed $ do
  s <- liftBase $ query acid DB.GetStats
  yield $ "Category count: " ++ show (DB.countCats s)
  yield $ "Feed count    : " ++ show (DB.countFeeds s)
  yield $ "Person count  : " ++ show (DB.countPersons s)
  yield $ "Entry count   : " ++ show (DB.countItems s)

cmdGet acid args = timed $ do
  let t = args !! 1
  let k = (read $ args !! 2) :: Int
  let out = \case
              Just s -> each $ lines s
              _      -> yield $ "No record found with ID == " ++ show k
  case t of
    "cat"    -> liftBase (query acid $ DB.LookupCat  k)   >>= out . fmap show
    "feed"   -> liftBase (query acid $ DB.LookupFeed k)   >>= out . fmap show
    "person" -> liftBase (query acid $ DB.LookupPerson k) >>= out . fmap show
    "item"   -> liftBase (query acid $ DB.LookupItem k)   >>= out . fmap show
    _        -> yield $ t ++ " is not a valid table name."

cmdGen acid args = timed $ do
  let t = args !! 1
  let n = (read $ args !! 2) :: Int
  g <- liftBase getStdGen
  case t of
    "cat"  ->
       replicateM_ n $ do
         a <- liftBase randomCat
         liftBase $ update acid $ DB.InsertCat a
    "person"  ->
       replicateM_ n $ do
         a <- liftBase randomPerson
         liftBase $ update acid $ DB.InsertPerson a
    "feed" -> do
      cs <- liftBase $ query acid DB.Cats2Seq
      let rs = take n $ randomRs (0, S.length cs - 1) g
      forM_ rs $ \r -> do
        f <- liftBase $ randomFeed $ DB.catID $ S.index cs r
        liftBase $ update acid $ DB.InsertFeed f
    "item" -> do
      fs <- liftBase $ query acid DB.Feeds2Seq
      let rs = take n $ randomRs (0, S.length fs - 1) g
      forM_ rs $ \r -> do
        i <- liftBase $ randomItem $ DB.feedID $ S.index fs r
        liftBase $ update acid $ DB.InsertItem i
    _      -> yield $ t ++ " is not a valid table name."


cmdSnap acid args = timed $ do
  liftBase $ createCheckpoint acid
  yield "Checkpoint created."

cmdClean acid args = timed $ do
  yield "Are you sure? (y/n)"
  r <- await
  case r of
    "y" -> do
             liftBase $ update acid DB.WipeDB
             yield "Database wiped clean. Have a nice day."
    _   -> yield "Crisis averted."

pipeLine acid =
  P.stdinLn
  >-> P.takeWhile (/= "quit")
  >-> processCommand acid
  >-> introMessage
  >-> P.map ("  " ++)
  >-> P.stdoutLn

main :: IO ()
main = runSafeT $ runEffect $ bracket
    (do t0 <- getCurrentTime
        putStrLn "  Opening DB..."
        acid <- openLocalState DB.emptyDB
        t1 <- getCurrentTime
        let diff = (fromRational $ toRational $ diffUTCTime t1 t0) :: Float
        putStrLn $ "  DB opened in " ++ show (diff * 1000) ++ " ms."
        return acid )
    (\acid -> do
        putStrLn "  Closing DB..."
        closeAcidState acid
        putStrLn "  Goodbye." )
    pipeLine
