{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import           Control.Monad         (replicateM, replicateM_)
import           Data.Acid
import qualified Data.IntMap           as M
import qualified Data.Sequence         as S
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import           FeedReader.DB         as DB
import           FeedReader.XML2DB
import           Pipes
import qualified Pipes.Prelude         as P
import           Pipes.Safe
import           System.Random         (randomIO, randomRIO)

introMessage = do
  yield "Welcome to the Jungle!"
  yield "Type 'help' for a list of commands."
  cat

helpMessage = do
  yield "Commands (use _ instead of space inside arguments):"
  yield "  help       : Prints this helpful message."
  yield "  date d     : Shows the result of parsing a RSS/Atom date field."
  yield "  rand l r   : Generates a random string of length in range l..r."
  yield "  stats      : Prints record counts."
  yield "  next d     : Prints the first item with ID > d."
  yield "  insert n   : Inserts n random entries into the DB."
  yield "  checkpoint : Creates an acid-state checkpoint."
  yield "  clean      : Wipes clean the database."
  yield "  quit       : Quits the program."

processCommand acid = do
  args <- words <$> await
  case head args of
    "help"       -> helpMessage
    "date"       -> checkArgs 1 args acid cmdDate
    "rand"       -> checkArgs 2 args acid cmdRand
    "stats"      -> checkArgs 0 args acid cmdStats
    "next"       -> checkArgs 1 args acid cmdNext
    "insert"     -> checkArgs 1 args acid cmdInsert
    "checkpoint" -> checkArgs 0 args acid cmdCheckpoint
    "clean"      -> checkArgs 0 args acid cmdClean
    _            -> yield $ "Command '" ++ head args ++ "' not understood."
  processCommand acid

checkArgs n args acid f =
  if n == length args - 1 then f acid args
  else yield $ "Command '" ++ head args ++ "' requires " ++ show n ++ " arguments\
               \ but " ++ show (length args) ++ " were supplied."

under '_' = ' '
under c   = c

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
  DB.UserCategory <$> randomRIO (1, 1000) <*> randomString 10 30

randomFeed cs = do
  catIdx <- randomRIO (0, S.length cs - 1)
  DB.Feed <$> pure 0
          <*> pure (S.index cs catIdx)
          <*> randomString 100 300
          <*> (DB.Text <$> randomString 100 300)
          <*> (DB.Text <$> randomString 100 300)
          <*> randomString 2 10
          <*> pure []
          <*> pure []
          <*> (DB.Text <$> randomString 10 50)
          <*> pure Nothing
          <*> randomTime

randomItem fs = do
  feedIdx <- randomRIO (0, S.length fs - 1)
  DB.Item <$> pure 0
          <*> pure (S.index fs feedIdx)
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

cmdDate acid args = do
  df <- liftBase getCurrentTime
  yield $ "Using " ++ show df ++ " as default date."
  let d = DB.text2UTCTime (under <$> args !! 1) df
  yield $ "Date parsed as: " ++ show d

cmdRand acid args = do
  let l = (read $ args !! 1) :: Int
  let r = (read $ args !! 2) :: Int
  str <- liftBase $ randomString l r
  yield str

dbCommand args f = do
  t0 <- liftBase getCurrentTime
  f t0
  t1 <- liftBase getCurrentTime
  let diff = (fromRational $ toRational $ diffUTCTime t1 t0) :: Float
  yield $ "Command: " ++ show (diff * 1000) ++ " ms"

cmdStats acid args = dbCommand args $ \df -> do
  s <- liftBase $ query acid DB.GetStats
  yield $ "Category count: " ++ show (DB.countCats s)
  yield $ "Feed count    : " ++ show (DB.countFeeds s)
  yield $ "Entry count   : " ++ show (DB.countItems s)

cmdNext acid args = dbCommand args $ \df -> do
  let ix = (read $ args !! 1) :: Int
  mbi <- liftBase $ query acid (DB.GetNextItem ix)
  case mbi of
    Nothing -> yield $ "No entry found with ID > " ++ show ix
    Just i  -> each $ lines $ show i

cmdInsert acid args = dbCommand args $ \df -> do
  let n = (read $ args !! 1) :: Int
  yield "Generating 20 random cats..."
  cs <- liftBase $ S.replicateM 20 randomCat
  yield "Generating 200 random feeds..."
  fs <- liftBase $ S.replicateM 200 $ randomFeed cs
  yield $ "Generating " ++ show n ++ " random entries..."
  liftBase $ replicateM_ n $ do
    item <- randomItem fs
    update acid $ DB.InsertItem item

cmdCheckpoint acid args = dbCommand args $ \df -> do
  liftBase $ createCheckpoint acid
  yield "Checkpoint created."

cmdClean acid args = dbCommand args $ \df -> do
  liftBase $ update acid DB.WipeDB
  yield "Database wiped clean. Have a nice day."

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
        acid <- openLocalState (FeedsDB M.empty M.empty M.empty)
        t1 <- getCurrentTime
        let diff = (fromRational $ toRational $ diffUTCTime t1 t0) :: Float
        putStrLn $ "  DB opened in " ++ show (diff * 1000) ++ " ms."
        return acid )
    (\acid -> do
        putStrLn "  Closing DB..."
        closeAcidState acid
        putStrLn "  Goodbye." )
    pipeLine
