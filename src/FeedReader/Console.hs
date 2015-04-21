{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module Main (main) where

import           Control.Monad         (forM, forM_, replicateM, unless)
import           Data.List             (intercalate)
import qualified Data.Sequence         as S
import           Data.Time.Clock       (getCurrentTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime,
                                        utcTimeToPOSIXSeconds)
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
  yield "Commands (use _ instead of space inside arguments):"
  yield "  help      : prints this helpful message"
  yield "  stats     : prints general stats"
  yield "  get t k   : prints the item with ID == k"
  yield "            : t is the table, one of 'cat', 'feed', 'person' or 'item'"
  yield "  page t p k: prints the next p entries with ID > k (t as above)"
  yield "  add t n   : inserts n random records into the DB (t as above)"
  yield "  gc        : performs GC"
  yield "  debug     : prints a debug message"
  yield "  quit      : quits the program"

processCommand h = do
  args <- words <$> await
  case head args of
    "help"    -> helpMessage
    "stats"   -> checkArgs 0 args h cmdStats
    "get"     -> checkArgs 2 args h cmdGet
    "page"    -> checkArgs 3 args h cmdPage
    "add"     -> checkArgs 2 args h cmdAdd
    "gc"      -> checkArgs 0 args h cmdGC
    "debug"   -> checkArgs 0 args h cmdDebug
    _         -> yield $ "Command '" ++ head args ++ "' not understood."
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
  DB.Cat <$> randomString 10 30

randomFeed c =
  DB.Feed <$> pure c
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
  DB.Person <$> randomString 10 30
            <*> randomString 100 300
            <*> randomString 10 30

randomItem f =
  DB.Item <$> pure f
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

timed f = do
  t0 <- liftBase getCurrentTime
  f
  t1 <- liftBase getCurrentTime
  yield $ "Command: " ++ show (DB.diffMs t0 t1) ++ " ms"

cmdStats h _ = timed $ do
  s <- liftBase $ DB.getStats h
  yield $ "Category count: " ++ show (DB.countCats s)
  yield $ "Feed count    : " ++ show (DB.countFeeds s)
  yield $ "Person count  : " ++ show (DB.countPersons s)
  yield $ "Entry count   : " ++ show (DB.countItems s)

type LookupRet a = IO (Maybe (DB.DocID a, a))

cmdGet h args = timed $ do
  let t = args !! 1
  let k = (read $ args !! 2) :: Int
  let out = \case
              Just s -> each $ lines s
              _      -> yield $ "No record found with ID == " ++ show k
  case t of
    "cat"    -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Cat)
                  >>= out . fmap (show . snd)
    "feed"   -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Feed)
                  >>= out . fmap (show . snd)
    "person" -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Person)
                  >>= out . fmap (show . snd)
    "item"   -> liftBase (DB.runLookup h (fromIntegral k) :: LookupRet Item)
                  >>= out . fmap (show . snd)
    _        -> yield $ t ++ " is not a valid table name."

cmdPage h args = timed $ do
  let t = args !! 1
  let p = (read $ args !! 2) :: Int
  let k = (read $ args !! 3) :: Int
  let extk k = if k == 0 then Nothing else Just $ fromIntegral k
  let format i = i ++ replicate (12 - length i) ' '
  let sCat (iid, i) = format (show iid) ++ show (catName i)
  let sFeed (iid, i) = format (show iid) ++
                       format (show $ feedCatID i) ++
                       show (feedUpdated i)
  let sPerson (iid, i) = format (show iid) ++ show (personName i)
  let sItem (iid, i) = format (show iid) ++
                       format (show $ itemFeedID i) ++
                       show (itemUpdated i)
  case t of
    "cat"    -> do
      as <- liftBase (DB.runPage h (extk k) "ID" p)
      yield $ format "ID" ++ "Name"
      each $ sCat <$> as
    "feed"   -> do
      as <- liftBase (DB.runPage h (extk k) "Updated" p)
      yield $ format "ID" ++ format "CatID" ++ "Updated"
      each $ sFeed <$> as
    "person" -> do
      as <- liftBase (DB.runPage h (extk k) "ID" p)
      yield $ format "ID" ++ "Name"
      each $ sPerson <$> as
    "item"   -> do
      as <- liftBase (DB.runPage h (extk k) "Updated" p)
      yield $ format "ID" ++ format "FeedID" ++ "Updated"
      each $ sItem <$> as
    _        -> yield $ t ++ " is not a valid table name."

cmdAdd h args = timed $ do
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
         liftBase $ DB.runInsert h a
       showIDs $ show <$> cs
    "person"  -> do
       ps <- replicateM n $ do
         a <- liftBase randomPerson
         liftBase $ DB.runInsert h a
       showIDs $ show <$> ps
    "feed" -> do
      cs' <- liftBase $ DB.runPage h Nothing "ID" 100
      let cs = S.fromList cs'
      let rs = take n $ randomRs (0, S.length cs - 1) g
      fs <- forM rs $ \r -> do
        f <- liftBase $ randomFeed $ fst $ S.index cs r
        liftBase $ DB.runInsert h f
      showIDs $ show <$> fs
    "item" -> do
      fs' <- liftBase $ DB.runPage h Nothing "Updated" 1000
      let fs = S.fromList fs'
      let rfids = (fst . S.index fs) <$> take n (randomRs (0, S.length fs - 1) g)
      is <- forM rfids $ liftBase . randomItem
      is' <- liftBase $ forM is $ DB.runInsert h
      showIDs $ show <$> is'
      return ()
    _      -> yield $ t ++ " is not a valid table name."


cmdGC h _ = timed $ do
  liftBase $ DB.performGC h
  yield "Checkpoint created."

cmdDebug h _ = timed $ do
  msg <- liftBase $ DB.debug h
  each $ lines msg

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
        putStrLn "  Opening DB..."
        h <- DB.open (Just "feeds.log") (Just "feeds.dat")
        t1 <- getCurrentTime
        putStrLn $ "  DB opened in " ++ show (DB.diffMs t0 t1) ++ " ms."
        return h )
    (\h -> do
        putStrLn "  Closing DB..."
        DB.close h
        putStrLn "  Goodbye." )
    pipeLine
