module Main (main) where

import qualified Data.Map as M
import Data.Time.Clock
import Data.Time.Calendar
import Data.Acid
import System.IO
import System.Environment
import FeedReader.Data.DB

stdCat = FeedCategory "Main Category"

stdFeed = Feed 0 "Default Feed" "http://example.com" stdCat

stdDate d = UTCTime (fromGregorian 2015 3 $ read d) $ fromRational 0.0

stdItem d t = FeedItem 0 stdFeed "" t (stdDate d) "" ""

main :: IO ()
main = do args <- getArgs
          acid <- openLocalState (FeedsDB M.empty M.empty M.empty)
          case args of
            [day]
              -> do mbi <- query acid (GetNextItem $ stdDate day)
                    case mbi of
                      Nothing   -> putStrLn $ "no item after " ++ (show $ stdDate day)
                      Just item -> putStrLn $ "next to " ++ day ++ ": " ++ (show item)
            [day, title]
              -> do update acid (InsertItem $ stdItem day title)
                    putStrLn "Done."
            _ -> do putStrLn "Invalid args."
          closeAcidState acid
