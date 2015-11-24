{-# LANGUAGE OverloadedStrings #-}

import Control.Lens                                 hiding ( (.=) )
import Control.Concurrent
import Control.Monad
import qualified Control.Exception                  as E

import           Data.Int
import           Data.Aeson hiding (Result)
import           Data.Aeson.Lens
import           Data.Maybe
import qualified Data.Text                          as T
import qualified Data.ByteString.Char8              as C8
import qualified Data.ByteString.Lazy               as BL
import qualified Data.ByteString                    as BS
import Data.Binary.Get


import           Network.Wreq
import qualified Network.HTTP.Client                as HT

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

import           Database.MySQL.Simple
import           Database.MySQL.Simple.QueryResults
import           Database.MySQL.Simple.Result

import Database.MySQL.Base.Types (Field(..), Type(..))


data CampaignInputMessage = CampaignInputMessage { campaignId :: Integer,
                                                   inputId :: Integer,
                                                   status :: T.Text,
                                                   thumbnailUrl :: T.Text
                                                 } deriving (Show)

data CreateEncodingResponse = CreateEncodingResponse { cid :: Integer,
                                                       jobId :: Maybe Integer,
                                                       jobStatus :: Maybe T.Text,
                                                       mpdUrl :: Maybe T.Text
                                                     } deriving (Show)

instance FromJSON CampaignInputMessage where
 parseJSON (Object v) =
    CampaignInputMessage <$> v .: "campaignId"
                         <*> v .: "inputId"
                         <*> v .: "status"
                         <*> v .: "thumbnailUrl"
 parseJSON _ = mzero

instance ToJSON CreateEncodingResponse where
    toJSON (CreateEncodingResponse cid (Just jobId) (Just jobStatus) (Just mpdUrl)) =
      object ["campaignId" .= cid, "jobId" .= jobId, "jobStatus" .= jobStatus, "mpdUrl" .= mpdUrl]

api :: String -> String
api s = "http://portal.bitcodin.com/api" ++ s --PROD
--api s = "http://private-anon-b5423cd68-bitcodinrestapi.apiary-mock.com/api" ++ s --MOCK

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createJob :: Integer -> IO (Response BL.ByteString)
createJob input = postWith opts (api "/job/create") payload
    where payload = encode $ object ["inputId" .= input,
                                   "encodingProfileId" .= (8867 :: Integer),
                                   "manifestTypes" .= ["mpd" :: T.Text, "m3u8" :: T.Text]
                                  ]


decodeCIMPayload :: C8.ByteString -> Maybe CampaignInputMessage
decodeCIMPayload p = decode $ BL.fromStrict p

handleConsume :: Either KafkaError KafkaMessage -> IO (Either String CampaignInputMessage)
handleConsume e =
    case e of
      (Left err) -> case err of
                      KafkaResponseError RdKafkaRespErrTimedOut -> return $ Left $ "[INFO] " ++ show err
                      _                                         -> return $ Left $ "[ERROR] " ++ show err
      (Right m) ->
          --print $ BL.fromStrict $ messagePayload m
          case decodeCIMPayload $ messagePayload m of
            Nothing -> return $ Left $ "[ERROR] decode campaignInput: " ++ (show $ messagePayload m)
            Just m -> return $ Right m

handleResponse :: Integer -> Response BL.ByteString -> IO (Either String CreateEncodingResponse)
handleResponse cid r =
    case code of
        201 -> do
          print $ r ^? responseBody . key "manifestUrls" . key "mpdUrl"
          case jobStatus encodingResponse of
            Just s -> case s of
              "Enqueued" -> return $ Right encodingResponse
              _ -> return $ Left $ "[ERROR] job status: " ++ show s
            Nothing -> return $ Left "[ERROR] job status could not be determined."
        _ -> return $ Left $ handleErrorResponse code r
    where
      code = r ^. responseStatus . statusCode
      rb k t = r ^? responseBody . k . t
      encodingResponse = CreateEncodingResponse cid
                                                (rb (key "jobId") _Integer )
                                                (rb (key "status") _String)
                                                (rb (key "manifestUrls" . key "mpdUrl") _String)

handleErrorResponse :: Int -> Response BL.ByteString -> String
handleErrorResponse e r =
  case e of
    404 -> "(404, Not found) " ++ show r
    _   -> show r

produce :: KafkaProduceMessage -> IO (Maybe KafkaError)
produce message = do
  let partition = 0
      host = "localhost:9092"
      topic = "campaignEncoding"
      kafkaConfig = []
      topicConfig = []
  withKafkaProducer kafkaConfig topicConfig
                    host topic
                    $ \kafka topic -> produceMessage topic (KafkaSpecifiedPartition partition) message

updateMedia :: Connection -> (T.Text, T.Text, T.Text) -> IO Int64
updateMedia conn (url, jobId, cid) = do
   let q = "update media set mpd_url=?, encoding_job=? where id = (select media_id from campaign where id=?)" :: Query
   let str = [url, jobId, cid]
   execute conn q str

encodeInputs :: IO ()
encodeInputs = do
    let partition = 0
        host = "localhost:9092"
        topic = "campaignInput"
        kafkaConfig = []
        topicConfig = []
    withKafkaConsumer kafkaConfig topicConfig
                      host topic
                      partition
                      KafkaOffsetStored
                      $ \kafka topic -> forever $ do
                        c <- handleConsume =<< consumeMessage topic partition 1000
                        case c of
                          Right m -> do
                            resp <- handleResponse (campaignId m) =<< createJob (inputId m)
                            case resp of
                              Right enc -> do
                                prod <- produce $ KafkaProduceMessage $ BL.toStrict $ encode enc
                                case prod of
                                  Nothing -> do
                                    putStrLn $ "[INFO] Produced Encoding: " ++ show enc
                                    conn <- getConnection
                                    u <- updateMedia conn (fromJust $ mpdUrl enc, T.pack $ show $ fromJust $ jobId enc, T.pack $ show $ cid enc)
                                    print u
                                  Just e -> print e
                              Left e -> putStrLn e
                          Left e -> putStrLn e
                        threadDelay 1000000

data JobStatus = Created | Enqueued | InProgress | Finished | EncodeError | Transfering | Transfered deriving (Enum, Show, Eq)
type JobId = Int
type EncodingJob = (JobId, JobStatus)
type ResponseError = T.Text
type JobStatusResponse = Either ResponseError T.Text

findJobIdStatus :: Connection -> IO [(Maybe Int, Int)]
findJobIdStatus conn = E.handle logError (query conn q [fromEnum Transfering :: Int])
                        where q = "select m.encoding_job, m.status from media m where m.status < ?" :: Query
                              logError (E.SomeException e) = do print ("findjobidstatus: " ++ show e); return []

findEncodingJobs :: Connection -> IO [EncodingJob]
findEncodingJobs conn = do
                        js <- findJobIdStatus conn
                        return $ map toEncodingJob (filterNothing js)
                        where toEncodingJob (jId, jStatus) = (jId, toEnum jStatus :: JobStatus) :: EncodingJob
                              filterNothing t = map (\(x, y) -> (fromJust x, y)) (filter (isJust . fst) t)

getJobStatus :: JobId -> IO (Either HT.HttpException (Response BL.ByteString))
getJobStatus job = do
  putStrLn $ "[INFO] GET: " ++ endpoint
  E.try (getWith opts endpoint)
  where endpoint = api "/job/" ++ show job ++ "/status"

handleError :: HT.HttpException -> ResponseError
handleError (HT.StatusCodeException s _ _) = T.pack $ show s
handleError e = T.pack $ show e

handleStatusResponse :: Either HT.HttpException (Response BL.ByteString) -> JobStatusResponse
handleStatusResponse (Left e ) = Left $ handleError e
handleStatusResponse (Right r) =
  case code of
    200 -> case jobStatus of
      Just s -> Right s
      Nothing -> Left $ T.pack $ "[ERROR] Job Status not found in response: " ++ show r
    _ -> Left $ T.pack $ handleErrorResponse code r
  where
    code = r ^. responseStatus . statusCode
    rb k t = r ^? responseBody . k . t
    jobStatus = rb (key "status") _String

startTransfer :: IO ()
startTransfer = putStrLn "Transfer started"

updateJob :: Connection -> EncodingJob -> JobStatusResponse -> IO ()
updateJob conn job rs = case rs of
  Left e -> putStrLn $ "[ERROR] " ++ show e
  Right s -> case s of
                "Created" -> updateIfStatusNotEquals Created
                "Enqueued" -> updateIfStatusNotEquals Enqueued
                "In Progress" -> updateIfStatusNotEquals InProgress
                "Finished" -> do
                                updateIfStatusNotEquals Finished
                                startTransfer
                "Error" -> updateIfStatusNotEquals EncodeError
                _ -> print $ "[ERROR] Unexpected job status for job " ++ show (fst job) ++ ": " ++ show s
             where updateIfStatusNotEquals s = fromMaybe (return ()) (updateJobStatus conn s job)

updateJobStatus :: Connection -> JobStatus -> EncodingJob -> Maybe (IO ())
updateJobStatus conn newStatus (jobId, jobStatus)
    | newStatus == jobStatus = Nothing
    | otherwise = Just $ check =<< E.try (execute conn q [fromEnum newStatus :: Int, jobId :: Int])
                  where q = "update media set status=? where encoding_job = ?" :: Query
                        check (Left (E.SomeException e)) = putStrLn $ "[ERROR] Updating status (" ++ show jobStatus ++ ") not successfull for job with id " ++ show jobId ++ ": " ++ show e
                        check (Right r) = putStrLn $ "[Info] Job with id " ++ show jobId ++ show "update to: " ++ show newStatus

handleJobs :: Connection -> IO ()
handleJobs conn = mapM_ (\(jobId, jobStatus) -> updateJob conn (jobId, jobStatus)
                                        . handleStatusResponse
                                        =<< getJobStatus jobId
                        ) =<< findEncodingJobs conn

getConnection :: IO Connection
getConnection = connect defaultConnectInfo {  connectHost = "127.0.0.1",
                                              connectPassword = "password",
                                              connectDatabase = "plads" }

main :: IO ()
main = do
  conn <- getConnection
  _ <- forkIO encodeInputs
  _ <- forever $ do
    handleJobs conn
    threadDelay 1000000
  putStrLn "running"
