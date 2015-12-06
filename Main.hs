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


import           Network.Wreq
import qualified Network.HTTP.Client                as HT

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

import           Database.MySQL.Simple

data MediaStatus = Uploaded
                 | InputCreated
                 | InputError
                 | EncodeCreated
                 | EncodeEnqueued
                 | EncodeInProgress
                 | EncodeFinished
                 | EncodeError
                 | Transfering
                 | Transfered
                 | TransferError
                 deriving (Enum, Show, Eq)

data MediaInputMessage = MediaInputMessage { miMediaId :: Integer,
                                             miInputId :: Integer,
                                             miStatus :: T.Text,
                                             miThumbnailUrl :: T.Text
                                           } deriving (Show)

data CreateEncodingResponse = CreateEncodingResponse { erMediaId :: Integer,
                                                       erJobId :: Maybe Integer,
                                                       erJobStatus :: Maybe T.Text,
                                                       erMpdUrl :: Maybe T.Text
                                                     } deriving (Show)

instance FromJSON MediaInputMessage where
 parseJSON (Object v) =
    MediaInputMessage <$> v .: "mediaId"
                      <*> v .: "inputId"
                      <*> v .: "status"
                      <*> v .: "thumbnailUrl"
 parseJSON _ = mzero

instance ToJSON CreateEncodingResponse where
    toJSON (CreateEncodingResponse cid (Just jobId) (Just jobStatus) (Just mpdUrl)) =
      object ["mediaId" .= cid, "jobId" .= jobId, "jobStatus" .= jobStatus, "mpdUrl" .= mpdUrl]

getConnection :: IO Connection
getConnection = connect defaultConnectInfo {  connectHost = "127.0.0.1",
                                              connectPassword = "password",
                                              connectDatabase = "plads" }

api :: String -> String
api s = "http://portal.bitcodin.com/api" ++ s --PROD
--api s = "https://private-anon-d220f1902-bitcodinrestapi.apiary-mock.com/api" ++ s --MOCK

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createJob :: Integer -> IO (Response BL.ByteString)
createJob input = postWith opts (api "/job/create") payload
    where payload = encode $ object ["inputId" .= input,
                                   "encodingProfileId" .= (8867 :: Integer),
                                   "manifestTypes" .= ["mpd" :: T.Text, "m3u8" :: T.Text]
                                  ]


decodeCIMPayload :: C8.ByteString -> Maybe MediaInputMessage
decodeCIMPayload p = decode $ BL.fromStrict p

handleConsume :: Either KafkaError KafkaMessage -> IO (Either String MediaInputMessage)
handleConsume e =
    case e of
      (Left err) -> case err of
                      KafkaResponseError RdKafkaRespErrTimedOut -> return $ Left $ "[INFO] " ++ show err
                      _                                         -> return $ Left $ "[ERROR] " ++ show err
      (Right m) ->
          --print $ BL.fromStrict $ messagePayload m
          case decodeCIMPayload $ messagePayload m of
            Nothing -> return $ Left $ "[ERROR] decode campaignInput: " ++ show (messagePayload m)
            Just m -> return $ Right m

handleResponse :: Integer -> Response BL.ByteString -> IO (Either String CreateEncodingResponse)
handleResponse cid r =
    case code of
        201 -> do
          print $ r ^? responseBody . key "manifestUrls" . key "mpdUrl"
          case erJobStatus encodingResponse of
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

updateMedia :: Connection -> (T.Text, T.Text, T.Text) -> IO Int64
updateMedia conn (url, jobId, mediaId) = execute conn q args
   where q = "update media set status=?, mpd_url=?, encoding_job=? where id=?" :: Query --TODO: handle exception
         args = [T.pack $ show $ fromEnum EncodeCreated, url, jobId, mediaId]


encodeInputs :: IO ()
encodeInputs = do
    conn <- getConnection
    let partition = 0
        host = "localhost:9092"
        topic = "mediaInput"
        kafkaConfig = []
        topicConfig = []
    withKafkaConsumer kafkaConfig topicConfig
                      host topic
                      partition
                      KafkaOffsetStored
                      $ \kafka topic -> forever $ do
                        c <- handleConsume =<< consumeMessage topic partition 1000
                        case c of
                          Right mediaInput -> do
                            resp <- handleResponse (miMediaId mediaInput) =<< createJob (miInputId mediaInput)
                            case resp of
                              Right encodeResponse -> do
                                u <- updateMedia conn (fromJust $ erMpdUrl encodeResponse, T.pack $ show $ fromJust $ erJobId encodeResponse, T.pack $ show $ erMediaId encodeResponse)
                                print $ "[INFO] Produced Encoding: " ++ show u
                              Left e -> putStrLn e
                          Left e -> putStrLn e
                        threadDelay 1000000

type JobId = Int
type EncodingJob = (JobId, MediaStatus)
type ResponseError = T.Text
type JobStatusResponse = Either ResponseError T.Text

findJobIdStatus :: Connection -> IO [(Maybe Int, Int)]
findJobIdStatus conn = E.handle logError (query conn q args)
                        where q = "select m.encoding_job, m.status from media m where m.status > ? AND m.status < ?" :: Query
                              args = [fromEnum InputCreated :: Int, fromEnum EncodeFinished :: Int]
                              logError (E.SomeException e) = do print ("findjobidstatus: " ++ show e); return []

findEncodingJobs :: Connection -> IO [EncodingJob]
findEncodingJobs conn = do
                        js <- findJobIdStatus conn
                        return $ map toEncodingJob (filterNothing js)
                        where toEncodingJob (jId, jStatus) = (jId, toEnum jStatus :: MediaStatus) :: EncodingJob
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

updateJobProgress :: Connection -> EncodingJob -> JobStatusResponse -> IO ()
updateJobProgress conn job rs = case rs of
  Left e -> putStrLn $ "[ERROR] " ++ show e
  Right s -> case s of
                "Created" -> updateIfStatusNotEquals EncodeCreated
                "Enqueued" -> updateIfStatusNotEquals EncodeEnqueued
                "In Progress" -> updateIfStatusNotEquals EncodeInProgress
                "Finished" -> do updateIfStatusNotEquals EncodeFinished
                                 startTransfer $ fst job
                "Error" -> updateIfStatusNotEquals EncodeError
                _ -> print $ "[ERROR] Unexpected job status for job " ++ show (fst job) ++ ": " ++ show s
             where updateIfStatusNotEquals s = fromMaybe sameStatusMessage (updateJobStatus conn s job)
                   sameStatusMessage = putStrLn $ "Job: " ++ show (fst job) ++ "still in status: " ++ show s

updateJobStatus :: Connection -> MediaStatus -> EncodingJob -> Maybe (IO ())
updateJobStatus conn newStatus (jobId, jobStatus)
    | newStatus == jobStatus = Nothing
    | otherwise = Just $ check =<< E.try (execute conn q [fromEnum newStatus :: Int, jobId :: Int])
                  where q = "update media set status=? where encoding_job = ?" :: Query
                        check (Left (E.SomeException e)) = putStrLn $ "[ERROR] Updating status (" ++ show jobStatus ++ ") not successfull for job with id " ++ show jobId ++ ": " ++ show e
                        check (Right r) = putStrLn $ "[Info] Job with id " ++ show jobId ++ show " has new Status: " ++ show newStatus

startTransfer :: JobId -> IO ()
startTransfer job = do
  p <- produce "mediaTransfer" (KafkaProduceMessage $ BL.toStrict $ encode job)
  case p of
    Just e -> putStrLn $ "[ERROR] Job " ++ show job ++ " could not be initialized for Transfer."
    Nothing -> putStrLn $ "[INFO] Job " ++ show job ++ " initialized for Transfer."

produce :: String -> KafkaProduceMessage -> IO (Maybe KafkaError)
produce t message = do
  let partition = 0
      host = "localhost:9092"
      topic = t
      kafkaConfig = []
      topicConfig = []
  withKafkaProducer kafkaConfig topicConfig
                    host topic
                    $ \kafka topic -> produceMessage topic (KafkaSpecifiedPartition partition) message

monitorJobs :: Connection -> IO ()
monitorJobs conn = mapM_ (\(jobId, jobStatus) ->
                            updateJobProgress conn (jobId, jobStatus) . handleStatusResponse =<< getJobStatus jobId
                        ) =<< findEncodingJobs conn

main :: IO ()
main = do
  conn <- getConnection
  _ <- forkIO encodeInputs
  _ <- forever $ do
    monitorJobs conn
    threadDelay 1000000
  putStrLn "running"
