{-# LANGUAGE OverloadedStrings #-}

import Control.Lens                                 hiding ( (.=) )
import Control.Concurrent                           ( threadDelay )
import Control.Monad

import           Data.Int
import           Data.Aeson
import           Data.Aeson.Lens
import           Data.Maybe
import qualified Data.Text                          as T
import qualified Data.Map                           as M
import qualified Data.ByteString.Char8              as C8
import qualified Data.ByteString.Lazy               as BL

import           Network.Wreq

import           Haskakafka
import           Haskakafka.InternalRdKafkaEnum

import           Database.MySQL.Simple

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
    toJSON (CreateEncodingResponse (cid) (Just jobId) (Just jobStatus) (Just mpdUrl)) =
      object ["campaignId" .= cid, "jobId" .= jobId, "jobStatus" .= jobStatus, "mpdUrl" .= mpdUrl]

api :: String -> String
api s = "http://portal.bitcodin.com/api" ++ s --PROD
--api s = "http://private-anon-b5423cd68-bitcodinrestapi.apiary-mock.com/api" ++ s --MOCK

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createJob :: Integer -> IO (Response BL.ByteString)
createJob input = do
    let payload = encode $ object ["inputId" .= input,
                                   "encodingProfileId" .= (8867 :: Integer),
                                   "manifestTypes" .= ["mpd" :: T.Text, "m3u8" :: T.Text] 
                                  ]
    postWith opts (api "/job/create") payload

decodeCIMPayload :: C8.ByteString -> Maybe CampaignInputMessage
decodeCIMPayload p = decode $ BL.fromStrict p

handleConsume :: Either KafkaError KafkaMessage -> IO (Either String CampaignInputMessage)
handleConsume e = do
    case e of
      (Left err) -> case err of
                      KafkaResponseError RdKafkaRespErrTimedOut -> return $ Left $ "[INFO] " ++ (show err)
                      _                                         -> return $ Left $ "[ERROR] " ++ (show err)
      (Right m) -> do
          --print $ BL.fromStrict $ messagePayload m
          case decodeCIMPayload $ messagePayload m of
            Nothing -> return $ Left $ "[ERROR] decode campaignInput: " ++ (show $ messagePayload m)
            Just m -> return $ Right m

handleResponse :: Integer -> Response BL.ByteString -> IO (Either String CreateEncodingResponse)
handleResponse cid r = do
    case code of
        201 -> do
          print $ r ^? responseBody . key "manifestUrls" . key "mpdUrl"
          case jobStatus encodingResponse of
            Just s -> case s of
              "Enqueued" -> return $ Right encodingResponse
              _ -> return $ Left $ "[ERROR] job status: " ++ show s
            Nothing -> return $ Left $ "[ERROR] job status could not be determined."
        _ -> return $ Left $ handleErrorResponse code r
    where
      code = (r ^. responseStatus . statusCode)
      rb = \k t -> r ^? responseBody . k . t
      encodingResponse = CreateEncodingResponse (cid)
                                                (rb (key "jobId") _Integer )
                                                (rb (key "status") _String)
                                                (rb (key "manifestUrls" . key "mpdUrl") _String)

handleErrorResponse :: Int -> (Response BL.ByteString) -> String
handleErrorResponse e r = do
  case e of
    404 -> "Not found."
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
                    $ \kafka topic -> do
  produceMessage topic (KafkaSpecifiedPartition partition) message

updateMedia :: Connection -> (T.Text, T.Text, T.Text) -> IO Int64
updateMedia conn (url, jobId, cid) = do
   let q = "update media set mpd_url=?, encoding_job=? where id = (select media_id from campaign where id=?)" :: Query
   let str = [url, jobId, cid]
   execute conn q str

main :: IO ()
main = do
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
        resp <- (handleResponse (campaignId m))=<< (createJob $ inputId m)
        case resp of
          Right enc -> do
            prod <- produce $ KafkaProduceMessage $ BL.toStrict $ encode enc
            case prod of
              Nothing -> do
                putStrLn $ "[INFO] Produced Encoding: " ++ show enc
                conn <- connect defaultConnectInfo {
                                                      connectPassword = "password",
                                                      connectDatabase = "plads"
                                                   }
                u <- updateMedia conn (fromJust $ mpdUrl enc, T.pack $ show $ fromJust $ jobId enc, T.pack $ show $ cid enc)
                print u
              Just e -> putStrLn $ show e
          Left e -> putStrLn e
      Left e -> putStrLn e
