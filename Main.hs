{-# LANGUAGE OverloadedStrings #-}

import Control.Lens                                 hiding ( (.=) )
import Control.Concurrent                           ( threadDelay )
import Control.Monad

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

data CampaignInputMessage = CampaignInputMessage { inputId :: Integer,
                                                   status :: T.Text,
                                                   thumbnailUrl :: T.Text
                                                 } deriving (Show)

data CreateEncodingResponse = CreateEncodingResponse { jobId :: Maybe Integer,
                                                       jobStatus :: Maybe T.Text,
                                                       mpdUrl :: Maybe T.Text
                                                     } deriving (Show)

instance FromJSON CampaignInputMessage where
 parseJSON (Object v) =
    CampaignInputMessage <$> v .: "inputId"
                         <*> v .: "status"
                         <*> v .: "thumbnailUrl"
 parseJSON _ = mzero

instance ToJSON CreateEncodingResponse where
  toJSON (CreateEncodingResponse (Just jobId) (Just jobStatus) (Just mpdUrl)) =
      object ["jobId" .= jobId, "jobStatus" .= jobStatus, "mpdUrl" .= mpdUrl]

api :: String -> String
--api s = "http://portal.bitcodin.com/api" ++ s --PROD
api s = "http://private-anon-b5423cd68-bitcodinrestapi.apiary-mock.com/api" ++ s --MOCK

opts :: Options
opts = defaults & header "bitcodin-api-key" .~ ["3d03c4648b4b6170e7ad7986b637ddcd26a6053a49eb2aa25ec01961a4dd3e2d"]

createJob :: Integer -> IO (Response BL.ByteString)
createJob input = do
    let payload = encode $ object ["inputId" .= input, "encodingProfile" .= (1 :: Integer)]
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
          print $ BL.fromStrict $ messagePayload m
          case decodeCIMPayload $ messagePayload m of
            Nothing -> return $ Left $ "[ERROR] decode campaignInput: " ++ (show $ messagePayload m)
            Just m -> return $ Right m

handleResponse :: Response BL.ByteString -> IO (Either String CreateEncodingResponse)
handleResponse r = do
    print r
    case code of
        201 -> do
          print $ jobStatus encodingResponse
          case fromJust $ jobStatus encodingResponse of
            "Enqueued" -> return $ Right encodingResponse
            _ -> return $ Left $ "[ERROR] job status: " ++ show (fromJust $ jobStatus encodingResponse)
        _ -> return $ Left $ handleErrorResponse code
    where
      code = (r ^. responseStatus . statusCode)
      rb = \x t -> r ^? responseBody . key x . t
      encodingResponse = CreateEncodingResponse (rb "jobId" _Integer )
                                                (rb "status" _String)
                                                (rb "manifestUrls" _String) --todo

handleErrorResponse :: Int -> String
handleErrorResponse e = do
  case e of
    404 -> "Not found."

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
        resp <- handleResponse =<< (createJob $ inputId m)
        case resp of
          Right input -> do
            prod <- produce $ KafkaProduceMessage $ BL.toStrict $ encode input
            case prod of
              Nothing -> putStrLn "produced"
              Just e -> putStrLn $ show e
          Left e -> putStrLn e
      Left e -> putStrLn e
