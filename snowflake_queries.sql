CREATE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'your arn role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your bucket name');




-- Now i create a database with name KAFKA_LIVE_DATA
CREATE DATABASE KAFKA_LIVE_DATA;

 -- use the above created database
USE DATABASE KAFKA_LIVE_DATA;

 -- now create a table with name  top_100_crypto_data_sink with columns name as below:
CREATE TABLE top_100_crypto_data_sink (
    SYSTEM_INSERTED_TIMESTAMP TIMESTAMP,
    RANK INTEGER,
    NAME VARCHAR,
    SYMBOL VARCHAR,
    PRICE NUMBER,
    PERCENT_CHANGE_24H FLOAT,
    VOLUME_24H NUMBER,
    MARKET_CAP NUMBER,
    CURRENCY VARCHAR
);
 
 -- now create a table with name top_100_crypto_data_json with columns json_text VARIANT (means data comes in json)
CREATE TABLE top_100_crypto_data_json(
    json_data VARIANT
);

-- Connect snowflake to data source(with AWS S3 bucket)
CREATE STAGE @ext_stage 
URL = 's3://kafka-stock-market-video-mudassir/'
STORAGE_INTEGRATION = s3_int;

show stages  


--second way (in this create storage integeration no need)
-- CREATE OR REPLACE STAGE ext_stage
-- URL = 's3://coinmarketcap-bucket/real_time_data/'
-- CREDENTIALS = (
--     AWS_KEY_ID='<key-id>',
--     AWS_SECRET_KEY='<secret-key>'
-- );

 
-- pipe for sink data
CREATE OR REPLACE PIPE live_crypto_data
AUTO_INGEST = TRUE
AS
COPY INTO KAFKA_LIVE_DATA.PUBLIC.top_100_crypto_data_sink
FROM @ext_stage;

-- pipe for json data
CREATE OR REPLACE PIPE live_crypto_data
AUTO_INGEST = TRUE
AS
COPY INTO KAFKA_LIVE_DATA.PUBLIC.top_100_crypto_data_json
FROM @ext_stage
FILE_FORMAT = (TYPE=JSON);
  

--for manually refreshing the snowpipe
ALTER PIPE live_crypto_data REFRESH;

SHOW PIPES;

--for sink data
SELECT count(*) AS COUNT FROM kafka_live_data.public.top_100_crypto_data_sink;

-- for json data
   -- Now select table and check data come in it or not 
select * from  kafka_live_data.public.top_100_crypto_data_json;


 
 
 