# real-time_crypto_data_pipeline_using_kafka_S3_and_Snowflake

## Introduction 
In this project, you will execute an End-To-End Data Engineering Project on Real-Time crypto Data using Kafka

I am using  Apache Kafka to produce and consume scraped data.

In this project, I've created a real-time data pipeline that utilizes Kafka to scrape, process, and load data onto S3 in JSON format. With a producer-consumer architecture, I ensure that the data is in the right format for loading onto S3 by performing minor transformations while consuming it.

<!-- But that's not all - I've also used AWS crawler to crawl the data and generate a schema catalog. Athena utilizes this catalog, allowing me to query the data directly from S3 without loading it first. This saves time and resources and enables me to get insights from the data much faster! -->

Moreover, I've connected S3 with Snowflake using Snowpipe. As data is loaded onto S3, a SNS notification is sent to Snowpipe, which then automatically starts loading the data into Snowflake. This makes data loading a seamless and automated process, freeing up time for other important tasks lile querry and analysis.

## Architecture 
![kafka_proj](crypto_data.png)


## Technology Used
- Programming Language - Python
- Amazon Web Service (AWS)
1. S3 (Simple Storage Service)
2. EC2 or Local Machine
3. Apache Kafka
4. Snowflake

## Follow the below Process  :  

    1)First step is to create S3 bucket:

<img src="./images/buc1.png">

<img src="./images/buck2.png">

<img src="./images/buc3.png">

<br>
   
    Now if you want work on ec2 instance machine then first create ec2 instance machine if not want to run ec2 then move forward

<img src="./images/ec1.png">

<br>

    Then click to launch instances
<img src="./images/ec2.png">
<img src="./images/ec3.png">
<img src="./images/ec4.png">
<br>

    Then click on create new keypair
<br>    
<img src="./images/ec5.png">

<br>

    write pair name
    
<img src="./images/ec7.png"> 

    Then click to create and mydata.pem file download in computer.Put this file to this project folder.
 

<img src="./images/ec9.png">
<img src="./images/ec10.png">

    click to launch instance and instance create
<br>

    Click to connect to your instance --> connect to instance

<img src="./images/ec11.png">

<img src="./images/ec12.png">

<br>

    Go to vscode and do as in pic

<img src="./images/ec18.png">
<img src="./images/ec19.png">

<br>

<img src="./images/ec13.png">
<br>

    go to security then  inbound rules go to security groups and click link 

<img src="./images/ec14.png">

<br>

    Then click on edit inbound rules
   
    
<img src="./images/ec15.png">   

<img src="./images/ec16.png">   

<br>
    
    Then click on Add rule and select as i do below pic and click on save rules

<img src="./images/ec17.png"> 

<br>

    Now go back vs code terminal where your instance connect
<img src="./images/ec18.png">
<img src="./images/ec19.png">

    Note: EC2 part end
<br>
   

    Now do the following the below steps (for both local and ec2 applicable)
<br>    

    If you not work on ec2 then must create virtual enviroment (only for local machine do this)
       
    sudo pip3 install python3 (if python not download)
    sudo pip3 install virtualenv
    sudo virtualenv venv
    sudo source venv/bin/activate
    deactivate (For enviroment deactivate)

<br>
    
        ---Download kafka----

    wget https://downloads.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz 
    tar -xzf kafka_2.13-3.5.0.tgz 

<br>
   
     -----------------------Install java ---------
    sudo apt update
    sudo apt install default-jre      or sudo yum install java-1.8.0-openjdk
    java -version 

<br>

        ---Now Start Work on Kafka--------

    cd kafka_2.13-3.5.0/ 

<br>

        Start Zoo-keeper:
    -------------------------------
    bin/zookeeper-server-start.sh config/zookeeper.properties

    -- Open another windowor terminal to start kafka
    

    Start Kafka-server:
    ----------------------------------------
    Duplicate the session & enter in a new console --
    export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"      (if you work on ec2 then do this otherwise on local machine no need)

    -- Go to Terminal 
    cd kafka_2.13-3.5.0/  

    bin/kafka-server-start.sh config/server.properties

<br>

     -- if you work on EC2 then do following:
       It is pointing to private server , change server.properties so that it can run in public IP .
       The solution is to put public ip of ec2 is need to put .(Public IPv4 address)

<img src="./images/ec13.png">

    copy ip and put it as tell below

    --then go to terminal 
    cd kafka_2.13-3.5.0/ 

    Do a "sudo nano config/server.properties" - change ADVERTISED_LISTENERS to public ip of the EC2 instance 

    -- go to terminal

    cd kafka_2.13-3.5.0/ 
    
    -- Run kafka again
    bin/kafka-server-start.sh config/server.properties

    Note: But if you work on your own machine the above issue not come

    Create the topic:
    -----------------------------
    Duplicate the session & enter in a new console --
    cd kafka_2.13-3.5.0/  

    bin/kafka-topics.sh --create --topic demo_testing2 --bootstrap-server {localhost or Put the Public IP of your EC2 Instance:9092} --replication-factor 1 --partitions 1

    (localhost for your machine and public IP when you used ec2)

    Start Producer:
    --------------------------
    bin/kafka-console-producer.sh --topic demo_testing2 --bootstrap-server {localhost or Put the Public IP of your EC2 Instance:9092} 
    (localhost for your machine and public IP when you used ec2)

    Start Consumer:
    -------------------------
    Duplicate the session & enter in a new console --

    cd kafka_2.13-3.5.0/  

    bin/kafka-console-consumer.sh --topic demo_testing2 --bootstrap-server {localhost or Put the Public IP of your EC2 Instance:9092}
    (localhost for your machine and public IP when you used ec2)
    
    ----Then go in new console------
    Download jupyter : https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html
    pip3 install jupyterlab
    jupyter lab

    Note : When you change the console don't forget to activate virtual enviroment in local machine

    Now run producer.ipynb
    put the things accordingly as mention in comment
    then run consumer.ipynb
    put the things accordingly as mention in comment


## Now I   used Snowflake for dataware house for querry and analysis (work same as athena and glue)
    I used snowpipe here to automate the ingest.

    https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3

    Follow the above documents for the steps of configuration with aws and snowflake
    -- First i done initial steps to configure aws and snowflake

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


 
 
 




<br>

## For issue in configuration with snowflake you can watch the below video    
Video Link - https://www.youtube.com/watch?v=uX3lbOgfNgo
