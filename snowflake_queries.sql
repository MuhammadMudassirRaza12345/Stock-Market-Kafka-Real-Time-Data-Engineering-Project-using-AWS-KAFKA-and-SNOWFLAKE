## Now I also used Snowflake for dataware house
    I used snowpipe here to automate the ingest.

    https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3

    Follow this documents for the steps of configuration with aws and snowflake
    First i done initial steps to configure aws and snowflake
    CREATE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::512013749955:role/mysnowflakerole'
    STORAGE_ALLOWED_LOCATIONS = ('s3://kafka-stock-market-video-mudassir/');

    DESC INTEGRATION s3_int;

    
    CREATE OR REPLACE DATABASE snowpipe_db

    
    USE SCHEMA snowpipe_db.public;

    CREATE STAGE mystage
    URL = 's3://kafka-stock-market-video-mudassir/'
    STORAGE_INTEGRATION = s3_int;

    show stages  

    CREATE OR REPLACE TABLE snowpipe_db.public.mytable(json_text VARIANT);

    create pipe snowpipe_db.public.mypipe auto_ingest=true as
    copy into snowpipe_db.public.mytable
    from @snowpipe_db.public.mystage
    file_format = (type = 'JSON');  

    show pipes  

    -- select SYSTEM$PIPE_STATUS('snowpipe_db.public.mytable');

    select * from snowpipe_db.public.mytable;



https://www.youtube.com/watch?v=uX3lbOgfNgo