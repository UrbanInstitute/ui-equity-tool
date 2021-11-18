library(aws.s3)
library(tidyverse)
library(dotenv)

load_dot_env()

# Upload to s3 bucket
stg = Sys.getenv("Stage")
ui_equity_bucket <- paste0(Sys.getenv("equity_file_bucket"), stg)


s3sync(path = "sample-data/",
       prefix = "sample-data/", 
       direction = "upload",
       bucket = ui_equity_bucket,
       acl = "public-read",
       create = T)

# Set some file ID translation list to be private
put_acl(object = "sample-data/sample_dataset_file_id_transalation_list.csv",
        bucket = ui_equity_bucket, 
        acl = "private")
