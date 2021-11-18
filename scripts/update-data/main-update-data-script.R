library(tidyverse)
library(reticulate)
library(dotenv)

# ---- Set params ----------

# Set year
year <- 2019

load_dot_env()

# Set AWS bucket and region from environment variables
stg <- Sys.getenv("Stage")
ui_equity_bucket <- paste0(Sys.getenv("equity_file_bucket"), stg)
ui_equity_infrastruture_bucket <- paste0(
    Sys.getenv("equity_infrastructure_bucket"),
    stg
)
ui_equity_bucket_region <- as.character(Sys.getenv("equity_file_bucket_region"))



# Set conda env to use, you will need to explicitly setup and name this conda
# enviornment on your local machine with the packages imported in the 03 python
# script and listed in the README. Note we named this conda env the same as the
# ui_equity_bucket env var, so you may need to change.
use_condaenv(condaenv = ui_equity_bucket)

# ---- Run Data Update scripts in order ----------

# Takes about an hour to run all three scripts, so grab a (few) coffees
source("scripts/update-data/01_download-and-clean-acs-data.R")
source("scripts/update-data/02_generate-baseline-proportions.R")
source_python("scripts/update-data/03_upload_ref_data_to_s3.py")

# Using the sourced python function from script 03, write out tool data to S3
write_out_tool_data_to_s3(year=as.character(year), 
                          output_bucket=ui_equity_infrastruture_bucket,
                          output_region=ui_equity_bucket_region)
