library(tidyverse)
library(aws.s3)
library(dotenv)

# ---- Set params -----
load_dot_env()

# Upload to s3 bucket
stg <- Sys.getenv("Stage")
ui_equity_bucket <- paste0(Sys.getenv("equity_file_bucket"), stg)

libraries <- s3read_using(read_csv,
                          object = "sample-data/us_library_outlets.csv",
                          bucket = ui_equity_bucket) %>%
  mutate(HOURS = case_when(
    HOURS >= 0 ~ HOURS,
    # ILMS code for closed outlet
    HOURS == -3 ~ 0,
    # ILMS code for missing
    HOURS == -1 ~ NA_real_
  ))

# impute missing values with the mean
libraries <- libraries %>%
  mutate(HOURS = ifelse(!is.na(HOURS), HOURS, mean(HOURS, na.rm = TRUE)))

s3write_using(libraries,
              FUN = write_csv,
              object = "sample-data/us_library_outlets.csv",
              bucket = ui_equity_bucket,
              opts = c(acl = "public-read"))


lihtc <- s3read_using(read_csv,
                      object = "sample-data/al_lihtc.csv",
                      bucket = ui_equity_bucket) %>%
  mutate(LI_UNITS = case_when(
    !is.na(LI_UNITS) ~ LI_UNITS,
    # if total units is not null and low income units is null
    # impute with total units value
    !is.na(N_UNITS) & is.na(LI_UNITS) ~ N_UNITS,
    # otherwise impute with average number of units
    T ~ mean(LI_UNITS, na.rm = TRUE)
  ))

s3write_using(lihtc,
              FUN = write_csv,
              object = "sample-data/al_lihtc.csv",
              bucket = ui_equity_bucket,
              opts = c(acl = "public-read"))
