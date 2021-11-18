library(jsonlite)
library(tidyverse)

url <- "https://gbfs.niceridemn.com/gbfs/es/station_information.json"
df <- fromJSON(url) %>%
    pluck("data") %>%
    pluck("stations") %>%
    # These are read in by R as list columns and can't be written out to CSV
    select(
        -rental_methods,
        -eightd_station_services,
        -rental_uris
    ) %>%
    as_tibble()

df %>% write_csv("sample-data/minneapolis_bikes.csv")


# Note to use this Socrata API URL for the NOLA data as it returns ECMAScript
# formatted dates. The other URLs on NOLA's open data portal
# (https://data.nola.gov/City-Administration/311-Calls-Historic-Data-2012-2018-/3iz8-nghx)
# return date strings in non ECMAScript format and won't work with the date
# filters on the tool. This URL is based on the Socrata API CSV download url,
# but with a user added limit parameter to get all the results in one call.
# https://data.nola.gov/resource/3iz8-nghx.csv?$limit=500000