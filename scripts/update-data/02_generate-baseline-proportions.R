library(sf)
library(tidycensus)
library(here)
library(units)
library(testthat)
library(readxl)
library(tidyverse)

# --- Set parameters ---

# Using `year` param from main-update-script.R


# --- Helper Functions ----
safe_divide_with_zeros_unvec <- function(num, denom) {
  # if any of numerator or denominator is NA, return NA. If num and denom are
  # 0, then return 0. Else do normal division

  if (is.na(num) | is.na(denom)) {
    return(NA_real_)
  } else if (num == 0 & denom == 0) {
    return(0)
  } else {
    return(num / denom)
  }
}

safe_divide_with_zeros <- Vectorize(safe_divide_with_zeros_unvec)

# Define func to write out tool data
write_out_tool_data <- function(df,
                                geoid_col,
                                year,
                                geo,
                                folder,
                                is_geojson = TRUE) {
  # Function that writes out tool data with a separate file for
  # each unique geoid in the dataset
  #
  # INPUTS:
  #   df (dataframe): dataframe tool data
  #   geoid_col (column object): column containing geoid information
  #   year (int): year of data
  #   geo (str): name of geography, national, state, county, or city
  #   folder (str): name of folder within year/geo/ folder
  #   is_geojson (bool): whether file should be geojson (TRUE) or csv (FALSE)
  # RETURNS:
  #   no explict return but writes file
  
  unique_geoids <- unique(df %>% pull({{ geoid_col }}))
  dir.create(here(str_glue("reference-data/{year}/{geo}/")),
             showWarnings = FALSE
  )
  dir.create(here(str_glue("reference-data/{year}/{geo}/{folder}/")),
             showWarnings = FALSE
  )
  
  
  
  write_out <- function(df, filepath, is_geojson) {
    
    # writes out file for unique geoid
    # 
    # INPUTS:
    #   df (dataframe): dataframe of data for single geoid
    #   filepath (str): filepath to write data
    #   is_geojson (bool): whether file should be geojson (TRUE) or csv (FALSE)
    # RETURNS:
    #   no explicit return, writes out file to provided filepath
    
    if (is_geojson) {
      st_write(df,
               str_glue("{filepath}.geojson"),
               delete_dsn = TRUE
      )
    } else {
      write_csv(df, str_glue("{filepath}.csv"))
    }
  }
  
  # write out file for each unique geoid in geoid_col
  result = map(
    unique_geoids,
    ~ df %>%
      filter({{ geoid_col }} == .x) %>%
      write_out(
        filepath = str_glue("reference-data/{year}/{geo}/{folder}/{.x}"),
        is_geojson
      )
  )
  
}

# Define helper function to generate precomputed stats for a given geography
create_precomputed_stats <- function(geo_data_raw, tract_pct_vars){
  # Function to calculate precomputed demographic statistics for a given geographic dataset
  # INPUTS:
  #   geo_data_raw (sf_dataframe): dataframe of ACS data for given geography
  #   tract_pct_vars (vector of strings): vector of variable names
  # RETURNS:
  #   geo_data_precomputed (dataframe): precomputed statistics for geography
  
  geo_data_precomputed <- geo_data_raw %>%
    select(tract_pct_vars, paste0(tract_pct_vars, "_margin"), GEOID, disp_name, disp_name_abbv) 
  
  geo_data_precomputed_margins <- geo_data_precomputed %>%
    select(contains("_margin"), GEOID) %>%
    pivot_longer(cols = contains("_margin"), names_to = "var", values_to = "value_margin") %>%
    mutate(var = str_remove_all(var, "_margin"))
  
  geo_data_precomputed_means <- geo_data_precomputed %>%
    select(!contains("_margin"), GEOID, disp_name, disp_name_abbv) %>%
    pivot_longer(cols = -c("GEOID", "disp_name", "disp_name_abbv"), 
                 names_to = "var", values_to = "value")
  
  geo_data_precomputed <- geo_data_precomputed_means %>%
    left_join(geo_data_precomputed_margins, by = c("var", "GEOID")) 
  
  return(geo_data_precomputed)
}


# --- Define functions to generate baseline variables and MOEs  ----

generate_baseline_prop_variables_and_moes_us <- function(baseline_var, df_base, df_summary) {
  # Function to generate baseline variable proportions and MOEs for a given baseline variable
  # and numerator geography (df_base) when the denominator geography (df_summary) is the US
  # INPUTS:
  #   baseline_var (str): name of baseline variable
  #   df_base (sf dataframe): ACS data for numerator geography
  #   df_summary (sf dataframe): ACS data for denominator geography (US)
  # RETURNS:
  #   df_out: baseline variable proportions and margins of error for numerator geographies 
  #     where proportion is the proportion of the denominator geography's baseline pop
  #     that falls in the numerator geography
  
  prop_var <- str_replace_all(baseline_var, "num_", "prop_")
  
  baseline_var_margin <- paste0(baseline_var, "_margin")
  prop_var_margin <- paste0(prop_var, "_margin")
  
  baseline_var_num <- df_base %>% pull(baseline_var)
  baseline_var_num_margin <- df_base %>% pull(baseline_var_margin)
  
  baseline_var_denom <- df_summary %>% pull(baseline_var)
  baseline_var_denom_margin <- df_summary %>% pull(baseline_var_margin)
  
  df_base %>%
    mutate(
      {{ prop_var }} := safe_divide_with_zeros(baseline_var_num, baseline_var_denom),
      {{ prop_var_margin }} := moe_prop(
        num = baseline_var_num,
        denom = rep_len(baseline_var_denom, nrow(df_base)),
        moe_num = baseline_var_num_margin,
        moe_denom = rep_len(baseline_var_denom_margin, nrow(df_base))
      )
    ) %>%
    select(GEOID, prop_var, prop_var_margin) %>% 
    as_tibble()  %>% 
    mutate(national_geoid = "1")
}

generate_baseline_prop_variables_and_moes <- function(baseline_var, 
                                                      df_base, 
                                                      df_summary, 
                                                      is_city) {
  # Function to generate baseline variable proportions and MOEs for a given baseline variable
  # and numerator geography (df_base) and denominator geographies (df_summary) for all of the 
  # unique denominator geographies in df_summary (e.g for all states if df_summary is 
  # state-level data)
  # INPUTS:
  #   baseline_var (str): name of baseline variable
  #   df_base (sf dataframe): ACS data for numerator geography
  #   df_summary (sf dataframe): ACS data for denominator geography
  #   is_city (boolean): boolean that indicates whether df_summary is city for handling
  #     denom_geoid field correctly
  # RETURNS:
  #   df_out: baseline variable proportions and margins of error for numerator geographies 
  #     where proportion is the proportion of the denominator geography's baseline pop
  #     that falls in the numerator geography
  
  
  len_geoid <- df_summary %>%
    select(GEOID) %>%
    slice(1) %>%
    pull(GEOID) %>%
    nchar()
  
  
  prop_var <- str_replace_all(baseline_var, "num_", "prop_")
  
  baseline_var_margin <- paste0(baseline_var, "_margin")
  prop_var_margin <- paste0(prop_var, "_margin")
  
  if (is_city == FALSE){
    baseline_var_num <- df_base %>% 
      select(baseline_var, baseline_var_margin, GEOID) %>%
      mutate(denom_geoid = substr(GEOID, 1, len_geoid)) 
    
  } else {
    baseline_var_num <- df_base %>% 
      select(baseline_var, baseline_var_margin, GEOID, denom_geoid = place_geoid)  
  }
  
  baseline_var_denom <- df_summary %>%
    select(baseline_var, baseline_var_margin, denom_geoid = GEOID)
  
  
  df_all <- baseline_var_num %>% 
    left_join(baseline_var_denom, 
              by = "denom_geoid", 
              suffix = c("_num", "_denom"))
  
  baseline_var_num_name <- paste0(baseline_var, "_num")
  baseline_var_denom_name <- paste0(baseline_var, "_denom")
  
  baseline_var_num_margin <- paste0(baseline_var_margin, "_num")
  baseline_var_denom_margin <- paste0(baseline_var_margin, "_denom")
  
  df_out <- df_all %>%
    mutate(
      {{ prop_var }} := safe_divide_with_zeros( .data[[baseline_var_num_name]], 
                                                .data[[baseline_var_denom_name]]),
      {{ prop_var_margin }} := moe_prop(
        num = .data[[baseline_var_num_name]],
        denom = .data[[baseline_var_denom_name]],
        moe_num = .data[[baseline_var_num_margin]],
        moe_denom = .data[[baseline_var_denom_margin]]
      )
    ) %>%
    select(GEOID, denom_geoid, prop_var, prop_var_margin) %>%
    as_tibble()
}

# --- Define tool variables ----

# Set baseline variables for the geo disprity map, which we use to calculate
# proportions at the appropriate geography
baseline_vars <- c(
  "num_pop",
  "num_cb_renter_hh",
  "num_seniors",
  "num_no_internet",
  "num_under_200_poverty_line",
  "num_children",
  "num_pov"
)

# Set tract level variables we use for the demo disparity chart.
tract_pct_vars <- c(
  # Total Population variables
  "pct_black",
  "pct_white",
  "pct_asian",
  "pct_hisp",
  "pct_all_other_races",
  "pct_children",
  "pct_seniors",
  "pct_veterans",
  "pct_no_internet",
  "pct_unins",
  "pct_disability",
  "pct_renters",
  "pct_limited_eng_hh",
  "pct_bach",
  "pct_under_poverty_line",
  "pct_under_200_poverty_line",
  "pct_cb_renter_hh",
  "pct_unemp",
  "pct_less_hs_diploma",

  # Under 18 Popluation variables
  "pct_under18_unins",
  "pct_under18_disability",
  "pct_under18_pov",
  "pct_under18_limited_eng_hh",
  "pct_under18_white_alone",
  "pct_under18_black_alone",
  "pct_under18_asian_alone",
  "pct_under18_hisp",
  "pct_under18_all_other_races_alone",

  # Poverty Population variables
  "pct_pov_children",
  "pct_pov_seniors",
  "pct_pov_white_alone",
  "pct_pov_black_alone",
  "pct_pov_asian_alone",
  "pct_pov_hisp",
  "pct_pov_all_other_races_alone",
  "pct_pov_bach",
  "pct_pov_less_than_hs",
  "pct_pov_unemployed",
  "pct_pov_veterans",
  "pct_pov_disability",
  "pct_pov_unins"
)

# Create variable definitions for percentages which are not directly reported by
# the ACS and we need to calculate ourselves (along with associated MOEs)
prop_definitions_df <- tribble(
  ~var, ~num, ~denom,
  # For Total Population ACS variables
  "pct_no_internet", "num_no_internet", "num_hhs",
  "pct_under_200_poverty_line", "num_under_200_poverty_line", "num_pov_calc_pop",
  "pct_all_other_races", "num_all_other_races", "num_pop",
  "pct_less_hs_diploma", "num_less_than_hs", "num_25_older",
  
  # For Population Under 18  ACS variables
  "pct_under18_unins", "num_under19_unins", "num_under19_noninst",
  "pct_under18_pov", "num_under18_pov", "num_under18_pov_pop",
  "pct_under18_disability", "num_under18_disability", "num_under18_noninst",
  "pct_under18_white_alone", "num_under18_white_alone", "num_children",
  "pct_under18_black_alone", "num_under18_black_alone", "num_children",
  "pct_under18_asian_alone", "num_under18_asian_alone", "num_children",
  "pct_under18_hisp", "num_under18_hisp", "num_children",
  "pct_under18_all_other_races_alone", "num_under18_all_other_races_alone", "num_children",
  "pct_under18_limited_eng_hh", "num_5_17_limited_eng_hh", "num_5_17_in_hh",
  
  
  # For Population in Poverty  ACS variables
  "pct_pov_children", "num_pov_children", "num_pov",
  "pct_pov_seniors", "num_pov_over65", "num_pov",
  "pct_pov_white_alone", "num_pov_white_alone", "num_pov",
  "pct_pov_black_alone", "num_pov_black_alone", "num_pov",
  "pct_pov_asian_alone", "num_pov_asian_alone", "num_pov",
  "pct_pov_hisp", "num_pov_hisp", "num_pov",
  "pct_pov_bach", "num_pov_bach", "num_pov_over_25",
  "pct_pov_less_than_hs", "num_pov_less_than_hs", "num_pov_over_25",
  "pct_pov_unemployed", "num_pov_unemployed", "num_pov_civ_labor_force",
  "pct_pov_veterans", "num_pov_veterans", "num_pov_civ_18_older",
  "pct_pov_disability", "num_pov_disability", "num_pov_noninst_pop",
  "pct_pov_unins", "num_pov_unins", "num_pov_noninst_pop",
  "pct_pov_all_other_races_alone", "num_pov_all_other_races_alone", "num_pov",
)


# for city level analysis, we need to calculate all proportions,
# including those directly reported for other geographies or
# calculated in the 01 script
addl_prop_definitions_city <- tribble(
  ~var, ~num, ~denom,
  # For Total Population ACS variables
  "pct_black", "num_black", "num_pop",
  "pct_white", "num_white", "num_pop",
  "pct_asian", "num_asian", "num_pop",
  "pct_hisp", "num_hisp", "num_pop",
  "pct_children", "num_children", "num_pop",
  "pct_seniors", "num_seniors", "num_pop",
  "pct_veterans", "num_veterans", "num_civ_pop_18_older",
  "pct_unins",  "num_unins", "num_noninst_pop",
  "pct_disability", "num_disability", "num_noninst_pop",
  "pct_renters", "num_renters", "num_hhs",
  "pct_limited_eng_hh", "num_limited_eng_hh", "num_hhs",
  "pct_bach", "num_bach", "num_25_older",
  "pct_under_poverty_line", "num_pov", "num_pov_calc_pop",
  "pct_cb_renter_hh", "num_cb_renter_hh", "num_renter_hh",
  "pct_unemp", "num_unemp", "num_civ_pop_in_labor_force"
)

# --- Read in geography data and cleanup ----

# get state fips, codes, and names for left_join 
state_abbv <- fips_codes %>%
  select(state, state_code, state_name) %>%
  unique()

# read in raw data and create display names for geographic and demographic bias charts
tract_data_raw_all <- st_read(str_glue("reference-data/{year}/clean-acs-data/tract.geojson")) %>%
  mutate(state_code = substr(GEOID, 1, 2)) %>%
  left_join(state_abbv, by = "state_code") %>%
  separate(NAME, sep = ", ", into = c("tract", "county", NA)) %>%
  # disp_name is the full tract name (ie "Census Tract 001")
  rename(disp_name = tract) 

tract_data_raw <- tract_data_raw_all %>%
  select(baseline_vars,
         paste0(baseline_vars, "_margin"),
         GEOID,
         tract_pct_vars,
         paste0(tract_pct_vars, "_margin"),
         disp_name,
         state_code
  ) %>%
  mutate(disp_name_abbv = "")

us_data_raw <- st_read(str_glue("reference-data/{year}/clean-acs-data/us.geojson")) %>%
  mutate(disp_name = "US",
         disp_name_abbv = "US") %>%
  select(baseline_vars, paste0(baseline_vars, "_margin"), 
         tract_pct_vars,
         paste0(tract_pct_vars, "_margin"),
         GEOID, 
         disp_name,
         disp_name_abbv) 
  
state_data_raw <- st_read(str_glue("reference-data/{year}/clean-acs-data/state.geojson")) %>%
  mutate(disp_name = NAME) %>%
  left_join(state_abbv, by = c("disp_name" = "state_name")) %>%
  rename("disp_name_abbv" = "state") %>%
  select(baseline_vars, paste0(baseline_vars, "_margin"), 
         GEOID,
         tract_pct_vars,
         paste0(tract_pct_vars, "_margin"),
         disp_name,
         disp_name_abbv) 

county_data_raw <- st_read(str_glue("reference-data/{year}/clean-acs-data/county.geojson")) %>%
  mutate(county_name = gsub(",.*$", "", NAME),
         state_code = substr(GEOID, 1, 2)) %>%
  left_join(state_abbv, by = "state_code") %>%
  # Display name is county name, state abbreviation i.e. Cook County, IL
  unite("disp_name", c("county_name", "state"), sep = ", ", remove = FALSE) %>%
  select(baseline_vars, paste0(baseline_vars, "_margin"), 
         GEOID,
         tract_pct_vars,
         paste0(tract_pct_vars, "_margin"),
         disp_name,
         disp_name_abbv = county_name) 


# Drop geometry for faster computation of some statistics
tract_data <- tract_data_raw %>%
  st_drop_geometry()

us_data <- us_data_raw %>%
  st_drop_geometry()

state_data <- state_data_raw %>%
  st_drop_geometry()

county_data <- county_data_raw %>%
  st_drop_geometry()

# --- Generate baseline proportions for each geographic level (US, State, County) ----

## National Level ##
us_state_data_props <- map(baseline_vars,
                        generate_baseline_prop_variables_and_moes_us,
                        df_base = state_data,
                        df_summary = us_data
) %>%
  reduce(left_join, by = c("GEOID", "national_geoid"))

# Create crosswalk between states and regions for demographic chart dropdown menu 
# on national tool
download.file(url = "https://www2.census.gov/programs-surveys/popest/geographies/2014/state-geocodes-v2014.xls",
              destfile = here(str_glue("reference-data/{year}"), "state_region_crosswalk.xls"),
              mode = "wb")

state_region_crosswalk <- read_excel(here(str_glue("reference-data/{year}"), "state_region_crosswalk.xls"),
                                     skip = 6,
                                     col_names = c("region", "division", "state", "name"))

region_name_code <- state_region_crosswalk %>% 
  filter(!duplicated(region)) %>%
  select(region, name) %>%
  rename(region_name = name)

state_region_crosswalk <- state_region_crosswalk %>%
  filter(state != "00") %>%
  left_join(region_name_code, by = "region") %>%
  rename(disp_name = name) %>%
  select(disp_name, region_name)

us_state_data_full <- state_data_raw %>%
  left_join(us_state_data_props, by = "GEOID") %>%
  left_join(state_region_crosswalk, by = "disp_name")

us_tract_data_props <- map(baseline_vars,
                           generate_baseline_prop_variables_and_moes_us,
                           df_base = tract_data,
                           df_summary = us_data
) %>%
  reduce(left_join, by = c("GEOID", "national_geoid"))


us_tract_data_full <- tract_data_raw %>%
  left_join(us_tract_data_props, by = "GEOID")

## State Level ##
state_county_data_props <- map(baseline_vars,
                        generate_baseline_prop_variables_and_moes,
                        df_base = county_data,
                        df_summary = state_data,
                        is_city = FALSE
) %>%
  reduce(left_join, by = c("GEOID", "denom_geoid"))


state_county_data_full <- county_data_raw %>%
  left_join(state_county_data_props, by = "GEOID") 


state_tract_data_props <- map(baseline_vars,
                               generate_baseline_prop_variables_and_moes,
                               df_base = tract_data,
                               df_summary = state_data,
                              is_city = FALSE
) %>%
  reduce(left_join, by = c("GEOID", "denom_geoid"))


state_tract_data_full <- tract_data_raw %>%
  left_join(state_tract_data_props, by = "GEOID") 

## County Level ##
county_tract_data_props <- map(baseline_vars,
                         generate_baseline_prop_variables_and_moes,
                         df_base = tract_data,
                         df_summary = county_data,
                         is_city = FALSE
) %>%
  reduce(left_join, by = c("GEOID", "denom_geoid"))


county_tract_data_full <- tract_data_raw %>%
  left_join(county_tract_data_props, by = "GEOID") 


# --- Test that proportions add up to 1 ----

check_that_all_prop_values_sum_to_one = function(df_props, geoid_col) {
  # df_props = us_tract_data_props
  non_zero_sums = df_props %>%
    group_by({{geoid_col}})  %>%
    # group_by(national_geoid) %>%
    select(starts_with("prop") & !ends_with("margin")) %>%
    summarize(across(.cols = everything(), .fns = sum)) %>%
    ungroup() %>%
    pivot_longer(cols = -c({{ geoid_col }})) %>%
    # pivot_longer(cols = -c(national_geoid)) %>%
    # Add arbitarily small scale bc a few times values are very close to but not
    # exactly 1
    # Filter out value == 0, bc some counties have 0 CB renter HH and they
    # all have have values of 0 for their prop values. 
    filter((value > 0 & value < 0.9999999999999) | value > 1.000000000000001)
  
  num_non_zero_sums <- non_zero_sums %>%
    arrange(value)  %>% 
    nrow()

  test_that(
    "All proportions add up to 1",
    expect_equal(
      num_non_zero_sums,
      0
    )
  )
}


check_that_all_prop_values_sum_to_one(us_state_data_props, national_geoid)
check_that_all_prop_values_sum_to_one(us_tract_data_props, national_geoid)
check_that_all_prop_values_sum_to_one(state_county_data_props, denom_geoid)
check_that_all_prop_values_sum_to_one(state_tract_data_props, denom_geoid)
check_that_all_prop_values_sum_to_one(county_tract_data_props, denom_geoid)

# --- Generate precomputed demographic stats for each geographic level (US, State, County) ----


# Generate precomputed demographic statistics for US, State, County

us_data_precomputed <- create_precomputed_stats(us_data, 
                                                tract_pct_vars)
state_data_precomputed <- create_precomputed_stats(state_data, 
                                                   tract_pct_vars)
county_data_precomputed <- create_precomputed_stats(county_data, 
                                                    tract_pct_vars)


# For the frontend sentences to make sense, we replace District of Columbia 
# with "the District of Columbia" in the state and county precomputed data, 
# which
state_data_precomputed = state_data_precomputed %>% 
  tidylog::mutate(disp_name = if_else(
    disp_name == "District of Columbia",
    "the District of Columbia",
    disp_name
  ))

county_data_precomputed = county_data_precomputed %>% 
  tidylog::mutate(disp_name = if_else(
    disp_name == "District of Columbia, DC",
    "the District of Columbia, DC",
    disp_name
  ))


# --- Write out all data ----

# Writeout national level and state level precomputed data, natl data and tract data, will use geoid of 1
write_out_tool_data(us_data_precomputed,
                    geoid_col = GEOID,
                    geo = "national",
                    folder = "precomputed_stats",
                    is_geojson = FALSE,
                    year = year 
                    )

write_out_tool_data(state_data_precomputed,
                    geoid_col = GEOID,
                    geo = "state",
                    folder = "precomputed_stats",
                    is_geojson = FALSE,
                    year = year)

write_out_tool_data(county_data_precomputed,
                    geoid_col = GEOID,
                    geo = "county",
                    folder = "precomputed_stats",
                    is_geojson = FALSE,
                    year = year)


write_out_tool_data(us_state_data_full,
                    geoid_col = national_geoid,
                    geo = "national",
                    folder = "state",
                    is_geojson = TRUE,
                    year = year
                    )

write_out_tool_data(us_tract_data_full,
                    geoid_col = national_geoid,
                    geo = "national",
                    folder = "tract",
                    is_geojson = TRUE,
                    year = year
)

write_out_tool_data(state_county_data_full,
                    geoid_col = denom_geoid,
                    geo = "state",
                    folder = "county",
                    is_geojson = TRUE,
                    year = year
                    )

write_out_tool_data(state_tract_data_full,
                    geoid_col = denom_geoid,
                    geo = "state",
                    folder = "tract",
                    is_geojson = TRUE,
                    year = year
)

write_out_tool_data(county_tract_data_full,
                    geoid_col = denom_geoid,
                    geo = "county",
                    folder = "tract",
                    is_geojson = TRUE,
                    year = year
                    )


# --- Generate baselines and precomputed stats for places for city tool ----

big_places <- st_read(str_glue("reference-data/{year}/city/all_polygons.geojson")) %>%
  rename(place_geoid = GEOID, disp_name = cleaned_name)

generate_citywide_stats <- function(var, num, denom, df) {
  # Helper function to generate citywide stats given numerator, denomiator,
  # and df of ACS variables for census tracts for one or more places. We define
  # cities as colletions of census tracts (With area cutoffs) so we need to
  # generate citywide statistics 
  # INPUT:
  #    var: name of percentage column as string
  #    num: numerator column as a string
  #    denom: denomerator column as a string
  #    df: sf dataframe of all census tracts in city
  # RETURNS:
  #   result (dataframe): dataframe of citywide statistics
  
  num_margin <- paste0(num, "_margin")
  denom_margin <- paste0(denom, "_margin")
  var_margin <- paste0(var, "_margin")
  result <- df %>%
    # Roll up tract level data to city summaries
    group_by(place_geoid) %>%
    summarise(
      {{ num }} := sum(.data[[num]]),
      {{ denom }} := sum(.data[[denom]]),
      {{ num_margin }} := sqrt(sum(.data[[num_margin]]**2)),
      {{ denom_margin }} := sqrt(sum(.data[[denom_margin]]**2))
    ) %>%
    # compute margins
    mutate(
      {{ var }} := round( .data[[num]] / .data[[denom]] * 100, 3),
      {{ var_margin }} := ifelse(
        # When num and denom are 0, the default ACS MOE formulas will
        # return NA. Only happens in  ~1628 tracts in the worst case
        # and doesn't happen for any of the map/geo baseline vars. We
        # decide to use an MOE of 0 in this case
        (.data[[num]] == 0 & .data[[denom]] == 0),
        0,
        round(tidycensus::moe_prop(
          num = .data[[num]],
          moe_num = .data[[num_margin]],
          denom = .data[[denom]],
          moe_denom = .data[[denom_margin]]
        ) * 100, 3)
      )
    )

  return(result)
}

generate_citywide_sums <- function(var, df) {
  # Function to calculate the citywide sum for a given variable provided a 
  # dataframe of ACS data at the tract level for one or more places
  # INPUTS:
  #   var (str): name of ACS variable
  #   df (dataframe): dataframe of tract-level ACS data for one or more places
  # RETURNS:
  #   result (dataframe): dataframe with estimate and margin of error for 
  #     citywide sum of provided variable
  
  var_margin <- paste0(var, "_margin")
  
  result <- df %>%
    group_by(place_geoid) %>%
    summarise(
      estimate := sum(.data[[var]]),
      moe =
        tidycensus::moe_sum(
          moe = .data[[var_margin]],
          estimate = .data[[var]]
        )
      ) %>%
    rename(
      {{ var }} := estimate,
      {{ var_margin }} := moe
    )
  
  return(result)
}

generate_city_data_by_state <- function(tract_df, 
                                        places_df, 
                                        year, 
                                        prop_defs,
                                        baseline_vars){
  # Function to generate the city-level data for the tool for all
  # places in a given state
  # INPUTS:
  #   tract_df (dataframe): dataframe of tract-level ACS data for a state
  #   places_df (sf dataframe): dataframe of place boundaries for all places
  #     of 50,000 or more in a state
  #   year (int): year associated with final year of 5-year ACS
  #   prop_defs (dataframe): dataframe providing numerator and denominator variable
  #     names for proportions to be calculated
  #   baseline vars (vector of strings): names of baseline variables 
  # RETURNS:
  #   no explicit return, writes precomputed stats and baseline proportions for 
  #     every place in the state
  
  tracts_t <- tract_df %>%
    # When working with crs and geometry cols inside pwalk/pmap, need to
    # set crs first and then transform.
    st_set_crs(4269) %>%
    # Transform to Albers Equal Area as we're doing spatial operations across
    # US
    st_transform("ESRI:102008") %>%
    mutate(
      # Add area of tract to use for city definition cutoffs
      area_tract = st_area(.)) 
  
  geo <- places_df %>%
    # When working with crs and geometry cols inside pwalk/pmap, need to
    # set crs first and then transform.
    st_set_crs(4269) %>%
    # Transform to Albers Equal Area as we're doing spatial operations across
    # US
    st_transform("ESRI:102008")
  
  # capture display_name to join with precomputed stats
  place_name <- geo %>%
    st_drop_geometry() %>%
    select(place_geoid, disp_name) %>%
    mutate(disp_name_abbv = "")
  
  # spatially join place boundaries and tracts
  tract_data_city <- st_intersection(tracts_t, st_make_valid(geo)) %>%
    mutate(
      area_tract_covered = st_area(.),
      percent_tract_covered = area_tract_covered / area_tract
    ) %>%
    # Only keep tracts whose area is at least 1% covered by a Place
    filter(percent_tract_covered > set_units(0.01, 1)) %>%
    st_drop_geometry()
  
  # calculate citywide proportions for demographic disparity chart
  city_stats <- pmap(prop_defs, generate_citywide_stats, df = tract_data_city) %>%
    reduce(dplyr::left_join, by = c("place_geoid"))
  
  # calculate citywide totals and margins of error for denominators for baseline vars
  city_sums <- map(baseline_vars, 
                       generate_citywide_sums, 
                       df = tract_data_city) %>%
    reduce(dplyr::left_join, by = "place_geoid") %>%
    rename("GEOID" = "place_geoid")
  
  tract_pct_vars <- prop_defs$var
  city_data_precomputed <- create_precomputed_stats(city_stats %>%
                                                      left_join(place_name, by = "place_geoid") %>%
                                                      rename(GEOID = place_geoid), 
                                                    tract_pct_vars) 
  # write out precomputed stats
  write_out_tool_data(city_data_precomputed,
                      geoid_col = GEOID,
                      geo = "city",
                      folder = "precomputed_stats",
                      is_geojson = FALSE,
                      year = year)
  
  # calculate tract-level proportions for geographic disparity baselines
  city_tract_data_props <- map(baseline_vars,
                               generate_baseline_prop_variables_and_moes,
                               df_base = tract_data_city,
                               df_summary = city_sums,
                               is_city = TRUE) %>%
    reduce(left_join, by = c("GEOID", "denom_geoid"))

  city_tract_data_full <- tract_df %>%
    select(baseline_vars,
           paste0(baseline_vars, "_margin"),
           GEOID,
           tract_pct_vars,
           paste0(tract_pct_vars, "_margin"),
           disp_name) %>%
    right_join(city_tract_data_props, by = "GEOID") 
  
  write_out_tool_data(city_tract_data_full,
                      geoid_col = denom_geoid,
                      geo = "city",
                      folder = "tract",
                      is_geojson = TRUE,
                      year = year
  )
}


generate_city_data <- function(fips_code, 
                               all_places, 
                               all_tracts, 
                               year, 
                               prop_defs,
                               baseline_vars){
  
  # Function to create city-level data for all places with population
  # greater than 50,000 in the US
  # INPUTS:
  #   fips code (str): two-digit fips code for state
  #   all_places (sf dataframe): dataframe of all places with population greater
  #     greater than 50,000 in the US
  #   all_tracts (sf dataframe): dataframe of ACS data for all tracts in the US
  #   year (int): year associated with end of 5-year ACS data
  #   prop_defs (dataframe): dataframe providing numerator and denominator variable
  #     names for proportions to be calculated
  #   baseline vars (vector of strings): names of baseline variables
  # RETURNS:
  #   no explicit return, writes precomputed stats and baseline proportions for
  #     every place with population greater than 50,000 in the US

  places_df <- all_places %>%
    filter(state_fips == fips_code) %>%
    # Explicitly transform to EPSG 4269
    st_transform("EPSG:4269")
  tract_df <- all_tracts %>%
    filter(state_code == fips_code) %>%
    # Explicitly transform to EPSG 4269
    st_transform("EPSG:4269")
  
  generate_city_data_by_state(tract_df, 
                              places_df,
                              year = year, 
                              prop_defs = prop_defs,
                              baseline_vars = baseline_vars)
}

# get list of fips state codes to iterate over
state_fips <- fips_codes %>%
  filter(as.numeric(state_code) < 57) %>%
  mutate(state_code = as.character(state_code)) %>%
  pull(state_code) %>%
  unique()

# Remove DC because we use county-level DC data because the accuracy is greater
state_fips <- state_fips[state_fips != "11"]

prop_defs_all <- rbind(prop_definitions_df, addl_prop_definitions_city) 

map(state_fips,
    generate_city_data,
    all_places = big_places,
    all_tracts = tract_data_raw_all,
    prop_defs = prop_defs_all,
    baseline_vars = baseline_vars,
    year = year)


# Copy DC County Level Files to City Folder as the county files have lower MOEs
# as they are census reported instead of aggregated from tracts manually
file.copy(str_glue("reference-data/{year}/county/tract/11001.geojson"),
          str_glue("reference-data/{year}/city/tract/1150000.geojson"),
          overwrite = T)

file.copy(str_glue("reference-data/{year}/county/precomputed_stats/11001.csv"),
          str_glue("reference-data/{year}/city/precomputed_stats/1150000.csv"),
          overwrite = T)

# For the precomputed DC City Level files, we need to overwrite disp_name so 
# that the frontend can display the correct city level name. We decided on these
# names after dicussing with Comms
dc_city_precomputed_stats = read_csv(
    str_glue("reference-data/{year}/city/precomputed_stats/1150000.csv")
  ) %>% 
  tidylog::mutate(
    disp_name = if_else(
      disp_name == "the District of Columbia, DC",
      "Washington, DC",
      disp_name
    )
    )

dc_city_precomputed_stats %>% 
  write_csv(str_glue("reference-data/{year}/city/precomputed_stats/1150000.csv"))
