library(tidycensus, quiet = TRUE)
library(tidyverse, quiet = TRUE)
library(sf)
library(lwgeom)
library(units)
library(jsonlite)
library(testit)
library(stringi)
options(tigris_use_cache = TRUE)


year <- 2018

# use Census key stored in Renviron. if you don't have one, you can sign up for
# one here: https://api.census.gov/data/key_signup.html
k <- Sys.getenv("CENSUS_API_KEY")
census_api_key(k)


### ----- Define ACS Variables to Pull---------

# Defining ACS vars we want to pull. Note these definitions change from year to
# year based on the whims of the Census folks. If you are using a year that is
# not 2018, you need to go to
# https://api.census.gov/data/2017/acs/acs5/profile/variables.html and make sure
# that the colnames align with the right ACS variables. The text descriptions of
# the variables should stay about the same from year to year so you can use that
# to identify if variable numbers have changed. Note that only the 2018
# variables have been updated and checked

census_vars_18 <- tribble(
    ~colnames, ~acs_vars,
    # All denominators used for the demographic variables
    "num_pop", "DP05_0001E",
    "num_pov_pop", "S1701_C01_001E",
    "num_hhs", "DP02_0001E",
    "num_occ_units", "DP04_0045E",
    "num_civ_pop_18_older", "DP02_0068E",
    "num_25_older", "DP02_0058E",
    "num_16_older", "DP03_0001E",
    "num_noninst_pop", "DP03_0095E",
    "num_renter_hh", "DP04_0136E",

    # Variables with percentages and numerators
    "pct_black", "DP05_0078PE",
    "num_black", "DP05_0078E",
    "pct_white", "DP05_0077PE",
    "num_white", "DP05_0077E",
    "pct_asian", "DP05_0080PE",
    "num_asian", "DP05_0080E",
    "pct_hisp", "DP05_0071PE",
    "num_hisp", "DP05_0071E",
    "pct_anai", "DP05_0079PE",
    "num_anai", "DP05_0079E",
    "pct_nh_pi", "DP05_0081PE",
    "num_nh_pi", "DP05_0081E",
    "pct_other_race", "DP05_0082PE", # Other race as measured by census (not all other races as displayed in feature)
    "num_other_race", "DP05_0082E",
    "pct_two_race", "DP05_0083PE",
    "num_two_race", "DP05_0083E",
    "pct_children", "DP05_0019PE",
    "num_children", "DP05_0019E",
    "pct_seniors", "DP05_0024PE",
    "num_seniors", "DP05_0024E",
    "pct_veterans", "DP02_0069PE", # denom = civilian pop. 18 years and over
    "num_veterans", "DP02_0069E",
    "pct_hs_or_higher", "DP02_0066PE", # denom = pop. 25 or older
    "num_hs_or_higher", "DP02_0066E",
    "pct_bach", "DP02_0064PE", # denom = population 25 or older
    "num_bach", "DP02_0064E",
    "pct_unemp", "DP03_0007PE", # denom = population 16 or older
    "num_unemp", "DP03_0007E",
    "pct_unins", "DP03_0099PE", # denom = total civilian non noninst. pop.
    "num_unins", "DP03_0099E",
    "pct_disability", "DP02_0071PE", # denom = total civilian non noninst. pop.
    "num_disability", "DP02_0071E",
    "pct_renters", "DP04_0047PE", # denom = occupied housing units (aka total households)
    "num_renters", "DP04_0047E",
    "pct_limited_eng_hh", "S1602_C04_001E", # denom = occupied housing units (aka total households)
    "num_limited_eng_hh", "S1602_C03_001E",
    "pct_under_poverty_line", "S1701_C03_001E", # denom = pop. for whom pov is calculated
    "num_pov", "S1701_C02_001E",
    # Note we define cost burdened renters as renter households who pay >35% income on rent
    "pct_cb_renter_hh", "DP04_0142PE", # denom = renter occ. housing Units
    "num_cb_renter_hh", "DP04_0142E",


    # Variables with only numerators (percentages not avaialable)
    # Note for these, we will have to compute % and moe for % ourselves
    "num_no_internet", "B28002_013E", # denom = occupied housing units (aka total households)
    "num_under_200_poverty_line", "S1701_C01_042E", # denom = pop. for whom pov is calculated
)

# Define ACS variables and baseline vars (using numerators and denominators)
var_definitions <- tribble(
    ~var, ~num, ~denom,
    "pct_white", "num_white", "num_pop",
    "pct_black", "num_black", "num_pop",
    "pct_asian", "num_asian", "num_pop",
    "pct_hisp", "num_hisp", "num_pop",
    "pct_children", "num_children", "num_pop",
    "pct_veterans", "num_veterans", "num_civ_pop_18_older",
    "pct_seniors", "num_seniors", "num_pop",
    "pct_hs_or_higher", "num_hs_or_higher", "num_25_older",
    "pct_bach", "num_bach", "num_25_older",
    "pct_unemp", "num_unemp", "num_16_older",
    "pct_unins", "num_unins", "num_noninst_pop",
    "pct_disability", "num_disability", "num_noninst_pop",
    "pct_renters", "num_renters", "num_hhs",
    "pct_under_poverty_line", "num_pov", "num_pov_pop",
    "pct_under_200_poverty_line", "num_under_200_poverty_line", "num_pov_pop",
    "pct_cb_renter_hh", "num_cb_renter_hh", "num_renter_hh",
    "pct_no_internet", "num_no_internet", "num_hhs",
    "pct_limited_eng_hh", "num_limited_eng_hh", "num_hhs",
)

baseline_vars <- tribble(
    ~var,
    "num_pop",
    "num_cb_renter_hh",
    "num_seniors",
    "num_no_internet",
    "num_under_200_poverty_line"
)


### -- Setup---------

# Create output dirs if it doesn't exist on local file systems
output_fdir <- paste0("reference-data/", year, "/")

if (!dir.exists(output_fdir)) {
    dir.create(paste0(output_fdir, "tracts_by_place/"), recursive = TRUE)
    dir.create(paste0(output_fdir, "precomputed_place_stats/"), recursive = TRUE)
}

# Based on input year, select appropriate ACS variable list. Note that only
# year = 2018 currently uses the right acs variables
if (year == 2015) {
    var_list <- census_vars_15
} else if (year == 2016) {
    var_list <- census_vars_16
} else if (year == 2017) {
    var_list <- census_vars_17
} else if (year == 2018) {
    var_list <- census_vars_18
} else {
    print("year is not one of 2015, 2016, 2017, or 2018")
    quit()
}





### ---- Helper Functions ---------

## Create helper functions to compute MOE of products and proportions based on
# ACS handbook formulas in Ch 7.
compute_moe_for_prop <- function(num, denom, num_moe, denom_moe) {
    # Function to generate MOEs for percentages given numerator, denominator and
    # other MOEs. We use this for some of the derived statistics in the Census
    # tables where only numerator/denominator are reported.
    prop <- num / denom
    inside_sqrt <- num_moe^2 - (prop^2 * denom_moe^2)


    # If inside of sqrt is negative, then we use formula for measures of
    # error for derived ratio instead of the formula for measures of error
    # for dervied percentages as laid out in p56 of
    # https://www.census.gov/content/dam/Census/library/publications/2018/acs/acs_general_handbook_2018_ch08.pdf.
    # All formulas below are pulled from that pdf
    if (is.na(num) & is.na(denom)) {
        # In some rare cases(30 / 8000 tracts in CA), both num and denom are 0,
        # & below formulas return NaN. For these rare cases, we return a MOE of 0.
        return(NA_integer_)
    } else if (is.na(inside_sqrt) & denom == 0 & num == 0) {
        return(0)
    } else if (inside_sqrt > 0) {
        result <- sqrt(num_moe^2 - (prop^2 * denom_moe^2)) * (1 / denom)
        return(result)
    } else {
        print(paste0("{{num}}", ": sqrt was negative!", inside_sqrt))
        result <- sqrt(num_moe^2 + (prop^2 * denom_moe^2)) * (1 / denom)

        return(result)
    }
}

compute_moe_for_product <- function(x1, x1_moe, x2, x2_moe) {
    # formula 9 from the census doc
    result <- sqrt((x1**2 * x2_moe**2) + (x2**2 * x1_moe**2))
    return(result)
}

safe_divide <- function(num, denom) {
    # if numerator is 0, then return 0. If not perform division. If either num
    # or denom is NA, then return 0. We have confirmed that of the 142 tracts
    # that have any NA values for our variables of interest or ther MOEs, none
    # overlap with the list of Census places with populations above 50k. To be
    # consistent and ensure no NA contagion, we manually convert all NA's to 0
    if (is.na(num) | is.na(denom)) {
        return(0)
    } else if (num == 0) {
        return(0)
    } else {
        return(num / denom)
    }
}



## Create data download/munging helper functions
generate_big_places_list <- function(yr = 2018, us_state_list) {
    # function to create df of places with population of over 50k. Need to use
    # tidycensus for population and then join with tigris provided place
    # boundaries bc sf does not yet support geometries for places
    # INPUTS:
    #     yr: integer, endyear of 5 year ACS for places boundaries and
    #     population estimates to be pulled from
    #     us_state_list: Character vectors of state abbreviations

    # get population data for all census places in US with pop > 50k
    places <- map_df(us_state_list, function(x) {
        get_acs(
            geography = "place", variables = "B01003_001", # this is constant across years while DP variable for total pop changes
            state = x, year = yr
        ) %>%
            filter(estimate >= 50000)
    })

    # tigris: get spatial data for all census places in US
    places_geo <- map(us_state_list, function(x) {
        tigris::places(
            state = x, class = "sf", year = yr,
            refresh = TRUE
        ) %>%
            select(STATEFP, PLACEFP, GEOID, NAME, NAMELSAD, geometry)
    }) %>% do.call("rbind", .)

    # Filter to places with places with pop over 50k
    places_geo <- places_geo %>%
        filter(GEOID %in% places$GEOID) %>%
        select(-NAME)

    # Get list of states for left joining
    state_abbv <- tigris::states(cb = T, year = 2018, class = "sf") %>%
        select(
            state_fips = GEOID,
            state_abbv = STUSPS
        ) %>%
        st_drop_geometry()

    # join spatial and population data for big places
    big_places <- places_geo %>%
        left_join(places, by = c("GEOID" = "GEOID")) %>%
        st_transform(4326) %>%
        rename(
            place_geoid = GEOID,
            state_fips = STATEFP
        ) %>%
        mutate(
            cleaned_name = stri_replace_last(NAMELSAD, "", regex = "city"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "CDP"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "town"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "village"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "\\(balance\\)"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "unified government"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "consolidated government"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "metropolitan government"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "metro government"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "urban county"),
            cleaned_name = stri_replace_last(cleaned_name, "", regex = "municipality"),
            cleaned_name = str_trim(cleaned_name)
        ) %>%
        left_join(state_abbv, by = "state_fips")


    return(big_places)
}

generate_tool_data_by_state <- function(state_fip, acsvars, yr = 2018, big_places_df) {
    # For a given state_fip and year, generates the tool datasets for all census
    # places within the state with population above 50k. it then saves these
    # data locally in the Github repo as CSVs and geojsons
    # INPUT:
    #     state_fip: state fip as a character
    #     acsvars: tibble with two columns: colnames (which contains human
    #       readable colum names like num_pop) and acs_vars, which contains the
    #       full acs variable name (like DP05_0001E).
    #     yr: 5 year ACS endyear to use as integer (ie 2018)
    #     big_places_df: dataframe of all census places in US with pop over 50k.
    #       this should be generated by the generate_big_places_list fxn
    # OUTPUT:
    #     None, but will write out geojsons into local file system

    ### ---- Downlaad and Clean ACS variables ---------

    acsvars_margins <- acsvars %>%
        mutate(
            colnames = paste0(colnames, "_margin", sep = ""),
            acs_vars = str_replace(acs_vars, "E", "M")
        )

    all_acsvars <- rbind(
        acsvars, acsvars_margins,
        # Add this tribble of other column names outputted by get_acs so that we can
        # rename columns later with `match`
        tribble(
            ~colnames, ~acs_vars,
            "NAME", "NAME",
            "GEOID", "GEOID",
            "geometry", "geometry"
        )
    )

    tracts <- get_acs(
        geography = "tract", variables = acsvars$acs_vars,
        state = state_fip, geometry = TRUE,
        output = "wide", year = yr
    ) %>%
        # May need to change this to st_transform(102008) depending on your version of GDAL/PROJ
        st_transform("ESRI:102008")

    # Rename from ACS variable names to our standardized names for easy use
    # across years when variable names change
    colnames(tracts) <- all_acsvars$colnames[match(names(tracts), all_acsvars$acs_vars)]


    # Data munge
    tracts_t <- tracts %>%
        mutate(
            # Add area of tract to use for city definition cutoffs
            area_tract = st_area(.),
            # split NAME column to replace long state names with abbreviations
            x = str_split(NAME, ",")
        ) %>%
        mutate(
            # Extract tract name (ie Census tract 1) from NAME column. This will
            # be used in conjunction with state_abbv and cleaned_name column in
            # place data to generate NAME column for display on frontend
            tract_name = map(x, ~ paste0(
                .x %>% pluck(1)
            ))
        ) %>%
        unnest(c(tract_name)) %>%
        select(-x) %>%
        # Need rowwise here as the compute_moe_for_prop user
        # defined functions won't work without specifying rowwise groups first
        rowwise() %>%
        # All percentages and percentage MOEs are given in whole numbers by acs.
        # To be consistent, we divide by 100 to get decimal values
        mutate(across(starts_with("pct"), ~ .x / 100)) %>%
        # If any tract values are NA, then forcibly coerce them to be 0.
        # This may seem dangerous but we have confirmed that only 143 tracts
        # across the US have NA values for any of the variables we are
        # interested in. And NONE of those tracts overlap with any of our big
        # places. To see confirmation of this, see
        # `scripts/check_problematic_na_tracts.R`
        tidylog::mutate(across((starts_with("num") | starts_with("pct")), ~ ifelse(is.na(.x), 0, .x))) %>%
        # Generate the percentage and SE for the two ACS variables don't have
        # precomputed percentages available directly from the acs
        mutate(
            pct_no_internet = safe_divide(num_no_internet, num_hhs),
            pct_under_200_poverty_line = safe_divide(num_under_200_poverty_line, num_pov_pop),
            pct_no_internet_margin = compute_moe_for_prop(
                num = num_no_internet,
                num_moe = num_no_internet_margin,
                denom = num_hhs,
                denom_moe = num_hhs_margin
            ),
            pct_under_200_poverty_line_margin = compute_moe_for_prop(
                num = num_under_200_poverty_line,
                num_moe = num_under_200_poverty_line_margin,
                denom = num_pov_pop,
                denom_moe = num_pov_pop_margin
            )
        ) %>%
        ungroup() %>%
        st_as_sf()


    # Filter %>% to only census places in the current state
    big_places_df <- big_places_df %>%
        filter(state_fips == state_fip)

    #### ----- Loop through places in state ----------
    # For every census place,
    # perform a spatial join with all tracts in the state to find out which tracts
    # should be included in the city definition. Then compute the ACS variables
    # and SE used by the tool.
    x <- big_places_df %>%
        pmap(generate_tool_data_by_place, yr = 2018, tracts_in_state = tracts_t)

    places <- big_places_df %>% select(place_geoid)
    return(places)
}

generate_tool_data_by_place <- function(geometry, STATEFP, place_geoid, cleaned_name, state_abbv, ..., yr = 2018, tracts_in_state = NA_character_) {
    # Function that iterates over big_places sf dataframe. For each row (ie
    # census place) it ,
    # spatially joins the tracts in the state to that places, and keeps the tract if at
    # least 1% of it's area is covered by the census place.
    # INPUT
    # Don't touch! These are the columns within big_places that we use in fxn
    # All unused columns are represented with the .... We use this input
    # structure so we can use this function with purrr's map functions. Only
    # input that is not a column in big_places_sf is:
    #   state_tracts = An sf dataframe of all the census tracts in the place's (df)
    #   yr:  Endyear of 5 year ACS to use for data download. Defaults to 2018 (numeric vector)


    # Explicitly change CRS to projected Albers Equal Area
    geo <- st_sfc(geometry) %>%
        st_sf() %>%
        # When working with crs and geometry cols inside pwalk/pmap, need to
        # set crs first and then transform.
        st_set_crs(4269) %>%
        # Transform to Albers Equal Area as we're doing spatial operations across
        # US
        st_transform("ESRI:102008")

    r <- st_intersection(tracts_in_state, geo) %>%
        mutate(
            area_tract_covered = st_area(.),
            percent_tract_covered = area_tract_covered / area_tract
        ) %>%
        # Only keep tracts whose area is at least 1% covered by a Place
        filter(percent_tract_covered > set_units(0.01, 1)) %>%
        pull(GEOID)

    # final result is all tracts that are at least 1% covered by place
    tracts_in_place <- tracts_in_state %>% filter(GEOID %in% r) %>%
        # transform crs to 4326 bc we're writing as geojsons
        st_transform(4326) %>%
        # rename to tract_geoid to avoid confusion with place_geoid
        rename(tract_geoid = GEOID) %>%
        # manually construct NAME column for use in frontend
        mutate(NAME = paste0(tract_name, ", ", cleaned_name, ", ", state_abbv))



    ### ---Generate citywide estimates and SE-------------------
    # The citywide estimates are for each of our demographic variables and the
    # denominators of our baseline variables

    city_stats <- pmap(var_definitions, generate_citywide_stats, df = tracts_in_place)

    # Append citywide calculations for pct all other races separately as we need to
    # combine several acs variables for this calculation
    city_stats <- tracts_in_place %>%
        summarize(
            num_pop = sum(num_pop),
            num_pop_margin = sqrt(sum(num_pop_margin**2)),
            num_all_other_races = sum(num_anai) + sum(num_nh_pi) +
                sum(num_other_race) + sum(num_two_race),
            num_all_other_races_margin = sqrt(sum(num_anai_margin**2) +
                sum(num_nh_pi_margin**2) +
                sum(num_other_race_margin**2) +
                sum(num_two_race_margin**2))
        ) %>%
        mutate(
            pct_all_other_races = safe_divide(num_all_other_races, num_pop),
            pct_all_other_races_margin = compute_moe_for_prop(
                num = num_all_other_races,
                num_moe = num_all_other_races,
                denom = num_pop,
                denom_moe = num_pop_margin
            ),
            id = 1
        ) %>%
        st_drop_geometry() %>%
        {
            append(city_stats, list(.))
        } %>%
        # convert list of dfs into one row wide df by left_joining all dfs
        reduce(dplyr::left_join, by = "id", suffix = c("", "_duplicate.y")) %>%
        select(
            # select all percentages and MOEs,
            starts_with("pct_"),
            # select numerators of baseline  variables
            num_pop,
            num_cb_renter_hh,
            num_seniors,
            num_no_internet,
            num_under_200_poverty_line,
            num_pop_margin,
            num_cb_renter_hh_margin,
            num_seniors_margin,
            num_no_internet_margin,
            num_under_200_poverty_line_margin
        )



    ### ---- Generate baseline variables for all tracts-----
    tracts_in_place_baselines <- baseline_vars %>%
        pmap(generate_baseline_props, df = tracts_in_place, city_df = city_stats) %>%
        reduce(dplyr::left_join, by = "tract_geoid")

    ### ---- Generate all_other_races columns for all tracts ----
    tracts_in_place <- tracts_in_place %>%
        rowwise() %>%
        mutate( # Note we use the raw counts  for each of the individual racial
            # group and total population to compute the SE for
            # pct_all_other_races. We could have  used the percetnage MOEs from
            # each of the individual pct_nh_pi, pct_anai, etc metrics. But we
            # do this to be consistent with Census Handbook which only provides
            # formulas for count variables.
            num_all_other_races = sum(num_anai, num_nh_pi, num_other_race, num_two_race, na.rm = T),
            num_all_other_races_margin = sqrt(sum(num_anai_margin**2, num_nh_pi_margin**2, num_other_race_margin**2, num_two_race_margin**2, na.rm = T)),
            pct_all_other_races = safe_divide(num_all_other_races, num_pop),
            pct_all_other_races_margin = compute_moe_for_prop(
                num = num_all_other_races,
                num_moe = num_all_other_races_margin,
                denom = num_pop,
                denom_moe = num_pop_margin
            )
        ) %>%
        ungroup() %>%
        st_as_sf()

    ### ---Append baseline variables to all tracts data -------

    tracts_in_place <- tracts_in_place_baselines %>%
        left_join(tracts_in_place %>%
            # Select the demographic variables as we will need this in tool for data
            # implied average calculations
            select(
                tract_geoid,
                NAME,
                starts_with("pct"),
                num_all_other_races,
                num_all_other_races_margin,
                num_pop,
                num_pop_margin
            ) %>%
            # These columsn are already captured with pct_all_other_races, so we
            # exclude for simplicity
            select(
                -contains("pct_anai"),
                -contains("pct_nh_pi"),
                -contains("pct_other_race"),
                -contains("pct_two_race")
            ),
        by = "tract_geoid"
        ) %>%
        st_as_sf()


    ### ---- Write out data locally -------

    # Define and create output directories
    tracts_output_fpath <- paste0("reference-data/", yr, "/tracts_by_place/")
    precomputed_city_stats_path <- paste0("reference-data/", yr, "/precomputed_place_stats/")

    dir.create(tracts_output_fpath, recursive = T, showWarnings = F)
    dir.create(precomputed_city_stats_path, recursive = T, showWarnings = F)

    # Write out all tracts in the Census place as a geojson
    tracts_in_place %>%
        st_write(paste0(tracts_output_fpath, place_geoid, ".geojson"),
            delete_dsn = TRUE
        )

    # Transform city stats so that every row is an ACS variable
    city_stats_long <- city_stats %>%
        select(ends_with("_margin") & starts_with("pct")) %>%
        rename_with(.fn = ~ str_replace_all(.x, "_margin", "")) %>%
        pivot_longer(
            cols = starts_with("pct"),
            names_to = "census_var",
            values_to = "city_value_sd"
        ) %>%
        left_join(
            city_stats %>%
                select(!ends_with("margin") & starts_with("pct")) %>%
                pivot_longer(
                    cols = starts_with("pct"),
                    names_to = "census_var",
                    values_to = "city_value"
                ),
            by = "census_var"
        )
    # Write out citywide demographic vars and sd
    city_stats_long %>%
        write_csv(paste0(precomputed_city_stats_path, place_geoid, ".csv"))



    return(city_stats_long %>% mutate(geoid = place_geoid))
}

generate_citywide_stats <- function(var, num, denom, df) {
    # Helper function to generate citywide stats given numerator, denomiator,
    # and df of AVS variables for census tracts
    # INPUT:
    #    var: name of percentage column as string
    #    num: numerator column as a string
    #    denom: denomerator column as a string
    #    df: sf dataframe of all census tracts in city

    num_margin <- paste0(num, "_margin")
    denom_margin <- paste0(denom, "_margin")
    var_margin <- paste0(var, "_margin")
    result <- df %>%
        # Roll up tract level data to city summaries
        summarize(
            {{ num }} := sum(.data[[num]]),
            {{ denom }} := sum(.data[[denom]]),
            {{ num_margin }} := sqrt(sum(.data[[num_margin]]**2)),
            {{ denom_margin }} := sqrt(sum(.data[[denom_margin]]**2)),
            # Create dummy id column for easy merging w/ left_join later
            id = 1
        ) %>%
        # compute margins
        mutate(
            {{ var }} := .data[[num]] / .data[[denom]],
            {{ var_margin }} := compute_moe_for_prop(
                num = .data[[num]],
                num_moe = .data[[num_margin]],
                denom = .data[[denom]],
                denom_moe = .data[[denom_margin]]
            )
        ) %>%
        st_drop_geometry()

    return(result)
}

generate_baseline_props <- function(var, df, city_df) {
    # Helper function to generate tool baseline proportions and sds given
    # numerator, denominator and df of citywide statistics
    # INPUT:
    #    var: name of column with which to make proportion. Should be a count
    #       column starting with num_
    #    df: sf dataframe of all census tracts in city
    #    city_df: One row df of citywide statistics

    prop_name <- paste0(str_replace(var, "num_", ""), "_prop")
    prop_name_sd <- paste0(prop_name, "_sd")
    var_margin <- paste0(var, "_margin")

    result <- df %>%
        mutate(
            {{ prop_name }} := .data[[var]] / city_df[[var]]
        ) %>%
        rowwise() %>%
        mutate(
            {{ prop_name_sd }} := compute_moe_for_prop(
                num = .data[[var]],
                num_moe = .data[[var_margin]],
                denom = city_df[[var]],
                denom_moe = city_df[[var_margin]]
            )
        ) %>%
        ungroup() %>%
        select(tract_geoid, {{ prop_name }}, {{ prop_name_sd }})

    return(result)
}



### ---- Generate Data----------------

# Create list of all US state fips to loop over
us <- fips_codes %>%
    filter(as.numeric(state_code) < 57) %>%
    pull(state_code) %>%
    unique()

# Sometimes tigris/the census website errors out if you don't set this option to
# TRUE. But sometimes this only works if you set this equal to FALSE. This seems
# to be a consistent problem with tigris and/or the census website.
options(tigris_use_cache = TRUE)


# Get list of states for left joining
state_abbv <- tigris::states(cb = T, year = 2018, class = "sf") %>%
    select(
        state_fips = GEOID,
        state_abbv = STUSPS
    )

## Generate list of big places
# Generate big_places dataframe from 2014-2018 ACS.
big_places <- generate_big_places_list(yr = year, us_state_list = us)



# Write out big_places geojson
st_write(big_places,
    delete_dsn = TRUE,
    paste0(
        "reference-data/", year,
        "/big_places_us_manual_join_1p_cutoff.geojson"
    )
)


# Generate data files locally for all states in US. This takes about 30 minutes
purrr::walk(us, generate_tool_data_by_state, acsvars = var_list, yr = year, big_places_df = big_places)


# Check that number of files in 'tracts_by_place' and 'precomputed_place_stats`
# is the same

num_x <- list.files(paste0("reference-data/", year, "/tracts_by_place/")) %>% length()
num_y <- list.files(paste0("reference-data/", year, "/precomputed_place_stats/")) %>% length()

assert("Number of SE files and Tract files are the same", num_x == num_y)