library(tidycensus)
library(sf)
library(tigris)
library(tidyverse)
library(dtplyr)
library(testthat)
library(aws.s3)
library(stringi)

# --- Setup params ---

# Using `year` param from main-update-script.R

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
    "pct_less_hs_diploma",
    "pct_bach",
    "pct_under_poverty_line",
    "pct_under_200_poverty_line",
    "pct_cb_renter_hh",
    "pct_unemp",

    # Under 18 Popluation variables
    "pct_under18_unins",
    "pct_under18_disability",
    "pct_under18_pov",
    "pct_under18_limited_eng_hh",
    "pct_under18_white_alone",
    "pct_under18_black_alone",
    "pct_under18_asian_alone",
    "pct_under18_white",
    "pct_under18_hisp",
    "pct_under18_all_other_races_alone",

    # Poverty Population variables
    "pct_pov_children",
    "pct_pov_seniors",
    "pct_pov_white_alone",
    "pct_pov_black_alone",
    "pct_pov_asian_alone",
    "pct_pov_hisp",
    "pct_pov_white",
    "pct_pov_all_other_races_alone",
    "pct_pov_bach",
    "pct_pov_less_than_hs",
    "pct_pov_unemployed",
    "pct_pov_veterans",
    "pct_pov_disability",
    "pct_pov_unins"
)

# Create variable definitions for vars we need to manually add together
add_definitions_df <- tribble(
    ~var, ~vars_to_add,
    # For Population Under 18 ACS variables
    "num_under18_disability", c("num_under18_one_disability", "num_under18_two_or_more_disability"),
    "num_under18_pov", c("num_under18_pov_under_6", "num_under18_pov_6_11", "num_under18_pov_12_17"),
    "num_under18_white_alone", c("num_under5_white_alone_male", "num_5_9_white_alone_male", "num_10_14_white_alone_male", "num_15_17_white_alone_male", "num_under5_white_alone_female", "num_5_9_white_alone_female", "num_10_14_white_alone_female", "num_15_17_white_alone_female"),
    "num_under18_black_alone", c("num_under5_black_alone_male", "num_5_9_black_alone_male", "num_10_14_black_alone_male", "num_15_17_black_alone_male", "num_under5_black_alone_female", "num_5_9_black_alone_female", "num_10_14_black_alone_female", "num_15_17_black_alone_female"),
    "num_under18_anai_alone", c("num_under5_anai_alone_male", "num_5_9_anai_alone_male", "num_10_14_anai_alone_male", "num_15_17_anai_alone_male", "num_under5_anai_alone_female", "num_5_9_anai_alone_female", "num_10_14_anai_alone_female", "num_15_17_anai_alone_female"),
    "num_under18_asian_alone", c("num_under5_asian_alone_male", "num_5_9_asian_alone_male", "num_10_14_asian_alone_male", "num_15_17_asian_alone_male", "num_under5_asian_alone_female", "num_5_9_asian_alone_female", "num_10_14_asian_alone_female", "num_15_17_asian_alone_female"),
    "num_under18_nhpi_alone", c("num_under5_nhpi_alone_male", "num_5_9_nhpi_alone_male", "num_10_14_nhpi_alone_male", "num_15_17_nhpi_alone_male", "num_under5_nhpi_alone_female", "num_5_9_nhpi_alone_female", "num_10_14_nhpi_alone_female", "num_15_17_nhpi_alone_female"),
    "num_under18_other_alone", c("num_under5_other_alone_male", "num_5_9_other_alone_male", "num_10_14_other_alone_male", "num_15_17_other_alone_male", "num_under5_other_alone_female", "num_5_9_other_alone_female", "num_10_14_other_alone_female", "num_15_17_other_alone_female"),
    "num_under18_two_or_more_alone", c("num_under5_two_or_more_alone_male", "num_5_9_two_or_more_alone_male", "num_10_14_two_or_more_alone_male", "num_15_17_two_or_more_alone_male", "num_under5_two_or_more_alone_female", "num_5_9_two_or_more_alone_female", "num_10_14_two_or_more_alone_female", "num_15_17_two_or_more_alone_female"),
    "num_under18_white", c("num_under5_white_male", "num_5_9_white_male", "num_10_14_white_male", "num_15_17_white_male", "num_under5_white_female", "num_5_9_white_female", "num_10_14_white_female", "num_15_17_white_female"),
    "num_under18_hisp", c("num_under5_hispanic_male", "num_5_9_hispanic_male", "num_10_14_hispanic_male", "num_15_17_hispanic_male", "num_under5_hispanic_female", "num_5_9_hispanic_female", "num_10_14_hispanic_female", "num_15_17_hispanic_female"),
    "num_under18_all_other_races_alone", c("num_under18_other_alone", "num_under18_nhpi_alone", "num_under18_anai_alone", "num_under18_two_or_more_alone"),
    "num_less_than_hs", c("num_less_than_9th_grade", "num_9th_to_12th_grade"),
    
    # For Population in Poverty ACS variables
    "num_pov_disability", c("num_pov_disability_under_18", "num_pov_disability_18_64", "num_pov_disability_65_over"),
    "num_pov_veterans", c("num_pov_veteran_18_64", "num_pov_veteran_65_over"),
    "num_pov_civ_18_older", c("num_pov_veteran_18_64", "num_pov_nonveteran_18_64", "num_pov_veteran_65_over", "num_pov_nonveteran_65_over"),
    "num_pov_all_other_races_alone", c("num_pov_other_alone", "num_pov_nhpi_alone", "num_pov_anai_alone", "num_pov_two_or_more"),

    # For Total Population
    "num_all_other_races", c("num_other", "num_anai", "num_nhpi", "num_two_or_more")
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
    "pct_under18_anai_alone", "num_under18_anai_alone", "num_children",
    "pct_under18_asian_alone", "num_under18_asian_alone", "num_children",
    "pct_under18_nhpi_alone", "num_under18_nhpi_alone", "num_children",
    "pct_under18_other_alone", "num_under18_other_alone", "num_children",
    "pct_under18_two_or_more_alone", "num_under18_two_or_more_alone", "num_children",
    "pct_under18_white", "num_under18_white", "num_children",
    "pct_under18_hisp", "num_under18_hisp", "num_children",
    "pct_under18_all_other_races_alone", "num_under18_all_other_races_alone", "num_children",
    "pct_under18_limited_eng_hh", "num_5_17_limited_eng_hh", "num_5_17_in_hh",


    # For Population in Poverty  ACS variables
    "pct_pov_children", "num_pov_children", "num_pov",
    "pct_pov_seniors", "num_pov_over65", "num_pov",
    "pct_pov_white_alone", "num_pov_white_alone", "num_pov",
    "pct_pov_black_alone", "num_pov_black_alone", "num_pov",
    "pct_pov_asian_alone", "num_pov_asian_alone", "num_pov",
    "pct_pov_anai_alone", "num_pov_anai_alone", "num_pov",
    "pct_pov_nhpi_alone", "num_pov_nhpi_alone", "num_pov",
    "pct_pov_other_alone", "num_pov_other_alone", "num_pov",
    "pct_pov_two_or_more", "num_pov_two_or_more", "num_pov",
    "pct_pov_hisp", "num_pov_hisp", "num_pov",
    "pct_pov_white", "num_pov_white", "num_pov",
    "pct_pov_bach", "num_pov_bach", "num_pov_over_25",
    "pct_pov_less_than_hs", "num_pov_less_than_hs", "num_pov_over_25",
    "pct_pov_unemployed", "num_pov_unemployed", "num_pov_civ_labor_force",
    "pct_pov_veterans", "num_pov_veterans", "num_pov_civ_18_older",
    "pct_pov_disability", "num_pov_disability", "num_pov_noninst_pop",
    "pct_pov_unins", "num_pov_unins", "num_pov_noninst_pop",
    "pct_pov_all_other_races_alone", "num_pov_all_other_races_alone", "num_pov",
)

# For some Total Population variables, the ACS reported percentages are NA
# because both the ACS numerator and denominators are 0. In that case we
# manually insert an estimate and MOE of 0 for the percentage. This happens bc
# for a lot of the Total Population variables, we pull the percentages directly
# from the DP_* tables which report percentages (and are somtimes NA)
prop_definitions_df_to_correct <- tribble(
    ~var, ~num, ~denom,
    "pct_white", "num_white", "num_pop",
    "pct_black", "num_black", "num_pop",
    "pct_asian", "num_asian", "num_pop",
    "pct_hisp", "num_hisp", "num_pop",
    "pct_children", "num_children", "num_pop",
    "pct_veterans", "num_veterans", "num_civ_pop_18_older",
    "pct_seniors", "num_seniors", "num_pop",
    "pct_bach", "num_bach", "num_25_older",
    "pct_unemp", "num_unemp", "num_civ_pop_in_labor_force",
    "pct_unins", "num_unins", "num_noninst_pop",
    "pct_disability", "num_disability", "num_noninst_pop",
    "pct_renters", "num_renters", "num_hhs",
    "pct_under_poverty_line", "num_pov", "num_pov_calc_pop",
    "pct_cb_renter_hh", "num_cb_renter_hh", "num_renter_hh",
    "pct_limited_eng_hh", "num_limited_eng_hh", "num_hhs"
)

# --- Define Helper Functions----
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
# Need to vectorize above fxn so that it can work with vectors in functions
safe_divide_with_zeros <- Vectorize(safe_divide_with_zeros_unvec)

read_in_acs_var_defs <- function(year) {
    # Read in all three ACS variable definition CSV files (which need to be
    # manually constructed)
    # INPUT:
    #   year: the year of data to read in, num
    # OUTPUT: all_acs_vars: A dataframe of all acs variable to pull.

    # Read in ACS variables for that year. If it doesn't exist,
    # you will have to create the three spreadsheets below manually.
    var_list_total_pop <- read_csv(paste0("reference-data/", year, "/acs_variable_definitions/total_population.csv"))
    var_list_under18 <- read_csv(paste0("reference-data/", year, "/acs_variable_definitions/under18_population.csv"))
    var_list_pov <- read_csv(paste0("reference-data/", year, "/acs_variable_definitions/poverty_population.csv"))

    all_acs_vars <- bind_rows(var_list_total_pop, var_list_under18, var_list_pov) %>%
        # For readability some of the files have blank rows and we filter them
        # out
        filter(!is.na(colnames)) %>%
        # We have some manual notes in the CSV for when the variable is a sum of
        # other ACS variables
        filter(acs_vars != "sum of below 4") %>%
        # Sometimes the same ACS variable appears on multiple sheets. Filter out if
        # ACS variable and description are the same
        tidylog::distinct()

    return(all_acs_vars)
}

create_acs_variables_addition <- function(var, vars_to_add, df) {
    # Function which creates derived estimates by adding together multiple ACS variables
    # INPUT:
    #   var: name of new variable as string. Should start with "num_" as all
    #       new variables should be numbers
    #   vars_to_add: name of variables to be added together as a single item
    #       list with a chr vector
    #       ie c("var1", "var2"). This should be a list column in the
    #       var_definitions_add dataframe
    #   df: sf dataframe, which should contain all columns contained in the
    #       vars_to_add vector
    # OUTPUT:
    #   result: A dataframe with 3 columns, the geoid, the new num_ column and
    #   num_*_margin column

    var_margin <- paste0(var, "_margin")

    vars_to_add <- unlist(vars_to_add)
    vars_to_add_margin <- paste0(vars_to_add, "_margin")

    # Use non-standard evaluation (dark magic) to construct vectors of 
    # variables and variable_margins
    vars_to_add_expr <- vars_to_add %>%
        paste0(collapse = ",") %>%
        paste0("c(", ., ")") %>%
        rlang::parse_expr()


    vars_to_add_margin_expr <- vars_to_add_margin %>%
        paste0(collapse = ",") %>%
        paste0("c(", ., ")") %>%
        rlang::parse_expr()

    result <- df %>%
        rowwise() %>%
        mutate(
            # Need to use !! syntax for proper non-standard evaluation of 
            # vectors
            {{ var }} := sum(!!vars_to_add_expr),
            {{ var_margin }} :=
                tidycensus::moe_sum(
                    moe = !!vars_to_add_margin_expr,
                    estimate = !!vars_to_add_expr
                )
        ) %>%
        ungroup() %>%
        select(GEOID, {{ var }}, {{ var_margin }})

    return(result)
}

create_acs_variables_proportions <- function(var, num, denom, df) {
    # Function to create proportion and associated moe columns from a numerator
    # and denominator column
    # INPUT:
    #    var: name of percentage column as string. Should start with "pct_"
    #    num: numerator column as a string
    #    denom: denomerator column as a string
    #    df: sf dataframe, which should contain columns named num and denom
    # OUTPUT:
    #   result: A dataframe with 3 columns, the geoid, the pct_ column and
    #   pct_*_margin column

    num_margin <- paste0(num, "_margin")
    denom_margin <- paste0(denom, "_margin")
    var_margin <- paste0(var, "_margin")

    result <- df %>%
        # Use dtplyr evaluation to make this function quicker, matters for 73k
        # row tract file
        dtplyr::lazy_dt() %>%
        # compute proportion variables and margins
        mutate(
            {{ var }} := round(safe_divide_with_zeros(
                num = .data[[num]], denom = .data[[denom]]
            ) * 100, 3) #multiply by 100 to convert to percentage
        ) %>%
        mutate(
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
                # We multiply the MOE by 100 to get MOE as a percentage, same as
                # the propotion variable above.
                ) * 100, 3)
            )
        ) %>%
        ungroup() %>%
        select(GEOID, {{ var }}, {{ var_margin }}) %>%
        as_tibble()

    return(result)
}

adjust_acs_variables_proportions <- function(var, num, denom, df) {
    # For some pct_* columns, ACS returns NA values bc the num and denom are
    # both 0. In this case, we manually adjust so that the estimate and MOE is 0
    # This only happens in a max of 900/73k tracts and only happens
    # for a few variables in the DP_* variables we use for the total populaion
    # INPUT:
    #    var: name of percentage column as string. Should start with "pct_"
    #    num: numerator column as a string
    #    denom: denomerator column as a string
    #    df: sf dataframe, which should contain columns named var,num and denom
    # OUTPUT:
    #   result: A dataframe with 3 columns, the geoid, the pct_ column and
    #   pct_*_margin column

    num_margin <- paste0(num, "_margin")
    denom_margin <- paste0(denom, "_margin")
    var_margin <- paste0(var, "_margin")

    result <- df %>%
        dtplyr::lazy_dt() %>%
        mutate(
            {{ var }} := ifelse((.data[[num]] == 0) & (.data[[denom]] == 0),
                0,
                .data[[var]]
            )
        ) %>%
        mutate(
            {{ var_margin }} := ifelse((.data[[num]] == 0) & (.data[[denom]] == 0),
                0,
                .data[[var_margin]]
            )
        ) %>%
        # There are 17 values of pct_children_margin and 46 values of
        # pct_hisp_margin where the num and denom are
        # positive, but are both controlled estimates and hence have margins of
        # zero. In these specific cases, we transform the MOEs to 0
        tidylog::mutate(
            {{ var_margin }} := ifelse(is.na(.data[[var_margin]]) &
                .data[[num_margin]] == 0 &
                .data[[denom_margin]] == 0 &
                .data[[denom]] != 0 &
                .data[[num]] != 0,
            0,
            .data[[var_margin]]
            )
        ) %>%
        ungroup() %>%
        select(GEOID, {{ var }}, {{ var_margin }}) %>%
        as_tibble()

    return(result)
}

append_proportion_columns <- function(df, 
                                      prop_definitions_df, 
                                      prop_definitions_df_to_correct) {
    # Creates proportion and moes for given num and denom acs variables
    # based on formulas laid out in the prop_definitions_df dataframe
    # INPUT:
    #   df: A dataframe returned from tidycensus in wide format,
    #       and with human readable clean names rather than S1701_001M
    #   prop_definitions_df: A dataframe with 3 columns as defined below
    #       var: name of percentage column as string. Should start with "pct_". You can make this up
    #       num: numerator column as a string. Should be a column in df
    #       denom: denomerator column as a string. Should be a column in df
    #   prop_definitions_df_to_correct: A dataframe whih defines proportions to
    #       adjust if the ACS reports NA perecntages, but 0 for num and denom,
    #       should have same 3 columns as above df.

    df_no_geometry <- df %>% st_drop_geometry()
    newly_created_proportion_variables <- pmap(
        prop_definitions_df,
        create_acs_variables_proportions,
        df = df_no_geometry
    ) %>%
        reduce(dplyr::left_join, by = "GEOID")


    # For some pct_* columns, ACS returns NA values bc the num and denom are
    # both 0. In this case, we manually adjust so that the estimate and MOE is
    # 0. This only happens in a max of 900/73,000 tracts and only happens for a
    # few variables in the DP variables we use for the full population
    na_adjusted_proportion_variables <- pmap(
        prop_definitions_df_to_correct,
        adjust_acs_variables_proportions,
        df = df_no_geometry
    ) %>%
        reduce(dplyr::left_join, by = "GEOID")



    result <- df %>%
        # Unselect the pct_* and pct_*_margin variables we have adjusted and
        # then re-add them with a left join
        select(-prop_definitions_df_to_correct$var, 
                -paste0(prop_definitions_df_to_correct$var, "_margin")) %>%
        left_join(na_adjusted_proportion_variables, by = "GEOID") %>%
        # Add on newly created proportion variables
        left_join(newly_created_proportion_variables, by = "GEOID")

    return(result)
}

append_addition_columns <- function(df, add_definitions_df) {
    # Function which creates and appends estimates and moes for the
    # acs variables we need to add together based on formulas laid out in the
    # add_definitions_df dataframe
    # INPUT:
    #   df: A dataframe returned from tidycensus in "wide" format,
    #       and with human readable clean names rather than S1701_001M
    #   add_definitions_df: A dataframe with 2 columns as defined below
    #       var: name of new column as string. Should start with "num_". 
    #           You can make this up
    #       var_definitions_add: a character vector of columns to add together
    #           ie c("x","y")

    df_no_geometry <- df %>% st_drop_geometry()


    # The *_all_other_race variables depend on a few other variables which are created
    # in add_definitions_df. Bc we generate these variables with map_df,
    # the variables aren't immediately joined and available for use with future
    # definitions laid out in add_definitions_df. So we first add all the non
    # all_other_race variables. Then after joining to original data,
    # we add the *_all_other_race variables
    var_definitions_add_not_other_race <- add_definitions_df %>%
        filter(!str_detect(var, "all_other_race"))

    var_definitions_add_other_race <- add_definitions_df %>%
        filter(str_detect(var, "all_other_race"))


    # Add all non all_other_race variables
    newly_created_addition_variables <- pmap(
        var_definitions_add_not_other_race,
        create_acs_variables_addition,
        df = df_no_geometry
    ) %>%
        reduce(dplyr::left_join, by = "GEOID")


    intermediate_result <- df %>%
        left_join(newly_created_addition_variables, by = "GEOID")

    # Add all_other_race variables
    all_other_race_addition_variables <- pmap(
        var_definitions_add_other_race,
        create_acs_variables_addition,
        df = intermediate_result %>% st_drop_geometry()
    ) %>%
        reduce(dplyr::left_join, by = "GEOID")



    # Final join to result
    result <- intermediate_result %>%
        left_join(all_other_race_addition_variables, by = "GEOID")
    
    return(result)
}

read_in_and_clean_acs_data <- function(year,
                                       geography,
                                       acs_variable_df,
                                       prop_definitions_df,
                                       prop_definitions_df_to_correct,
                                       add_definitions_df,
                                       write_out = TRUE) {
    # Read in and clean ACS variables for use in Spatial Equity Tool.
    # INPUT:
    #   year: endyear of 5 year ACS data to use
    #   geography: geography to pull data for. Must be one of "us", "state", 
    #       "county", or "tract"
    #   acs_variable_df: dataframe which specifies which acs variables to pull.
    #       Must have two columns named `colnames` (human readable name) and
    #       `acs_vars` (Census variable name like DP05_001)
    #   prop_definitions_df: dataframe of proportions to create manually from
    #       numerator and denominator values. This needs to happen bc the ACS
    #       doesn't report some specific variables as percentages, we need to
    #       calculate the estimate and MOE of the percentage ourselves. The
    #       dataframe must have 3 columns as defined below
    #   var: name of percentage column as string. Should start with "pct_". You
    #       can make this up
    #       num: numerator column as a string. Should be a column in df
    #       denom: denomerator column as a string. Should be a column in df
    #   prop_definitions_df_to_correct: A dataframe of proportions to correct.
    #       For some Total Population variables, the ACS reported percentages are NA
    #       because both the ACS numerator and denominators are 0. In that case we
    #       manually insert an estimate and MOE of 0 for the percentage.
    #   add_definitions_dfL A dataframe of variables to create via addition of
    #       multiple ACS variables


    acs_variables_named <- acs_variable_df %>%
        mutate(acs_vars = paste0(acs_vars, "E")) %>%
        bind_rows(
            acs_variable_df %>%
                mutate(
                    colnames = paste0(colnames, "_margin"),
                    acs_vars = paste0(acs_vars, "M")
                )
        )


    if (geography == "us") {
        acs_raw_cleaned <-
            get_acs(
                geography = geography,
                variables = acs_variable_df$acs_vars,
                year = year,
                output = "wide",
                geometry = TRUE,
                refresh = TRUE
            ) %>%
            # rename to human friendly variable names
            select(GEOID, NAME, geometry, acs_variables_named %>%
                select(-notes) %>%
                deframe())
    
    } else {

        # Create list of all US state fips to loop over
        # Restrict to fips less than 57 to exclude territories
        state_fips <- fips_codes %>%
            filter(as.numeric(state_code) < 57) %>%
            pull(state_code) %>%
            unique()

        # Get list of state abbvs <> state fips for use in left_join
        state_abbv <- fips_codes %>%
            filter(as.numeric(state_code) < 57) %>%
            select(
                state_abbv = state,
                state_fips = state_code
            ) %>%
            distinct()
        
        # pull ACS data for each state
        acs_raw <- map(
            state_fips,
            ~ {
                print(.x)
                data <- get_acs(
                    geography = geography,
                    variables = acs_variable_df$acs_vars,
                    state = .x,
                    year = year,
                    output = "wide",
                    geometry = TRUE,
                    refresh = TRUE
                )
                return(data)
            }
        ) %>%
            reduce(rbind) %>%
            # rename to human friendly variable names
            select(GEOID, NAME, geometry, acs_variables_named %>%
                select(-notes) %>%
                deframe())
        
        if (geography == "tract") {
            
            # For the 2017 and 2018 5 year ACS data, there was an error in Rio
            # Arriba county for some variables, so we manually adjust. Note that
            # This is not an issue for 2019 data.
            if (year %in% c(2017, 2018)) {
                
                # For now we replace Rio Arriba tract NA values with 0.
                rio_arriba_tracts <- acs_raw %>%
                    filter(str_starts(GEOID, "35039")) %>%
                    tidylog::mutate(across(-geometry, .fns = ~ replace_na(.x, 0)))
                
                x <- acs_raw %>%
                    # remove old Rio Arriba tracts and append new tracts with
                    # NAs replaced
                    filter(!str_starts(GEOID, "35039")) %>%
                    bind_rows(rio_arriba_tracts)
                
                # Insert tests for Rio Arriba counties
                testthat::test_that("Non Rio-Arriba tracts weren't affected", {
                    testthat::expect_true(
                        dplyr::all_equal(
                            x %>%
                                filter(!str_starts(GEOID, "35039")) %>%
                                st_drop_geometry(),
                            acs_raw %>%
                                filter(!str_starts(GEOID, "35039")) %>%
                                st_drop_geometry()
                        )
                    )
                })
                
                testthat::test_that("Only 9 Rio-Arriba tracts were changed", {
                    testthat::expect_true(
                        dplyr::all_equal(
                            x %>%
                                filter(str_starts(GEOID, "35039")) %>%
                                st_drop_geometry(),
                            acs_raw %>%
                                filter(str_starts(GEOID, "35039")) %>%
                                st_drop_geometry(),
                        ),
                        "- Rows in x but not in y: 1, 2, 3, 4, 5, 6, 7, 8, 9\n- Rows in y but not in x: 1, 2, 3, 4, 5, 6, 7, 8, 9\n"
                    )
                })

                # After tests pass, overwrite the acs_raw object
                acs_raw <- x
            }


            # For tract level data, we need to do a bit more data cleaning
            acs_raw_cleaned <- acs_raw %>%
                mutate(
                    # Add area of tract to use for city definition cutoffs
                    area_tract = st_area(.),
                    # Split NAME column to extract tract name and county name
                    # later
                    x = str_split(NAME, ","),
                    # Extract state and county fips from full GEOID
                    state_fips = str_sub(GEOID, start = 1, end = 2),
                    county_fips = str_sub(GEOID, start = 3, end = 5),
                ) %>%
                mutate(
                    # Extract tract name (ie Census tract 1) from NAME column.
                    # This will be used in conjunction with state_abbv and
                    # cleaned_name column in place data to generate NAME column
                    # for display on frontend
                    tract_name = map_chr(x, ~ paste0(
                        .x %>% pluck(1)
                    )),
                    county_name = map_chr(x, ~ paste0(
                        .x %>% pluck(2)
                    ) %>% str_trim())
                ) %>%
                # x is a list column bc we used str_split. We don't need it
                # anymore so we delete. Now we can store future dfs as flat
                # files
                select(-x) %>%
                left_join(state_abbv, by = "state_fips")
        } else {
            acs_raw_cleaned <- acs_raw
        }

        
    }

    ### --- Clean ACS Data -----

    # In the raw tidycensus reported values, some margin values might be NA.
    # This represents ACS controlled estimates and in accordance with Census
    # guidance, we convert the NA margins to 0 margins. First we record how
    # many controlled estimate margins have NA values and reord them

    # To find rows where ANY numeric variable is greater than zero
    rowAny <- function(x) rowSums(x) > 0

    controlled_estimate_nas <- acs_raw_cleaned %>%
        # Only select columns with pattern num_*_margin
        select(GEOID, starts_with("num_")) %>%
        tidylog::filter(rowAny(
            across(
                .cols = everything(),
                .fns = ~ is.na(.x)
            )
        )) %>%
        map_df(~ sum(is.na(.))) %>%
        pivot_longer(cols = everything(), names_to = "var") %>%
        arrange(desc(value))


    testthat::test_that(
        "All num_* variables with NA values are MOEs",
        testthat::expect_true(
            controlled_estimate_nas %>%
                filter(value > 0) %>%
                pull(var) %>%
                str_ends("_margin") %>%
                all()
        )
    )

    # Write out number of controlled estimates for each variables (should be max
    # of 133 which occurs for the num_pop_margin variable)
    dir.create(str_glue("reference-data/{year}/logs"), showWarnings = FALSE)
    controlled_estimate_nas %>%
        write_csv(str_glue("reference-data/{year}/logs/{geography}_controlled-estimate-na-counts.csv"))

    # Overwrite NA MOEs with 0 as we have manually confirmed they are all
    # controlled estimates
    acs_raw_controlled_estimate_replaced <- acs_raw_cleaned %>%
        tidylog::mutate(across(starts_with("num_"), ~ replace_na(.x, 0)))


    # Manually append columns created by addition and proportions
    # acs_all <- acs_raw_controlled_estimate_replaced %>%
    acs_all <- acs_raw_controlled_estimate_replaced %>%
        append_addition_columns(add_definitions_df) %>%
        # Create custom proportion variables and their MOEs, also for pct_*
        # columns where the ACS reports NAs (bc the num and denom are both 0),
        # manually replace with 0 as estimate. Also replacec MOE in this case
        # to be 0
        append_proportion_columns(
            prop_definitions_df,
            prop_definitions_df_to_correct
        )

    # Perform data checks on ACS data 
    # Ensure pct columns are all between 0 and 100
    test_that("Percentages are between 0 and 1",
        expect_equal(
            acs_all %>%
                select(starts_with("pct_") & !ends_with("_margin")) %>%
                st_drop_geometry() %>%
                pivot_longer(cols = everything())  %>% 
                filter(rowAny(
                        across(
                            .cols = where(is.numeric),
                            .fns = ~ .x < 0 | .x > 100
                        )
                )
            ) %>%
            nrow(),
             0
        )
    )

    # write out clean data
    if (write_out) {
        dir.create(str_glue("reference-data/{year}/clean-acs-data/"), showWarnings = FALSE)
        st_write(acs_all, dsn = str_glue("reference-data/{year}/clean-acs-data/{geography}.geojson"), delete_dsn = TRUE)
    }
    return(acs_all)
}

generate_big_places_list <- function(yr, us_state_list) {
    # function to create df of places with population of over 50k. Need to use
    # tidycensus for population and then join with tigris provided place
    # boundaries bc sf does not yet support geometries for places
    # INPUTS:
    #     yr: integer, endyear of 5 year ACS for places boundaries and
    #     population estimates to be pulled from
    #     us_state_list: Character vectors of state FIPS 
    #
    # RETURNS:
    #   big_places: sf_dataframe, population data for all census places in US with pop > 50k
    places <- map_df(us_state_list, function(x) {
        get_acs(
            geography = "place", variables = "B01003_001", # this is constant across years while DP variable for total pop changes
            state = x, year = yr
        ) %>%
            filter(estimate >= 50000)
    })
    
    # tigris: get spatial data for all census places in US
    places_geo <- map(us_state_list, function(x) {
        # For KY, users have reproted that Louisville City is incorrectly
        # specified in Census Tigris Files. We have confirmed that the correct
        # geography should be the combination of 2 GEOIDS (2148000, 2148006) as
        # that is the correct geography for which the ACS releases data. Howveer
        # the Tigris files have those geographies separetely for historical /
        # reporting reasons: 
        if (x == "21") {
            geos = tigris::places(
                state = x, class = "sf", year = yr,
                refresh = TRUE
            ) %>%
                select(STATEFP, PLACEFP, GEOID, NAME, NAMELSAD, geometry)
            
            lousiville_city_separate_geos = geos %>%
                filter(GEOID %in% c("2148000", "2148006"))
            
            lousiville_city_corrected_geos = lousiville_city_separate_geos %>%
                mutate(geometry = st_union(lousiville_city_separate_geos))
            
            other_ky_geos = geos %>%
                filter(!GEOID %in% c("2148000", "2148006"))
            
            ky_geos = bind_rows(lousiville_city_corrected_geos, other_ky_geos)
            
            return(ky_geos)
            
        } else {
            geos = tigris::places(
                state = x, class = "sf", year = yr,
                refresh = TRUE
            ) %>%
                select(STATEFP, PLACEFP, GEOID, NAME, NAMELSAD, geometry)
            
            return(geos)
        }
    }) %>% do.call("rbind", .)
    
    # Filter to places with places with pop over 50k
    places_geo <- places_geo %>%
        filter(GEOID %in% places$GEOID) %>%
        select(-NAME)
    
    # Get list of states for left joining
    state_abbv <- tigris::states(cb = T, year = yr, class = "sf")  %>% 
        select(state_fips = GEOID,
               state_abbv = STUSPS) %>% 
        st_drop_geometry()
    
    # join spatial and population data for big places
    print(st_crs(places_geo))
    big_places <- places_geo %>%    
        left_join(places, by = c("GEOID" = "GEOID")) %>%
        st_transform("EPSG:4269") %>% 
        rename(place_geoid = GEOID,
               state_fips = STATEFP) %>% 
        mutate(cleaned_name = stri_replace_last(NAMELSAD, "", regex = "city"),
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
        left_join(state_abbv, by = "state_fips") %>%
        # For places, generate disp_name by combining full city name and st abbv
        unite("disp_name", c("cleaned_name", "state_abbv"), sep = ", ")
    
    
    return(big_places)
}

# --- Read in ACS Data ----

# Turn off cache to force tigris to redownload the data files
options(tigris_use_cache = TRUE)

acs_variable_df <- read_in_acs_var_defs(year = year)

us_data <- read_in_and_clean_acs_data(
    year = year,
    geography = "us",
    acs_variable_df = acs_variable_df,
    prop_definitions_df = prop_definitions_df,
    prop_definitions_df_to_correct = prop_definitions_df_to_correct,
    add_definitions_df = add_definitions_df,
    write_out = TRUE
)

tract_data <- read_in_and_clean_acs_data(
    year = year,
    geography = "tract",
    acs_variable_df = acs_variable_df,
    prop_definitions_df = prop_definitions_df,
    prop_definitions_df_to_correct = prop_definitions_df_to_correct,
    add_definitions_df = add_definitions_df,
    write_out = TRUE
)


state_data <- read_in_and_clean_acs_data(
    year = year,
    geography = "state",
    acs_variable_df = acs_variable_df,
    prop_definitions_df = prop_definitions_df,
    prop_definitions_df_to_correct = prop_definitions_df_to_correct,
    add_definitions_df = add_definitions_df,
    write_out = TRUE
)

county_data <- read_in_and_clean_acs_data(
    year = year,
    geography = "county",
    acs_variable_df = acs_variable_df,
    prop_definitions_df = prop_definitions_df,
    prop_definitions_df_to_correct = prop_definitions_df_to_correct,
    add_definitions_df = add_definitions_df,
    write_out = TRUE
)


# --- Generate and write out all_polygons files ----

# Write all_polygons files used to ID state/county in user uploaded data
# with the cleaned_name shown in data summary box

# Create output directories for all_polygons data
dir.create(str_glue("reference-data/{year}/state"), showWarnings = FALSE)
dir.create(str_glue("reference-data/{year}/county"), showWarnings = FALSE)
dir.create(str_glue("reference-data/{year}/city"), showWarnings = FALSE)

state_abbv <- fips_codes %>%
    select(state, state_code) %>%
    unique()

all_county_polygons <- county_data %>%
    mutate(state_code = substr(GEOID, 1, 2)) %>%
    left_join(state_abbv, by = "state_code") %>%
    mutate(cleaned_county = gsub(",.*$", "", NAME)) %>%
    # Change DC to the District of Columbia to make sentences on frontend
    # work correctly
    tidylog::mutate(cleaned_county = if_else(
        cleaned_county == "District of Columbia",
        "the District of Columbia",
        cleaned_county
    )) %>% 
    unite("cleaned_name", c("cleaned_county", "state"), sep = ", ") %>%
    select(GEOID, cleaned_name) 

st_write(all_county_polygons, 
         str_glue("reference-data/{year}/county/all_polygons.geojson"), 
         delete_dsn = TRUE) 

all_state_polygons <- state_data %>%
    mutate(cleaned_name = NAME) %>%
    # Change DC to the the District of Columbia to make sentences on frontend
    # work correctly
    tidylog::mutate(cleaned_name = if_else(
        cleaned_name == "District of Columbia",
        "the District of Columbia",
        cleaned_name
    )) %>% 
    select(GEOID, cleaned_name) 

st_write(all_state_polygons, 
         str_glue("reference-data/{year}/state/all_polygons.geojson"), 
         delete_dsn = TRUE)


# For states, the tigris shp file is large (about 5.2 MB even with CB set to 
# TRUE). This causes national API responses, which contain the state geometries
# to be above API Gateway's builtin limit of 5 MB responses. So just for states, 
# we download in a generalized shapefile with higher resolution. We DON'T use 
# the generalized shapefile for actual spatial joins/calculations bc that would
# be less accurate. But we do use the generalized shpfile to return smaller 
# geometry columns to display on the frontend. 

states_20m = states(
    year = year,
    cb = T,
    # resolution = 20m is most generalized shpfile with smallest size
    resolution = "20m"
) %>% 
    select(num_geoid = GEOID, state_abbv = STUSPS) %>% 
    # Filter out PR, which has fips = 72
    filter(num_geoid < 57)

states_20m %>% 
    st_write(
        str_glue(
            "reference-data/{year}/state/all_polygons_20m_generalized.geojson"
            ),
        delete_dsn = T
    )

# Create list of  US state fips to loop over to generate big places df
us <- fips_codes %>%
    filter(as.numeric(state_code) < 57) %>%
    pull(state_code) %>%
    unique()

all_city_polygons <- generate_big_places_list(yr = year, us_state_list = us) %>%
    rename(GEOID = place_geoid, cleaned_name = disp_name)

st_write(all_city_polygons,
         delete_dsn = TRUE,
         str_glue("reference-data/{year}/city/all_polygons.geojson")
         )
