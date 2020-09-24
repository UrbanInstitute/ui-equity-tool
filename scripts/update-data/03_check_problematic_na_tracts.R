library(tidyverse)
library(weights)

# Get list of all tracts from Low Income Job Loss Tool
all_tracts <- st_read("https://ui-lodes-job-change-public.s3.amazonaws.com/job_loss_by_tract.geojson")

all_acsvars <- tribble(
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

t <- us %>%
  map_df(
    ~ get_acs(
      geography = "tract", variables = acsvars$acs_vars,
      state = .x, geometry = TRUE,
      output = "wide", year = yr
    ) %>%
      st_drop_geometry()
  )

x <- t
colnames(t) <- all_acsvars$colnames[match(names(t), all_acsvars$acs_vars)]

rowAny <- function(x) rowSums(x) > 0

na_rows <- t %>%
  filter(rowAny(across(starts_with("num_"), ~ is.na(.x))))

prob_tracts <- all_tracts %>%
  filter(GEOID %in% na_rows$GEOID) %>%
  select(GEOID) %>%
  mutate(state = str_sub(GEOID, 1, 2))
states_with_bad_tracts <- prob_tracts %>%
  pull(state) %>%
  unique()

# How many problematic tracts with some NA values are there?
nrow(prob_tracts)

big_places <- st_read("reference-data/2018/big_places_us_manual_join_1p_cutoff.geojson")
big_places <- big_places %>% filter(STATEFP %in% states_with_bad_tracts)

# Doesn't look like there are any overlapping places and tracts
mapview(prob_tracts, col.regions = "red") + mapview(big_places, col.regions = "forestgreen")


# See if any problematic tracts overlap with any big census place
one_tract <- st_join(prob_tracts %>% mutate(area = st_area(.)),
  big_places,
  how = "left"
) %>%
  filter(!is.na(place_geoid)) %>%
  nrow()

# There is one tract in NV that overlaps. But from looking at the map,
# only the very tip of the tract overlaps and it wouldn't meet our
# 1% cutoff bound. We confirm this by reading in that place's geojson.
carson_city <- st_read(paste0("reference-data/2018/tracts_by_place/", one_tract$place_geoid, ".geojson"))

# Check that one overlapping tract doesn't meet our 1% cutoff (and therefore
# isn't in the final place writeout of all tracts)
assert(!one_tract$GEOID %in% carson_city$tract_geoid)