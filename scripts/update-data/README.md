# Updating Reference Data for the Spatial Equity Data Tool

To update all the reference files for the Spatial Equity Data Tool, you just 
need to run `main-update-data-script.R`, which is really a wrapper to source
all the data update scripts in order. Script `03` defines a python function, 
which is then used in `main-update-data-script.R`. This may seem a little 
roundabout but we do this so we can easily define all parameters just within 
the main script. Before running `main-update-data-script.R`, you will want to do
the following:

   - Adjust the year parameter at the top of `main-update-data-script.R` to the 
   desired end year for the 5-year ACS.
   - Ensure that you have a `.env` file in the root directory with the appropriate
   environment variables set and which are used as parameters in
   `main-update-data-script.R`.

Below is a description of each of the scripts that `main-update-data-script.R`
sources: 

1) `01_download-and-clean-acs-data.R`: Reads and cleans ACS data for all tracts, counties, 
and states in the US as well as for the US as a whole. Writes out a file for each geographic
level (tract, county, state, us) to `reference-data/{year}/clean-acs-data/{geography}.geojson`.
Also creates geojson files for the geographic boundaries of all tracts, counties, places (cities) 
and states in the US. Writes out the files to `reference-data/{year}/{geography}/all_polygons.geojson`. 
Note that for states, we write a generalized shapefile with lower resolution to save file space to 
`reference-data/{year}/state/all_polygons_20m_generalized.geojson`.

2) `02_generate-baseline-proportions.R`: Generates the following files for the US and each unique 
state, county, and city (census place with population over 50k) in the US:
   - `precomputed statistics`: A CSV of geography-wide statistics and standard errors for
      the selected demographic ACS variables the tool uses. It uses the Census
      formulas for standard errors of user derived proportions/percentages to
      generate SEs. Each file is written to `reference-data/{year}/{geography}/{unique geoid}.csv`.
      For the US this is `1.csv`, for states the two digit FIPS code, for counties the five
      digit FIPS code (2 digit state FIPS code + 3 digit county FIPS code), and for cities the seven
      digit FIPS code (2 digit state FIPS code + 5 digit place FIPS code).
   - `geographic baseline files`: Geojson files of smaller geographies within the larger geography
      used for analysis. For the national geography this writes a file of all tracts in the US 
      (used for demographic disparity) and a file of all states in the US (used for geographic disparity).
      For each state, the tool writes out a file of all tracts in the state (used for demographic disparity) 
      and all counties in the state (used for geographic disparity). For each county and each city the tool
      writes out a file of all tracts in the geography. Note that for cities, the tracts in the city are 
      determined by a spatial join between the census place shapefile and the tract shapefile produced in step 1.
      These files are written to `reference-data/{year}/{geography}/{smaller geography}/{unique geoid}.geojson`.
      Note that the unique geoid is defined identically as for precomputed statistics above.   
     
3) `03_upload_ref_data_to_s3.py`: Defines a function to upload all the CSV's and geojsons located in the `reference-data/{year}/{geography}/*` 
   folders to S3 as pickles. We convert to pickles as we noticed speed gains when the data were read in by the 
   tool as pickles instead of geojsons/CSVs. Generally, you should only be uploading results to the staging bucket, 
   so that you can test and ensure that the results are as you'd expect. But if needed you can change
   the `output_bucket` and `output_region` arguments to this function as needed. Note this step is optional and not necessary if you aren't planning on using the AWS infrastructure.
   

