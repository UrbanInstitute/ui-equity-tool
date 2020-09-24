# Spatial Equity Data Tool

This repo contains all the data and code powering the Spatial Equity Data Tool.
It also contains the files needed to create, and deploy the
Spatial Equity Data Tool and all of it's required infrastructure on AWS COdestar
(a CI/CD service). If you are just interested in the tool
methodology/calculations and/or don't want to setup your own AWS infrastructure
for the tool, you can skip most of the files in this repo and head straight to
`scripts/lambda/equity_calculations.py`.


## Codestar Files
If you are not planning on deploying to AWS, or using AWS services, you can skip
this section

  - `template.yml`: A template that defines all the AWS resources needed for the
  operation of the Spatial Equity Data Tool. This includes an S3 bucket, 2 lambda
  functions, an API Gateway, and appropriate permissions for all of them. Note
  that we make use of some predefined roles, like `equity-assessment-tool-role`
  that we have manually created and is only available in Urban's AWS account. For
  reproducibility we have included a copy of the single IAM policy that is
  attached to the `equity-assessment-tool-role` in the `docs/` folder so you can
  recreate this role if you want. Note that you might need to edit the IAM role to
    reflect your S3 bucket path more directly. There are also several
    placeholder values in this template that start with `INSERT_` that you will
    need to provide yourself.

  - `buildspec.yml`: This file is used by AWS CodeBuild to package your
  application for deployment to AWS Lambda. It is a collection of build
  commands to build your Cloudformation template and run your build. We have
  modified this to also manually update the equity tool lambda function using
  `aws lambda update-function-code` as we have noticed Codestar often has
  problems updating lambda functions that rely on deployment packages stored
  in S3.

  - `template-configuration.json` - this file contains the project ARN with
  placeholders used for tagging resources with the project ID

  - `equity_tool_deployment_package.zip` - the deployment package with the
  compiled dependencies and the code (`equity_calculations.py`) that powers
  the workhorse lambda function. For instructions on how to create this
  deployment package from scratch, please see
  `docs/creating_deployment_pkg_with_geopandas.md`, 


## scripts/

### lambda/
This is the code powering the lambda functions defined in the Codestar template

  - `equity_calculations.py`: This is the workhorse function that does the
  geographic and demographic disparity calculations. It reads in user uploaded data,
  determines the dataset's source city (by performing a spatial join on a small
  sample of the data), reads in the city's demographic and geographic data, and
  calculates our disparity scores. It then writes out the outputs into S3 to be
  returned to the enduser by the API. Because this is a lambda function, the main
  code logic is contained in the `hander` function. This code is only meant to
  work in conjunction with the other AWS infrastructure setup by `template.yml`.
  If you do not want to use our AWS infrastructure to run the equity calculations,
  you will need to modify this script a good deal. To start with, you'd need to rewrite
  the data readin/writeout functions to read/write data locally instead of from
  S3,and remove most of the `update_status_json` calls. If you want to create a
  version of this script to locally calculate disparity scores and need help
  modifying this script to meet your needs, please reach out to us!


  - `getstatus_and_getfile.py`: The lambda function powering the API for data
  retrieval and checking the status of submitted jobs. There is a separate API
  that handles submissions of new jobs (and data). This function assumes that the
  data retrieval API has two endpoints defined in `template.yml`. 



### sample-data-generation/
These are scripts to generate the sample data used by the tool.

  - `generate_sample_bike_data`: This generates the Minneapolis Nice Ride bike
  station location dataset used as a sample dataset in our tool. The Nice Ride
  system data only allows download of thier system information as a JSON< so we
  transfrom that into a CSV for use with the tool. In the comments of this script,
  we explain how we downloaded the New Orleans 311 data (another sample
  dataset in our tool) using a custom Socrata API URL. For more information on all
  the sample datasets we use in the tool and how we downloaded/cleaed them, please
  see our [Data Catalog
  entry](https://datacatalog.urban.org/dataset/spatial-equity-data-tool-sample-datasets)


### update-data/

These scripts download and update the ACS data powering the Spatial Equity Data
Tool

  - `01_download_ref_data.R`: Generates list of census places in US with
  populations over 50k. For each census place, it generates two data files:
    - `{place_geoid}.geojson`: A geojson of all tracts in the census place (as
      determined by a spatial join). This geojson also contains fields for all
      the baseline ACS variables the tool uses. Note that `place_geoid` will
      be the full 7 digit GEOID (2 digit state geoid + 5 digit place geoid).
    - `{place_geoid}.csv`: A CSV of citywide statistics and standard errors for
      the selected demographic ACS variables the tool uses. It uses the Census
      formulas for standard errors of user derived proportions/percentages to
      generate SEs.
  These two output files are placed in the `reference-data/{year}/` folder
  on your local Github repo. This script also places a file called
  `big_places_us_manual_join_ip_cutoff.geojson` inside the `reference-data/{year}/`
  directory, which is a listing of all census places in the US with
      populations over 50,000 and the corresponding tracts within that place.
      See the [technical appendix](https://apps.urban.org/features/equity-data-tool/spatial_equity_technical_appendix.pdf)
      for more information on how the tool defines the boundaries of a city
      using census tracts.

  - `02_upload_ref_data_to_s3.py`: Uploads all the CSV's and geojsons (including
    the Census place geojson) located in the `reference-data/{year}` folder to
    S3 as pickles. We convert to pickles as we saw noteicable speed gains when
    the data were read in by the tool as pickles instead of geojsons/csvs. You
    can change the `output_bucket` and `output_region` in this script as needed.
    Note this step is optional and not necessary if you aren't planning on using
    the AWS infrastructure.

  - `03_check_problematic_na_tracts.R`: This checks that for all the cities covered by
  our tool, there are no tracts with NA values for any of our demographic
  variables. We found that across all 73,000+ tracts, only 143 tracts contained NA
  values for our demographic variables of interest. Of those 143 tracts, none of
  them were located in Census places with a population greater than 50,000. As a
  result, these problematic tracts do not affect our tools calculations.


## reference-data/
This folder contains the data written out locally by `update-data` scripts.

## docs/
Contains some documentation around setup of the lambda functions and sample
datasets.

  - `sample datasets/`: Contains all the sample datasets used in the tool, with
    the exception of the New Orleans data as it is bigger than Github's 100 MB
    file size limit. This folder also contains the form jsons that encodes the
    filters, and weights used by the tool for each sample dataset.
  - `creating_deployment_pkg_with_geopandas`: A how-to guide on creating a
    deployment packages with geopandas for use in AWS lambda. By creating this
    deployment package, you can import and use `geopandas` functions in your
    lambda code. We have found this to be cost effective, and efficient way to
    do spatial operations in AWS. This was last updated 2020-05-15.
  - `creating_deployment_pkg.sh`: A helper script that performs the steps laid
    out in the guide.



