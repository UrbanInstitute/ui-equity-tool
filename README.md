[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
# Spatial Equity Data Tool

This repo contains some example files, code and data which power Urban's [Spatial Equity Data tool](https://apps.urban.org/features/equity-data-tool/)

All of the tools infrastructure is deployed using AWS Codestar (a CI/CD
service). If you are just interested in the tool methodology/calculations and/or
don't want to setup your own AWS infrastructure for the tool, you can skip most
of the files in this repo and head straight to
`scripts/lambda/equity_calculations.py`. Make sure to read the description of
that script below to get a sense of the changes you will have to make.

# License

This code is licensed under the GPLv3 license.
# Prerequisites
## Environment Variables

Environment variables need to be set in two places:

1) In an `.env` file in the root of this repo. This file will be used by scripts
   that are run locally on your machine. The `.env` file should look like:
   ```
   Stage=XXX
   equity_file_bucket=XXX
   equity_infrastructure_bucket=XXX
   equity_file_bucket_region=XXX
   ```
   By default the `.env` file is added to the gitignore so it will need to be
   added manually to your repo locally. In the rest of this README, we will
   refer to these env variables as follows: <equity_file_bucket>. For
   confidentiality reasons, **we have inserted several placeholder values within
   the files in this repo that look like `<some-name>` that you will need to
   provide  yourself.**


2) In the CodeBuild project associated with the pipeline. This needs to be set 
   **manually** on the AWS console after the project has first been deployed. 
   These environment variables are used when deploying the scripts and AWS
   resources in `template.yaml` and `buildspec.yml` as well as in the Lambda
   function code. The key variable used in Codebuild is `Stage`.

## AWS credentials

You will need AWS credentials (we used AWS admin creds) in order to run the data
update scripts and upload data to S3. Install the AWS CLI, and configure the
credentials with the `aws configure` command. Your AWS creds will need access to
the following S3 buckets:

  - <equity_infrastructure_bucket>-stg
  - <equity_infrastructure_bucket>-prod
  - <equity_file_bucket>-stg
  - <equity_file_bucket>-prod

## Conda environment

You will need to install a conda environment with the packages laid
out in `environment.yml`. Note right now they all list OSx specific packages, as
that is a limitation of conda. Key packages to install are
`geopandas` version >= 0.9.0 and `boto3`.

As a backup here is a manual list of python packages I installed on my conda
environment from the `conda-forge` channel:

- `geopandas`
- `boto3` 

And R packages below:

- `sf`
- `tidycensus`
- `tidyverse`
- `tigris`
- `dtplyr`
- `testthat`
- `dotenv`
- `aws.s3`
- `stringi`
- `readxl`
- `here`
- `jsonlite`
- `httr`


# Files 
 
Below is an explanation of all folders and files in this repo. Italicized file
names are files required by AWS Codestar, the CI/CD service we use to deploy the
Spatial Equity Tool. Some of the folders/subfolders may not exist upon initial
cloning but will be written out as you run the `scripts/`

  - `.env`: A gitignored env file which contains environment variables used by
    scripts in the `scripts/` folder.

  - *`buildspec.yml`*: This file is used by AWS CodeBuild to package your
  application for deployment to AWS CloudFormation. It is a collection of build
  commands to build your Cloudformation template and run your build. Most of
  this file is just the default Codestar template. But we have
  modified this to also manually update the equity tool lambda function using
  `aws lambda update-function-code` as we have noticed Codestar often has
  problems updating lambda functions that rely on deployment packages stored
  in S3 without this line.

  - *`template.yml`*: A template that defines all the AWS resources needed for the
  operation of the Spatial Equity Data Tool. This includes an S3 bucket, 2 lambda
  functions, an API Gateway, and appropriate permissions for all of them. Note
  that we make use of some predefined roles, like `equity-assessment-tool-role`
  that we have manually created and is only available in Urban's AWS account. For
  reproducibility we have included a copy of the single IAM policy that is
  attached to the `equity-assessment-tool-role` in the `docs/` folder so you can
  recreate this role if you want. Note that you will need to edit the IAM role to
  reflect your S3 bucket path.

  - *`template-configuration.json`* - this file contains the AWS project ARN with
  placeholders used for tagging resources with the project ID.

  - `equity_tool_deployment_package_3.9.zip` - the deployment package with the
  compiled dependencies and the code (`equity_calculations.py`) that powers
  the workhorse lambda function. For instructions on how to create this
  deployment package from scratch, please see
  `docs/creating_deployment_pkg_with_geopandas.md`.

  - `environment.yml`: The conda environment file which lists all dependencies
    and packages used to run the R and python scripts in `scripts/`. You can use
    this to recreate our conda environment, though beware that most of the
    packages listed are OSx/Mac specific. 
  
  - `docs/`
    - `creating_deployment_pkg_with_geopandas.md`: Instructions for setting up
      an AWS lambda deployment package with the geopandas library and some
      other compiled dependencies from scratch. If you use the deployment package,
      you can import and use `geopandas` functions in your lambda code. We have 
      found this to be cost effective, and efficient way to do spatial 
      operations in AWS.
    - `create-deployment-pkg.sh`: A helper script that automates some of the
      steps laid out in `creating_deployment_pkg_with_geopandas.md`.

  - `reference-data/`: Contains all of the tool's reference data. Some
    subfolders may not exist when cloning, but are written out by `scripts/update-data/`
    - `2019/`: 
      * `acs_variable_definitions/`:
        * `poverty_population.csv`: CSV with manually checked ACS variable codes
          which correspond to human readable file names for the low-income population
          variables used in the tool
        * `total_population.csv`: CSV with manually checked ACS variable codes
          which correspond to human readable file names for the total population
          variables used in the tool
        * `under18_population.csv`: CSV with manually checked ACS variable codes
          which correspond to human readable file names for the child population
          variables used in the tool
      * `clean-acs-data/`: Contains cleaned ACS geography files written out by
        `scripts/update-data/01_download-and-clean-acs-data.R`
      * `city/`: city level precomputed statistics and tract files (for writeout
        to S3)
      * `county/`: county level precomputed statistics and tract files (for writeout
        to S3)
      * `state/`: state level precomputed statistics and tract files (for writeout
        to S3)
      * `national/`: national level precomputed statistics and tract files (for writeout
        to S3)


  - `scripts/`: 
    - `create-sample-data/`
      * `01_generate_sample_bike_data.R`: Generates sample dataset on bike share
       stations from Minneapolis, MN using the Nice Ride MN API.
      * `02_upload_sample_data_to_s3.R`: Uploads sample datasets from the
        `sample-data` folder into S3.
      * `03_impute_sample_data.R`: For a couple of the sample datasets, we
        impute some values in columns which we use as filters and weights in the
        tool, and then re-upload to S3.  
    - `lambda/`
      * `equity_calculations.py`: The key workhorse lambda function which
        performs geographic and demographic disparity calculations for datasets.
        This lambda function is triggered whenever a file is written to the
        `input-data/` prefix of the <equity_file_bucket>. At a high level, this
        lambda function reads in user uploaded data,
        determines the dataset's source geography (by performing a spatial join on a small
        sample of the data), reads in the geography's demographic and geographic data, and
        calculates disparity scores. It then writes out the outputs into S3 to be
        returned to the user by the API. Because this is a lambda function, the main
        code logic is contained in the `handler` function. This code is only meant to
        work in conjunction with the other AWS infrastructure setup by `template.yml`.
        If you do not want to use our AWS infrastructure to run the equity calculations,
        you will need to modify this script a good deal. To start with, you'd need to rewrite
        the data readin/writeout functions to read/write data locally instead of from
        S3, and remove most of the `update_status_json` calls. If you want to create a
        version of this script to calculate disparity scores locally and need help
        modifying this script to meet your needs, please reach out to us!

      * `getstatus_and_getfile.py`: Lambda function which checks status of
        existing jobs and gets data for completed jobs. This lambda function is
        connected to an API Gateway with different endpoints for checking status
        and getting completed files. See `template.yml` for the exact endpoint
        configurations. This API works closely with the internal frontend
        API to get the status of existing jobs and get data for
        completed jobs.
      
    - `update-data/`: Updates tool data
      * `README.md`: Contains more specific instructions on exactly how to
        generate or update the data for a year. 
      * `main-update-data-script.R`: Main handler update script which
        `source()`'s scripts `01` to `03`. You can set the year parameter in
        this script to choose which year of ACS data to update. For the chosen
        year, you need to ensure that you have manually created and checked ACS 
        variable definition files at
        `reference-data/{year}/acs_variable_definitions/`
      * `01_download-and-clean-acs-data.R`: Downloads and cleans ACS data, then
        writes to `reference-data/{year}/cleaned-acs-data/`
      * `02_generate-baseline-proportions.R`: Generates baseline proportions
        based on the denominator geography and writes out tool specific files to
        `reference-data/{year}/{geography}/*`
      * `03_upload_ref_data_to_s3.py`: Uploads the contents of
        `reference-data/{year}/{geography}/*` into the S3 infrastructure bucket
        as CSVs and pickles (pickled files used for performance speedups).



