[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
# Spatial Equity Data Tool

This repo contains some example files, code and data which power Urban's [Spatial Equity Data Tool](https://apps.urban.org/features/equity-data-tool/). We also have [robust
 public documentation](https://ui-research.github.io/sedt_documentation/) 
 describing how the tool works. 

All of the tool's infrastructure is deployed using GitHub Actions (a CI/CD
service). If you are just interested in the tool methodology/calculations and/or
don't want to setup your own AWS infrastructure for the tool, you can skip most
of the files in this repo and head straight to
`scripts/lambda/equity_calculations.py`. Make sure to read the description of
that script below to get a sense of the changes you will have to make.

# License

This code is licensed under the GPLv3 license.
# Prerequisites
## Environment Variables

Environment variables need to be set in an `.env` file in the root of this repo. 
This file will be used by scripts that are run locally on your machine. 
The `.env` file should look like:
   ```
   Stage=XXX
   equity_file_bucket=XXX
   equity_infrastructure_bucket=XXX
   equity_file_bucket_region=XXX
   ```
   By default the `.env` file is added to the gitignore so it will need to be
   added manually to your repo locally. For
   confidentiality reasons, **we have inserted several placeholder values within
   the files in this repo that look like `<some-name>` that you will need to
   provide yourself.**


## Staging and Production 
Updates to the `staging` and `production` branches in this repo will trigger a GitHub Actions workflow to deploy the updates to the relevant resources on AWS. GitHub Actions will display a green check when the updates are deployed. If there was an error deploying the updates, GitHub Actions will email the person who triggered the workflow. 

The AWS resources are managed through separate staging and production CloudFormation stacks. 

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
that is a limitation of conda. The packages are also laid out in
`packagelist.txt` (Mac) and `packagelist-win.txt` (Windows)
and you can recreate the conda environment with:

`conda create --name <env> --file packagelist.txt`

OR

`conda create --name <env> --file packagelist-win.txt`

Key packages to install are
`geopandas` version >= 0.9.0,`pandas` version = 1.3.0, and `boto3`.

As a backup, here is a manual list of python packages to install in a conda
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
 
Below is an explanation of all folders and files in this repo. 
  - `.env`: A gitignored env file which contains environment variables used by
    scripts in the `scripts/` folder. Will need to be placed there manually when
    you first clone this repo. 

  - `.github/workflows/sam-pipeline.yml`: A gitignored fole containing the GitHub Action workflow that builds and 
    deploys the SAM application. This is triggered by updates to the `staging` and
    `production` branches. The workflow first defines a `STAGE` and `STACK_NAME` 
    parameter based on which branch was updated. It then uses the relevant IAM role 
    to build and deploy the application to AWS. 

  - `template.yml`: A gitignored [SAM template](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-specification-template-anatomy.html) that defines all the AWS resources
    needed for the backend operation of the Spatial Equity Data Tool. This
    includes 2 S3 buckets, 4 Lambda functions, an API Gateway, a state machine, 
    an EventBridge rule, and appropriate permissions for all of them. Note that 
    we make use of some predefined roles (`equity-assessment-tool-lambda-role `, 
    `sedt-invoke-stepfunction-role`, and `sedt-stepfunction-role`) that we have 
    manually created and are only available in Urban's AWS account. You can create 
    the policies yourself in this template file, but it was really finicky and just 
    an absolute pain to get setup properly, so we resorted to manually creating the 
    policies in the IAM console and then referencing them in this template file.

  - `samconfig.toml`: A gitignored [SAM configuration file](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-config.html) 
    that is referenced when building and deploying the SAM application. This is also 
    where AWS resources tags are defined and applied to all resources in the stack. 

  - `environment.yml`: A gitignored conda environment file which lists all dependencies
    and packages used to run the R and python scripts in `scripts/`. Since this file has not been pushed to GitHub, we recommend using one fo the following two files:
  
    - `packagelist.txt`: A list of all packages in the Conda environment for OSx users. You can recreate the
      conda environment by running `$ conda create --name <env> --file
      packagelist.txt` from the root of the repo. Note that the R packages
      required to run the code in `scripts/update-data` may not show up correctly
      in this file and may need to still be manually installed into the conda
      environment.  
    - `packagelist-win.txt`: A list of all packages in the Conda environment for Windows users. You can recreate the
      conda environment by running `$ conda create --name <env> --file
      packagelist-win.txt` from the root of the repo. Note that the R packages
      required to run the code in `scripts/update-data` may not show up correctly
      in this file and may need to still be manually installed into the conda
      environment.  
  
  - `speed-test-data/`: Gitignored folder which contains datasets of varying
    sizes used to perform speed tests. CSVs are written into this folder by
    `scripts/run-tests/run_speed_tests.R`.

  - `edge-test-data/`: Gitignored folder which contains datasets used to perform
    edge case testing. CSV's are written into this folder by
    `scripts/run-tests/run_edge_case_tests.R`.

  - `sample-data/`: Folder which contains all of the sample datasets used in the
    tool, with the exception of the New Orleans data as it is bigger than
    Github's 100 MB file size limit. It also contains JSONS for each sample
    dataset which specify any default filters and weights which are used by the
    frontend.

  - `reference-data/`: Contains all of the tool's reference data. Some
    subfolders may not exist when cloning, but are written out by `scripts/update-data/`. The `reference-data/` directory will have a subdirectory for each ACS 5-year Survey end year currently supported by the tool. That is currently 2019, 2021, and 2022.
    - `{year}/`: 
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
      - `equity-calculations`
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
        * `Dockerfile`: The Dockerfile that defines a custom Lambda container image with the requisite Python packages needed by the calculator Lambda function. See [here](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html) for more information on using Lambda container images. 
        * `requirements.txt` The Dockerfile installs the packages in the `requirements.txt` file in this  directory. We specify that we want the equity-calculator Lambda function to use this image in the `template.yml` file using the `PackageType` property and by specifying the `DockerFile`, `DockerContext`, and `DockerTag` metadata. 

      * `check_files_to_wait_on.py`: Lambda function which determines whether the user-submitted request is from the public API and, if so, whether it includes custom demographic and geographic files. The function looks for files with the `input-data/` prefix in the <equity_file_bucket>. It is the first of three files in the step function. To read more about the step function, see [this blog post](https://medium.com/urban-institute/building-a-public-api-for-the-spatial-equity-data-tool-4f4d83c6f7cb). 

      * `determine_uploaded_files.py`: Lambda function which determines whether the files identified in `check_files_to_wait_on.py` have been successfully uploaded to <equity_file_bucket>. This function checks for the identified files with the `input-data/` prefix in the <equity_file_bucket>. If the files have not finished uploading, the step function waits and then triggers this Lambda function again. This cycle repeats until this lambda function determines that all of the files have finished uploading.
        
      * `getstatus_and_getfile.py`: Lambda function which checks status of existing jobs and gets data for completed jobs. This lambda function is connected to an API Gateway with different endpoints for checking status and getting completed files. See `template.yml` for the exact endpoint configurations. This API works closely with the internal frontend API to get the status of existing jobs and get data for completed jobs.
      
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



Please provide feedback by opening [GitHub Issues](https://github.com/UrbanInstitute/ui-equity-tool/issues) or contacting us at [sedt@urban.org](mailto:sedt@urban.org).


