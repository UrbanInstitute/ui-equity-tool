import geopandas as gpd
import pandas as pd
import numpy as np
import json
import time
import timeit
import boto3
import pickle
import io
import os
import glob
import sys

###------Setup Parameters------
year = str(2018)
output_bucket = "{INSERT_BUCKET_HERE}"
output_region = "{INSERT_REGION_HERE}"


###-----Define helper functions ---------
def place_pkl_in_s3(object, region, bucket, key):
    # function that takes a python object and stores as a pkl file in s3
    # INPUT:
    #     object: any python object. We only set object = a gpdataframe
    #     bucket: name of s3 bucket to uplaod to
    #     key: key for s3 object
    #     region = region for s3 bucket
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=pickle.dumps(object))


###-----Transfer files to S3----


# place pickled big_places file in S3
big_places = gpd.read_file(
    "reference-data/" + year + "/big_places_us_manual_join_1p_cutoff.geojson"
)
big_places = gpd.GeoDataFrame(big_places, crs=4326)
big_places_in_s3 = big_places[["place_geoid", "cleaned_name", "geometry"]]

place_pkl_in_s3(
    big_places_in_s3,
    output_region,
    output_bucket,
    "reference-data/" + year + "/big_places_us_manual_tract_join_1p_cutoff.pkl",
)

# Place all tract and SD files in S3 (for each census place)
place_tracts_fname = os.listdir("reference-data/" + year + "/tracts_by_place/")
counter = 0

for filename in place_tracts_fname:
    # read in the tracts file for the place, and upload to S3

    gdf_tracts = gpd.read_file(
        "reference-data/" + year + "/tracts_by_place/" + filename
    )
    gdf_tracts = gpd.GeoDataFrame(gdf_tracts, crs=4326)
    pkl_fname = filename.replace(".geojson", ".pkl")

    precomputed_stats = pd.read_csv(
        "reference-data/"
        + year
        + "/precomputed_place_stats/"
        + filename.replace("geojson", "csv")
    )
    print(filename)
    # Place pkl of census tracts in S3
    place_pkl_in_s3(
        gdf_tracts,
        region=output_region,
        bucket=output_bucket,
        key="reference-data/" + year + "/tracts_by_place/" + pkl_fname,
    )

    # Place pkl of precompted stats in S3
    place_pkl_in_s3(
        precomputed_stats,
        region=output_region,
        bucket=output_bucket,
        key="reference-data/" + year + "/precomputed_place_stats/" + pkl_fname,
    )

