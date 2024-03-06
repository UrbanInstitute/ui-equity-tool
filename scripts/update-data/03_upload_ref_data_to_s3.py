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


###-----Define helper functions ---------
def place_pkl_in_s3(object, region, bucket, key):
    """Takes a python datframe and store as a pkl file in S3

    Args:
        object (Obj): Any python object, but in our case we set to a geodataframe
        region (str): Name of AWS region that bucket is in
        bucket (str): Name of s3 bucket to upload to
        key (str): Key for s3 objet
    """    
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=pickle.dumps(object))

# Define function to write out all tool data files to s3:

def write_out_tool_data_to_s3(year, output_bucket, output_region):
    """Write out tool data to S3 as pickles or CSVs

    Args:
        year (str): Year of ACS data to write out
        output_bucket (str): Name of output S3 infrastructure buket
        output_region (str): Name of AWS region assoiated with bucket, should be
        "us-east-1"
        
    """    
    # geo_pairs (dict): Dictionary of geography pairs used in tool
    geo_pairs = {"national": ["state", "tract"], "state": ["county", "tract"], "county": ["tract"], "city": ["tract"] }
    
    # Iterate all combinations of denominator geography (level of analysis) and numerator geography
    # and write files to s3
    for geo_denom in geo_pairs.keys():
        for geo_num in geo_pairs[geo_denom]:

            # get all filenames for numerator-denominator geography pair
            place_geo_fname = os.listdir(
                "reference-data/" + year + "/" + geo_denom + "/" + geo_num + "/"
            )
            print(geo_denom, geo_num)

            # write each filename to s3 as pkl
            for filename in place_geo_fname:
                # read in the tracts file for the place, and upload to S3

                gdf_geo = gpd.read_file(
                    "reference-data/" + year + "/" + geo_denom + "/" + geo_num + "/" + filename
                )

                # Need to explicitly transform to EPSG 4326 bc geojsosn should
                # previosuly be in EPSG 4269 (NAD83) to allow for accurate spatial
                # operations in previous spatial operations. But techniaclly this is
                # wrong bc the GeoJSON spec only allows CRS 4326. So for the tool,
                # we explicitly reproject to
                gdf_geo = gdf_geo.to_crs("EPSG:4326")

                # gdf_geo = gpd.GeoDataFrame(gdf_geo, crs=4326)
                pkl_fname = filename.replace(".geojson", ".pkl")

                # Place pkl of census tracts in S3
                place_pkl_in_s3(
                    gdf_geo,
                    region=output_region,
                    bucket=output_bucket,
                    key="reference-data/" + year + "/" + geo_denom + "/" + geo_num + "/" + pkl_fname,
                )

        # read in precomputed stats for denominator geo
        print("precomputed {geo_denom}")
        precomputed_geo_fname = os.listdir(
                "reference-data/" + year + "/" + geo_denom + "/precomputed_stats/"
            )
        
        # write each filename to s3 as pkl
        for filename in precomputed_geo_fname:
            precomputed_stats = pd.read_csv(
                "reference-data/"
                + year
                + "/"
                + geo_denom
                + "/precomputed_stats/"
                + filename
            )
            pkl_fname = filename.replace(".csv", ".pkl")

            # Place pkl of precompted stats in S3
            place_pkl_in_s3(
                precomputed_stats,
                region=output_region,
                bucket=output_bucket,
                key="reference-data/"
                + year
                + "/"
                + geo_denom
                + "/precomputed_stats/"
                + pkl_fname,
            )

        # Place pkl of all polygons in s3 (national tool doesn't have polygons because
        # its just one polygon of the US)
        if geo_denom != "national":
            all_poly = gpd.read_file(
                        "reference-data/" + year + "/" + geo_denom + "/all_polygons.geojson"
                    )
                    
            all_poly = all_poly.to_crs("EPSG:4326")
            pkl_fname = "all_polygons.pkl"

            place_pkl_in_s3(
                all_poly,
                region=output_region,
                bucket=output_bucket,
                key="reference-data/" + year + "/" + geo_denom + "/" + pkl_fname,
            )
        
        # For state, write out generalized 20m shp file which is returnd to frontend
        # in order to limit file size of national level tool API responses
        if geo_denom == "state":
            all_poly_generalized = gpd.read_file(
                        "reference-data/" + year + "/" + geo_denom + "/all_polygons_20m_generalized.geojson"
                    )
                    
            all_poly_generalized = all_poly_generalized.to_crs("EPSG:4326")
            pkl_fname_generalized = "all_polygons_20m_generalized.pkl"

            place_pkl_in_s3(
                all_poly_generalized,
                region=output_region,
                bucket=output_bucket,
                key="reference-data/" + year + "/" + geo_denom + "/" + pkl_fname_generalized,
            )

