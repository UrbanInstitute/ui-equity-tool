import geopandas as gpd
import pandas as pd
import numpy as np
from urllib.parse import unquote_plus
from io import StringIO
import datetime
import json
import shapely
import boto3
import io
import sys
import pickle
import os
import datetime
import fiona.crs
import fiona
import math


data_bucket = os.environ["DATA_BUCKET"]
file_bucket_region = os.environ["FILE_BUCKET_REGION"]
data_bucket_region = os.environ["DATA_BUCKET_REGION"]
################ Helper Functions ###################


def create_and_write_status_json(
    system_key, bucket, file_bucket_region=file_bucket_region
):
    # Creates and write update JSON to update-data bucket on S3. This
    # updated json will be returned by the getstatus endpoint
    # INPUT:
    #   system_key: system file key on S3
    #   bucket: bucket to write to. This should be set by env vars at top of
    #       script
    # OUTPUT:
    #   update_json: A json (ie python list) that has already been written to S3
    #       with the appropriate system_key. (list)

    update_key = "update-data/" + system_key + ".json"
    s3 = boto3.client("s3", region_name=file_bucket_region)

    # This JSON and its values are hardcoded at the beginning, and throughout the
    # script, will be dynamically updated and pushed to S3
    update_json = {
        "updates": {
            "started_processing": True,
            "read_in_file": False,
            "num_rows_file": None,
            "num_filter_rows_dropped": None,
            "num_null_rows_dropped": None,
            "num_rows_dropped_total": None,
            "num_rows_for_processing": None,
            "num_rows_processed": None,
            "num_rows_final": None,
            "city_used": None,
            "sjoin_started": False,
            "finished": False,
            "error-messages": False,
        },
        "warnings": {
            "multiple_cities_flag": False,
            "num_null_latlon_rows_dropped": None,
            "num_null_filter_rows_dropped": None,
            "num_null_weight_rows_dropped": None,
            "num_out_of_city_rows_dropped": None,
            "multiple_cities_list": None,
        },
        "error-messages": {
            "form-data-parameter-validation-failed": False,
            "data_readin_error": False,
            "df_conversion_to_gdf_failed": False,
            "filter_coltypes_mismatch": False,
            "weight_coltypes_mismatch": False,
            "all_rows_filtered": False,
            "pts_not_in_us_city": False,
            "sjoin_failed": False,
        },
    }

    s3.put_object(
        Body=(bytes(json.dumps(update_json).encode("UTF-8"))),
        Bucket=bucket,
        Key=update_key,
        ACL="public-read",
    )

    return update_json


def update_status_json(system_key, status_json, bucket):
    # Places status JSON with update, warning, and error messages in S3
    # update-data folder:
    # INPUT:
    #   system_key: system file key on S3
    #   bucket: bucket to write to.
    #   new_json: None if creating update json for the first time. Or value of
    #       new_json if you want to update the value of this json
    # OUTPUT:
    #   None, but writes out the status_json to S3

    # Create S3 key from file_key
    update_key = "update-data/" + system_key + ".json"
    s3 = boto3.client("s3", region_name=file_bucket_region)

    s3.put_object(
        Body=(bytes(json.dumps(status_json).encode("UTF-8"))),
        Bucket=bucket,
        Key=update_key,
        ACL="public-read",
    )


def read_in_point_data(
    file_key, lon_name, lat_name, status_json, system_key, bucket,
):
    # Reads in point data from input-data folder in S3. It tries 3 CSV encodings
    # before erroring out
    # INPUT:
    #   file_key: File key on S3 (parsed from S3 put event)
    #   system_key: system file key on S3
    #   bucket: bucket to write to. This should be set by env vars at top of
    #        script
    #   new_json: None if creating update json for the first time. Or value of
    #       new_json if you want to update the value of this json
    # OUTPUT:
    #   point_data: a geopandas geodataframe of the point data uplaoded by the
    #       user. Each row should be a geographic point (gpd.gdf)

    if file_key.endswith("csv"):

        try:
            # Try reading in point data csv. It if fails, record
            # data_readin_error
            point_data = read_in_csv_from_s3_as_gdf(
                key=file_key,
                system_key=system_key,
                status_json=status_json,
                bucket=bucket,
                lon_column=lon_name,
                lat_column=lat_name,
            )
        except:
            status_json["error-messages"]["data_readin_error"] = True
            status_json["updates"]["error-messages"] = True
            update_status_json(system_key, status_json, bucket=bucket)
            raise ValueError("Data import from S3 not successful")

    if file_key.endswith("geojson"):
        try:
            point_data = select_geojson_as_gdf(bucket, file_key)
            if point_data.crs == None:
                point_data.crs = "EPSG:4326"
        except:
            status_json["error-messages"]["data_readin_error"] = True
            update_status_json(system_key, status_json, bucket=bucket)
            raise ValueError("Data import from S3 not successful")

    return point_data


def convert_coltypes(df, col_name, col_type, status_json, system_key, bucket):
    # Converts coltypes of CSV into the frontend provided column types.
    # INPUT:
    #   df: point_data dataframe (pd.df)
    #   col_name: Name of column (str)
    #   col_type: Type of columns, must be one of "number", "string", and "date"
    #     (str)
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    # OUTPUT:
    #   df: a dataframe with the appropriate column types converted (df)
    try:
        if col_type == "number":
            df[col_name] = pd.to_numeric(df[col_name])
        if col_type == "string":
            df[col_name] = df[col_name].astype(str)

            # When using astype(str) to convert columns to chr, np.nans will be
            # converted to 'nan'/ This is an open bug in v1.0.0 of pandas:
            # https://github.com/pandas-dev/pandas/issues/25353
            # So for now we manually convert those nan value back
            # to np.nan. This should be safe because `nan` strings are one of
            # the NA values that are automatically turned into np.nans by
            # pd.read_csv() so this should only affect true nan columns
            df[col_name] = df[col_name].replace("nan", np.nan)
        if col_type == "date":
            df[col_name] = pd.to_datetime(df[col_name])

        return df
    except ValueError:
        # If conversion wasn't successful, then throw
        # filter_coltypes_mismatch_error, record problematic column, and turn
        # error flag to True in `updates`
        status_json["error-messages"]["filter_coltypes_mismatch"] = True
        status_json["error-messages"][
            "filter_coltypes_mismatch_colname"
        ] = col_name
        status_json["updates"]["error-messages"] = True
        update_status_json(system_key, status_json, bucket=bucket)

        raise ValueError("Filter column types cant be converted")


def convert_coltypes_for_filter_and_weights(
    df, filters, weight, system_key, status_json, bucket
):
    # Converts weight column to numeric and filter column to
    # appropriate column type determined by frontend. The filter coltype
    # transformation is a wrapper of `convert_coltypes` function.
    # INPUT
    #   df: point_data dataframe (pd.df)
    #   filters: Filters from frontend form-data (list of dictionaries)
    #   weight: Name of weight column in data (str)
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    # OUTPUT
    #   df: a dataframe with the weight and filter columns in the appropriate
    #       column types (df)

    # Try converting weight column to numeric. If we can't, error out and record
    # in status_json
    try:
        df[weight] = pd.to_numeric(df[weight])
    except:
        status_json["error-messages"]["weight_coltypes_mismatch"] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(system_key, status_json, bucket=bucket)
        raise ValueError("Weight column type cant be converted to numeric")

    # Try converting the filter columns into the selected column types. If we
    # can't for any of the filters, error out and record in status_json
    for x in filters:
        col_type = x["filter_type"]
        col_name = x["filter_column"]

        df = convert_coltypes(
            df=df,
            col_name=col_name,
            col_type=col_type,
            status_json=status_json,
            system_key=system_key,
            bucket=bucket,
        )

    return df


def drop_null_filters_and_weights(
    df, filters, weight, system_key, status_json, bucket
):
    # Drops null values in filter and weight columns. When we read in
    # the csv into pandas, we use the pandas default NA coercer, which treats
    # many values as NA. These values are noted in the Tool FAQ.
    # INPUT
    #   df: point_data dataframe (pd.df)
    #   filters: Filters from frontend form-data (list of dictionaries)
    #   weight: Name of weight column in data (str)
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    # OUTPUT:
    #   df: a datafrmae where the null filter and weight columns are dropped. (df)
    filter_col_list = [x["filter_column"] for x in filters]

    nrow_init = df.shape[0]

    # Filter out and record null filter columns
    df = df.dropna(subset=filter_col_list)
    nrow_after_filter_nulls = df.shape[0]
    status_json["warnings"]["num_null_filter_rows_dropped"] = (
        nrow_init - nrow_after_filter_nulls
    )

    # Filter out null weight columns
    df = df.dropna(subset=[weight])

    # Also filter out rows that have weight of 0. These are treated as NA
    df = df[df[weight] != 0]

    # Record null weight columns (including weight 0 columns)
    nrow_after_filter_nulls_wts = df.shape[0]
    status_json["warnings"]["num_null_weight_rows_dropped"] = (
        nrow_after_filter_nulls - nrow_after_filter_nulls_wts
    )

    # Record total number of null rows dropped, and total rows dropped
    status_json["updates"]["num_null_rows_dropped"] = (
        status_json["warnings"]["num_null_latlon_rows_dropped"]
        + status_json["warnings"]["num_null_filter_rows_dropped"]
        + status_json["warnings"]["num_null_weight_rows_dropped"]
    )

    update_status_json(
        system_key=system_key, status_json=status_json, bucket=bucket
    )

    return df


def select_geojson_as_gdf(bucket, key):
    # Reads in geojson from s3 as a gdf
    # INPUT:
    #     bucket: s3 bucket, string
    #     key: full s3 key including prefixes, string
    # OUTPUT:
    #     gdf: a gdf
    s3 = boto3.client("s3", region_name=file_bucket_region)
    obj = s3.get_object(Bucket=bucket, Key=key)
    json_g = json.loads(obj["Body"].read().decode("utf-8"))
    gdf = gpd.GeoDataFrame.from_features(json_g["features"])
    gdf = gdf[["geometry"]]
    return gdf


def convert_date_string(string):
    # Converts date string that frontend uploads in mm/dd/yy format to YYYYmmdd
    # as that is the format Python needs dates in to be used with query (for
    # filtering)
    # INPUT:
    #   string: date string (date time string)
    # OUTPUT:
    #   a string with the date in YYYYmmdd format. (str)
    return datetime.datetime.strptime(string, "%m/%d/%Y").strftime("%Y%m%d")


def multi_text_constructor(filter_comp):
    # Conversts frontend comparison constructors to appropriate
    # pandas constructors to use with multile text values (ie filter to rows
    # where a column value matches some values in a list).
    # INPUT:
    #   filter_comp: filter comparison from frontend, either '==' or '!='. (str)
    # OUTPUT:
    #   either "in" or "not in" (str)

    if filter_comp == "==":
        return "in"
    elif filter_comp == "!=":
        return "not in"
    else:
        raise ValueError("Text Fileter does not contain valid comparison")


def wrap_multi_text_in_quotes(filter_val):
    # Wraps comma separated text in filters in appropriate
    # quotes. Need to be deliberate about using single quote marks vs double
    # quote marks this so that the quoting will work with pd.query.
    # INPUT:
    #   filter_val: Filter value from frontend (str)
    # OUTPUT:
    #   quoted_text: single string with comma separated text values in
    #       appropriate quotes (str)

    quoted_text = ",".join(['"' + x + '"' for x in filter_val.split(",")])
    return quoted_text


def generate_filter_strings(filter_list):
    # Generates list of filters, where each filter is a string, for easy feeding
    # into the pd.query function.We enclose all column names in backticks in
    # case user columns have spaces/operators in them.
    # INPUT:
    #   filter_list: List of filters from frontend (list of dictionaries)
    # OUTPUT:
    #   all_filter_queries: list of filters correctly formatted for feeding in to
    #       pd.query. (list of strings)

    numeric_filters = [
        "`"
        + x["filter_column"]
        + "`"
        + x["filter_comparison"]
        + str(x["filter_val"])
        for x in filter_list
        if x["filter_type"] == "number"
    ]
    string_filters = [
        "`"
        + x["filter_column"]
        + "`"
        + x["filter_comparison"]
        + '"'
        + str(x["filter_val"])
        + '"'
        for x in filter_list
        if (x["filter_type"] == "string") & ("," not in str(x["filter_val"]))
    ]

    # Construct filters for strings of comma separated values (ie column in (x1,
    # x2, x3)). This needs to be handled differently than regular string
    # filters as we need to allow for or conditions and therefore use `in` and
    # `not in` operators.
    multi_string_filters = [
        "`"
        + x["filter_column"]
        + "`"
        + multi_text_constructor(x["filter_comparison"])
        + "("
        + wrap_multi_text_in_quotes(str(x["filter_val"]))
        + ")"
        for x in filter_list
        if (x["filter_type"] == "string") & ("," in str(x["filter_val"]))
    ]
    # single_date_filters = [x for x in filters if x['filter_comparison'] == "singleDate" ]
    # date_range_filters = [x for x in filters if x['filter_comparison'] == "dateRange" ]

    single_date_filters = [
        "`"
        + x["filter_column"]
        + "`"
        + "=="
        + convert_date_string(x["filter_val"])
        for x in filter_list
        if x["filter_comparison"] == "singleDate"
    ]

    date_range_filters = [
        "`"
        + x["filter_column"]
        + "`"
        + ">="
        + convert_date_string(x["filter_val"].split("-")[0])
        + " & "
        + x["filter_column"]
        + "<="
        + convert_date_string(x["filter_val"].split("-")[1])
        for x in filter_list
        if x["filter_comparison"] == "dateRange"
    ]

    all_filter_queries = (
        numeric_filters
        + string_filters
        + multi_string_filters
        + single_date_filters
        + date_range_filters
    )
    return all_filter_queries


def apply_filters(df, filter_list_string, system_key, status_json, bucket):
    # Applies all filters to the data. Note order of filters applied is numeric,
    # string, text-separeted string,singleDate, and dateRange
    # INPUT
    #   df: point_data dataframe (pd.df)
    #   filter_list_string: Modified list of filters to feed into pd.query (list
    #       of strings)
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    # OUTPUT:
    #   df: a dataframe with user provided filters applied (df)

    nrow_before_filtering = df.shape[0]

    # Apply all filters to point data
    for f in filter_list_string:
        df = df.query(f)

    nrow_after_filtering = df.shape[0]

    if nrow_after_filtering == 0:
        status_json["updates"]["error-messages"] = True
        status_json["error-messages"]["all_rows_filtered"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError
    else:
        status_json["updates"]["num_filter_rows_dropped"] = (
            nrow_before_filtering - nrow_after_filtering
        )
        return df


def get_place_info(df, place_df, system_key, status_json, bucket):
    # Compute the Census place GEOID of a geodataframe with lat/lons using a
    # spatial join. If there are more than 50 rows in the gdf, this function
    # will take a 5% sample, before computing the place geoid. If there is more
    # than 1 city in the data, this function will  return the most frequently
    # appearing city in the sample.
    # INPUT:
    #   df: input gdf of lat/lon points
    #   place_df: gdf that lists polygons of all Census places
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    # OUTPUT:
    #   result: One row dataframe which is the place_df but filtered to the most
    #       frequently occurring city in the df (gpd.gdf)

    if df.shape[0] > 50:
        # 5% sample of points to reduce spatial join processing time
        npnts = int(round(0.05 * df.shape[0]))
        dfs = df.sample(npnts)
    else:
        dfs = df

    # Spatially join df to place_df and count place_geoids, sorted in desc order
    x = (
        gpd.sjoin(dfs, place_df, op="within", how="left")
        .groupby("place_geoid")["place_geoid"]
        .count()
        .sort_values(ascending=False)
    )

    if len(x) == 0:
        # If 0 points are matched to a city, most likely no points in data are within
        # big_places_shapefiles (ie a big US city with pop > 50k).
        # Before erroring out, lets make sure data wasn't badly coded. One common
        # data entry error is that lon/lat columns are flipped. So lets unflip
        # and try again
        df.geometry = df.geometry.map(
            lambda point: shapely.ops.transform(lambda x, y: (y, x), point)
        )

        x = (
            gpd.sjoin(dfs, place_df, op="within", how="left")
            .groupby("place_geoid")["place_geoid"]
            .count()
            .sort_values(ascending=False)
        )

    if len(x) == 1:
        # If there was only one city, then record it and return the city

        # Get city and write out to update JSON
        place = x.index[0]
        result = place_df[place_df.place_geoid.isin([place])]

        used_city = result.cleaned_name.values[0]

        status_json["updates"]["city_used"] = used_city
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )

        return result
    elif len(x) == 0:
        # If there were still no cities even after switching lat/lon columns,
        # record error in json and raise ValueError
        status_json["updates"]["error-messages"] = True
        status_json["error-messages"]["pts_not_in_us_city"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("Points not in a US city with population over 50,000")
    else:
        print(
            "There were multiple census places in the data, returning the most common one"
        )
        place = x.index[0]

        result = place_df[place_df.place_geoid.isin([place])]

        used_city = result.cleaned_name.values[0]
        all_cities = place_df[
            place_df.place_geoid.isin(x.index)
        ].cleaned_name.values.tolist()

        # Add multiple cities flag to JSON, list the cities, and write out main
        # city used
        status_json["warnings"]["multiple_cities_flag"] = True
        status_json["warnings"]["multiple_cities_list"] = all_cities
        status_json["updates"]["city_used"] = used_city
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )

        return result


def read_in_pkl_from_s3_as_gdf(key, bucket=data_bucket):
    # Reads in a pickled python file from s3 as a gdf.
    # INPUT:
    #     bucket: s3 bucket (str)
    #     key: full s3 key including prefixes (str)
    # OUTPUT:
    #     data: the pickled data (gpd.gdf)

    s3 = boto3.client("s3", region_name=data_bucket_region)
    response = s3.get_object(Bucket=bucket, Key=key)
    data = pickle.loads(response["Body"].read())
    return data


def try_readin_with_diff_encodings(s3_object):
    # Tries reading in a CSV from S3 with three encodings (utf-8, utf-16, and
    # latin1). If that doesn't work, then records a data_readin_error and errors
    # out,
    # INPUT:
    #   s3_object: path to s3
    try:
        df = pd.read_csv(io.BytesIO(s3_object["Body"].read()), encoding="utf-8")
        return df
    except:
        try:
            df = pd.read_csv(
                io.BytesIO(s3_object["Body"].read()), encoding="ISO-8859-1"
            )
            return df
        except:
            try:
                df = pd.read_csv(
                    io.BytesIO(s3_object["Body"].read()), encoding="utf-16"
                )
                return df
            except:
                raise ValueError(
                    "No appropriate CSV encoding. CSV can't be read in"
                )


def read_in_csv_from_s3_as_gdf(
    key, system_key, status_json, bucket, lon_column="lon", lat_column="lat",
):
    # Function to read in a csv from s3 as a gdf. Assumes csv has columns
    # named 'lat' and 'lon'.
    # INPUT:
    #   key: full s3 key including prefixes of input-data CSV (str)
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    #   lon_column: name of longitude column in data (str)
    #   lat_column: name of latitude column in data (str)
    # OUTPUT:
    #     gdf: a gdf of the CSV stored in S3.

    # Try reading in data from S3. Or error out with data_readin_error
    try:
        s3 = boto3.client("s3", region_name=data_bucket_region)
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = try_readin_with_diff_encodings(obj)

        status_json["updates"]["read_in_file"] = True
    except:
        status_json["updates"]["read_in_file"] = False
        status_json["error-messages"]["data_readin_error"] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("No appropriate CSV encoding. CSV can't be read in")

    # Check if lon and lat columns are in data. If not, raise ValueError
    if not ((lon_column in df.columns) & (lat_column in df.columns)):
        status_json["error-messages"]["latlon_cols_not_in_data"] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("provided lat/lon columns not in data")

    # Create urbn lat/lon columns for creation of geometry columns. This way we
    # preserve the original lat/lon columns if user has filters on the lat/lon
    # columns.
    df["lon_urbninstitute"] = df[lon_column]
    df["lat_urbninstitute"] = df[lat_column]

    # Try converting lat/lon column to numeric. If we can't raise ValueError
    try:
        df.lon_urbninstitute = pd.to_numeric(df.lon_urbninstitute)
        df.lat_urbninstitute = pd.to_numeric(df.lat_urbninstitute)
    except:
        status_json["error-messages"][
            "lat_lon_cannot_be_converted_numeric"
        ] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("lat_lon_cannot_be_converted_numeric")

    # Get number of rows in raw data to report number of rows filtered out
    nrow_raw = df.shape[0]

    # Record raw number of rows in file
    status_json["updates"]["num_rows_file"] = nrow_raw

    update_status_json(
        system_key=system_key, status_json=status_json, bucket=bucket
    )

    # Filter out null lat/lon and write how many rows were filtered out
    df = df.dropna(subset=["lat_urbninstitute", "lon_urbninstitute"])

    # Record null rows filtered out in file
    status_json["warnings"]["num_null_latlon_rows_dropped"] = (
        nrow_raw - df.shape[0]
    )
    update_status_json(system_key, status_json, bucket=bucket)

    # Try converting df to gdf and return gdf
    try:
        geometry = [
            shapely.geometry.Point(xy)
            for xy in zip(df.lon_urbninstitute, df.lat_urbninstitute)
        ]
        gdf = gpd.GeoDataFrame(
            df.drop(["lon_urbninstitute", "lat_urbninstitute"], axis=1),
            crs="EPSG:4326",
            geometry=geometry,
        )
        del df
        return gdf
    except:
        status_json["updates"]["error-messages"] = True
        status_json["error-messages"]["df_conversion_to_gdf_failed"] = True
        update_status_json(system_key, status_json, bucket=bucket)
        raise ValueError


def read_in_tracts_by_place_from_s3(bucket, place_geoid, year="2018"):
    # Reads in all place tracts from S3 as a gdf, where every row is a Census
    # tract in the city.
    # INPUT:
    #   bucket: Bucket where tracts_by_place are stored. Should be data_bucket
    #       (str)
    #   place_geoid: 7 digit full geoid of census place. 7 digits = 2 digit state
    #       fips + 5 digit place geoid.(str)
    # OUTPUT:
    #   data: gdf of all place tracts

    s3 = boto3.client("s3", region_name=data_bucket_region)
    # response = s3.get_object(Bucket = bucket, Key =
    #                         "tracts_by_place_pickles/" + str(place_geoid) +
    #                         ".pkl")
    response = s3.get_object(
        Bucket=bucket,
        Key="reference-data/"
        + year
        + "/tracts_by_place/"
        + str(place_geoid)
        + ".pkl",
    )
    data = pickle.loads(response["Body"].read())
    # data.reset_index(inplace=True)
    return data


def perform_chunked_spatial_join(
    point_df,
    poly_df,
    weight_col,
    system_key,
    status_json,
    bucket,
    loop_cutoff=80000,
):
    # Perform chunked spatial join of user uploaded point data to all census
    # tracts in the Census place. We do a chunked spatial joins to limit the
    # aof RAM used for each spatial joins and stay under Lambda's RAM limits.
    # INPUT:
    #   point_df: geodataframe of user uploaded point data with all filters applied
    #       and null values removed (gdf).
    #   poly_df: geodataframe of all census tracts in place (gdf)
    #   weight_col: weight column (str)
    #   status_json: Status json to update with warnings/updates (str)
    #   system_key: System key to use for updating status_json (str)
    #   bucket: bucket to write status_json to. Should always be = trigger_bucket
    #     (str)
    #   loop_cutoff: limit for how many points to join in each 'chunk' (int)
    # OUTPUT:
    #   temp: a geodataframe which is the same as poly_df, but with an additional
    #       column named 'weighted_count' that contains the weighted count of points
    #       fell within each tract.

    # After this point all you need is the weight column, and the geometry
    # column, We've already finished useing the filters and lat/lon columns
    point_df = point_df[[weight_col, "geometry"]]

    # Create pandas series with index being the tract geoid and all values being
    # 0. This is the series we will add our weighted tract counts to, from every
    # iteration of the spatial join loop
    temp = pd.Series(data=0, index=poly_df["tract_geoid"].unique())

    # Record number of points not in US city. This is a counter that will be
    # added to in every iteration of the loop
    pts_not_in_us_city = 0
    nrow_processed = 0

    # Update status json with sjoin_started flag and num_rows_for_processing field
    status_json["updates"]["sjoin_started"] = True
    status_json["updates"]["num_rows_for_processing"] = point_df.shape[0]
    update_status_json(
        system_key=system_key, status_json=status_json, bucket=bucket
    )

    # Perform spatial joins in chunks of 80k
    try:
        while point_df.shape[0] > 0:
            joined_pts = (
                gpd.sjoin(
                    point_df[:loop_cutoff][[weight_col, "geometry"]],
                    poly_df.loc[:, ["tract_geoid", "geometry"]],
                    op="within",
                    how="left",
                )
                # .groupby("tract_geoid")[weight_col]
                # .sum()
            )

            # Update number of rows processed in status json
            nrow_processed = nrow_processed + joined_pts.shape[0]

            status_json["updates"]["num_rows_processed"] = nrow_processed
            update_status_json(
                system_key=system_key, status_json=status_json, bucket=bucket
            )

            # Add points without a joined tract_geoid (ie pts not in US city) to
            # the total count
            pts_not_in_us_city = (
                pts_not_in_us_city
                + joined_pts[joined_pts.tract_geoid.isnull()].shape[0]
            )

            # Group joined points by tract_geoid and sum across weight column
            temp1 = joined_pts.groupby("tract_geoid")[weight_col].sum()

            # Add weights to the temp pandas series. if the item is missing or
            # None, it treats it as 0 and adds 0 for that tract
            temp = temp.add(temp1, fill_value=0)
            point_df = point_df[loop_cutoff:]

        # After the loop, temp is a pandas series where the indices are all the
        # unique geoids in the city, and the values are the sum of the weighted
        # counts within each tract. We convert that to a dataframe and sort
        # values
        temp = pd.DataFrame(
            {"tract_geoid": temp.index, "weighted_counts": temp.values}
        ).sort_values(by=["weighted_counts"], ascending=False)

        # Perform left join with poly_df to retain demographic and baseline
        # information in poly_df
        temp = poly_df.merge(temp, on="tract_geoid")

        # Write out number of pts not in the city and final row count in status
        # json
        status_json["warnings"][
            "num_out_of_city_rows_dropped"
        ] = pts_not_in_us_city

        status_json["updates"]["num_rows_final"] = (
            status_json["updates"]["num_rows_for_processing"]
            - pts_not_in_us_city
        )

        status_json["updates"]["num_rows_dropped_total"] = (
            status_json["updates"]["num_rows_file"]
            - status_json["updates"]["num_rows_final"]
        )

        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )

        # Deleting objects to try to conserve RAM. Not sure if this is actually
        # freeing up memory, need to look more into garbage collection rules.
        del joined_pts
        del temp1
        # !: This is deleting an object in the global environment, not an object
        # !: fed into the function
        del point_df

        # Return temp which is poly_df + an additional column for weighted
        # count of rows within each tract
        return temp
    except:
        status_json["updates"]["error-messages"] = True
        status_json["error-messages"]["sjoin_failed"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("Spatial join unsuccessful")


def place_df_as_csv_in_s3(df, bucket, key):
    # Place a df in s3 as a csv
    # INPUT:
    #     df: a dataframe or geodataframe
    #     bucket: s3 bucket to write out to (str)
    #     key: full s3 key including prefixes (str)
    # OUTPUT:
    #     None, but writes object to s3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client("s3", region_name=file_bucket_region)
    s3.put_object(
        Body=csv_buffer.getvalue(), Bucket=bucket, Key=key, ACL="public-read"
    )


def place_df_as_json_in_s3(df, bucket, key):
    # Place a df in s3 as a JSON
    # INPUT:
    #     df: a dataframe or geodataframe
    #     bucket: s3 bucket, string
    #     key: full s3 key including prefixes, string
    # OUTPUT:
    #     None

    # If df is of type geopandas, coerce to pandas for using with `to_json`
    if isinstance(df, gpd.GeoDataFrame):
        df = pd.DataFrame(df)

    # Note we use oreint="records" to maintain the structure expected by the
    # frontend.
    json_data = df.to_json(orient="records", default_handler=str)
    s3 = boto3.client("s3", region_name=file_bucket_region)
    # We set public-read ACL as we will be sending these links to frontend for
    # user download.
    s3.put_object(
        Body=(bytes(json_data.encode("UTF-8"))),
        Bucket=bucket,
        Key=key,
        ACL="public-read",
    )


def calculate_sig_diff_baselines(temp_df, baseline_name):
    # Calculates difference between baseline column and data_prop columns. Also
    # computed whether the difference is significant at 95% level after taking
    # into variation in baseline proportions from ACS.
    # INPUT:
    #   temp_df: A dataframe where every row representes a census tract. Must
    #       contain a 'data_prop' column, the baseline_name column, and the
    #       baseline_name_sd column.
    #   baseline_name: name of baseline column to calculate differences for
    #       (str)
    # OUTPUT:
    #   temp_df: A modified version of the input_df, but now with two additional
    #       columns: diff_baseline, and sig_diff_baseline (df)

    diff_col_name = "diff_" + baseline_name.replace("_prop", "")
    sig_diff_col_name = "sig_" + diff_col_name

    # Add column which is difference between data_prop and baseline_prop column
    temp_df[diff_col_name] = temp_df.apply(
        lambda row: row["data_prop"] - row[baseline_name], axis=1
    )

    # Calculate if diff_prop is significant, ie whether the 95% confidence interval
    # for baseline_prop contains data_prop. Note we use the critical value of
    # 1.96 for the 95% confidence interval as outline by CH 7 of the ACS
    # handbook.
    temp_df[sig_diff_col_name] = np.where(
        (
            temp_df["data_prop"]
            <= (temp_df[baseline_name] - 1.960 * temp_df[baseline_name + "_sd"])
        )
        | (
            temp_df["data_prop"]
            >= (temp_df[baseline_name] + 1.960 * temp_df[baseline_name + "_sd"])
        ),
        "TRUE",
        "FALSE",
    )

    return temp_df


def create_demographic_vars_and_sd(df_data, df_precomputed, censusvar, list):
    # Specific helper function to create stats_df dataframe, only works with df_data = temp.
    # Adds city_value, data_value, and data_value_sd columns using weighted aggregation of
    # census variables in temp df.
    #   df_data: df to compute statistics on, need to be = temp
    #   df_precomputed: precomputed df of citywide statistics nad margins
    #   censusvar: Name of census var on which calculation is done
    #   list: list_of_dicts to append to. After this function is run, this list
    #       will be transformed into a dataframe
    # Output:
    #    None, but appends the created dictionary to list_of_dicts (which starts
    #       out as an empty list).

    # Note that for demographic bias, we always compare the data sum to the
    # population sum (pop_prop).

    # The df_precomputed dataframe already contains
    precomputed_row = df_precomputed[df_precomputed["census_var"] == censusvar]
    city_value = precomputed_row["city_value"].iloc[0]
    city_value_sd = precomputed_row["city_value_sd"].iloc[0]

    census_var_margin = censusvar + "_margin"

    data_value = df_data.apply(
        lambda row: row["data_prop"] * row[censusvar], axis=1
    ).sum()

    # Compute margin for citywide estimates using Census formulas for
    # count variables (ie the numerator and denominator that make up the
    # censusvar). Note that row['data_prop'] is treated as a constant so we just
    # multiply that by the margin. See formula 1 here:
    # https://www.census.gov/content/dam/Census/library/publications/2018/acs/acs_general_handbook_2018_ch08.pdf
    data_value_margin = df_data.apply(
        lambda row: (row["data_prop"] * row[census_var_margin]) ** 2, axis=1
    ).sum()
    # To get sd, divide margin by 1.645
    data_value_sd = (math.sqrt(data_value_margin)) / 1.645

    # Compute significance using Census formulas for significance (a Z-test).
    # See formula 3 here:
    # https://www.census.gov/content/dam/Census/library/publications/2018/acs/acs_general_handbook_2018_ch07.pdf

    test_statistic_num = data_value - city_value
    test_statistic_den = math.sqrt(data_value_sd ** 2 + city_value_sd ** 2)
    test_stat = test_statistic_num / test_statistic_den

    # We set confidence level at 95%, so critical test-stat value is 1.96
    # If it is, then set sig_diff = True, False otherwise
    sig_diff = abs(test_stat) > 1.96

    vals = {
        "census_var": censusvar,
        "data_value": data_value,
        "city_value": city_value,
        "diff_data_city": data_value - city_value,
        "data_value_sd": data_value_sd,
        "city_value_sd": city_value_sd,
        "sig_diff": sig_diff,
    }
    list.append(vals)


# Hardcode year as 2018 as that is the only data we have precompiled
year = "2018"


def handler(event, context):


    # spcl chars like `(` and `)` are URL encoded weirdly by the S3 event object
    # causing s3 key not found errors. We fix this by using unquote_plus
    file_key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
    trigger_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    request_id = event["Records"][0]["responseElements"]["x-amz-request-id"]

    # Get system key and form key. We conservatively replace first and last
    # occurrence in case user has "input-data" or ".csv" in the file name. Note
    # this should never happen as proxy API renames files using unix timestamps
    # but we do this ot jus be safe.

    # Replace first occurrence of input-data/ with form-data/
    s = file_key.replace("input-data/", "", 1)
    system_key = "".join(s.rsplit(".csv", 1))

    # Get form data and extract lat/lon names, weights and filters
    form_key = "form-data/" + system_key + ".json"

    # Create and write out update JSON which we will be writing future updates,
    # warnings, and errors to
    status_json = create_and_write_status_json(
        system_key, bucket=trigger_bucket
    )

    # Read in form-data and take note of filters, weights, and lat/lon names
    client = boto3.client("s3")
    result = client.get_object(Bucket=trigger_bucket, Key=form_key)
    form_data = json.loads(result["Body"].read().decode())

    try:
        lat_name = form_data["lat_column"]
        lon_name = form_data["lon_column"]
        filters = form_data["filters"]
        weight_name = form_data["weight"]
    except:
        status_json["error-messages"][
            "form-data-parameter-validation-failed"
        ] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(
            system_key=system_key,
            status_json=status_json,
            bucket=trigger_bucket,
        )
        raise ValueError("Form data parameter validation failed")

    ######################################
    ########## Readin Data ###############
    ######################################

    # Read in point data from S3
    point_data = read_in_point_data(
        file_key,
        lon_name,
        lat_name,
        status_json,
        system_key,
        bucket=trigger_bucket,
    )

    # Create weight column if it doesn't exist and assign weight of 1 for all
    # rows Note: frontend will send blank strings if there are no selected
    # weight columns
    if weight_name == "":
        point_data["weight_column_urbaninstitute"] = 1
        weight_name = "weight_column_urbaninstitute"

    # Convert coltypes of filters and weights. If not possible to convert, this
    # fxn will raise ValueError
    point_data = convert_coltypes_for_filter_and_weights(
        df=point_data,
        filters=filters,
        weight=weight_name,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket,
    )

    # Drop and record null filter rows and null weight rows
    point_data = drop_null_filters_and_weights(
        df=point_data,
        filters=filters,
        weight=weight_name,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket,
    )

    # Apply user filters to data (and record number of rows filtered out)
    all_filter_queries = generate_filter_strings(filters)

    point_data = apply_filters(
        point_data,
        all_filter_queries,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket,
    )

    # Read in gdf of Census places with population greater than 50k
    place_data = read_in_pkl_from_s3_as_gdf(
        "reference-data/"
        + year
        + "/big_places_us_manual_tract_join_1p_cutoff.pkl",
        bucket=data_bucket,
    )
    # Convert place_data to 4326. This should be redundant as pickle in S3
    # should already have CRS = 4326. But we do this just to be safe.
    place_data = place_data.to_crs("EPSG:4326")

    #############################################
    ########## Spatially joining data ###########
    #############################################

    # Find the Census place that the data is from. Below fxn will take a random
    # sample of the data, spatially join it to the list of big census places,
    # and extract the Census place most frequently occurring in the
    # sample. It also tries to correct flipped lat/lon columns. The output of
    # this fxn is a one row dataframe with the appropriate Census place for
    # point_data
    place_df = get_place_info(
        point_data,
        place_data,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket,
    )
    place_geoid = place_df.place_geoid.values[0]

    # Read in all census tracts for that place
    poly_data = read_in_tracts_by_place_from_s3(
        data_bucket, place_geoid, year=year
    )

    # Spatially join points to tracts in chunks of 80k points to avoid
    # exceeding RAM limits of lambda. Note this function makes use of the global
    # variables point_data and poly_data to try to conserve RAM and not create copies of the
    # user dataframe
    temp = perform_chunked_spatial_join(
        point_df=point_data,
        poly_df=poly_data,
        weight_col=weight_name,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket,
    )

    # Deleteing point_data in order to save RAM and stay within Lambda's RAM
    # limits. We no longer use the individual point_data after this point and
    # instead use the tract summaries store in the temp dataframe.
    del point_data

    #################################################################
    ########## Calculate geographic bias and significance ###########
    #################################################################

    tot_points_joined = temp["weighted_counts"].sum()

    # data_prop =  proportion of datapoints within the tract,or # of weighted
    # datapoints within tract (ie weighted_counts) / total number of points in data (ie
    # tot_points_joined)
    temp["data_prop"] = temp.apply(
        lambda row: row["weighted_counts"] / tot_points_joined, axis=1
    )

    # Add two columns for every baseline: diff_baseline (which is the
    # difference between the baseline and the data_prop) and sig_diff_baseline
    # (which is a boolean for whether the difference is statistically
    # significant). This is determined by whether the 95% confidence interval
    # for baseline_prop contains data_prop.

    baseline_cols = [
        "pop_prop",
        "under_200_poverty_line_prop",
        "no_internet_prop",
        "seniors_prop",
        "cb_renter_hh_prop",
    ]

    temp = calculate_sig_diff_baselines(temp, baseline_name="pop_prop")
    temp = calculate_sig_diff_baselines(
        temp, baseline_name="under_200_poverty_line_prop"
    )
    temp = calculate_sig_diff_baselines(temp, baseline_name="cb_renter_hh_prop")
    temp = calculate_sig_diff_baselines(temp, baseline_name="no_internet_prop")
    temp = calculate_sig_diff_baselines(temp, baseline_name="seniors_prop")

    ###########################################################
    ########## Write out geographic bias outputs  #############
    ###########################################################

    # Specify variable names to include in geo-bias geojson. Not including all
    # variables to keep file small and limit data transfer times
    cols = [
        s
        for s in temp.columns
        if s.startswith("diff_")
        or s.startswith("sig_diff_")
        or s
        in (
            ["tract_geoid", "NAME", "weighted_counts", "geometry", "data_prop",]
            + baseline_cols
        )
    ]

    geojson_key = "output-data/geo-bias/geojson/" + system_key + ".geojson"
    geo_csv_key = "output-data/geo-bias/csv/" + system_key + ".csv"

    # Construct geo-bias geojson filepath
    geo_bias_fname = "s3://" + trigger_bucket + "/" + geojson_key

    # Round to 5 decimal points to reduce file size, and write out geojson
    temp[cols].round(5).to_file(geo_bias_fname, driver="GeoJSON")

    # The above line places an output geojson in S3, but doesn't set public-read
    # ACL, which is needed for public download by users. So we do that manually.
    # The other csv/json objects we write out are automatically uploaded with
    # public-read ACLs
    boto3.resource("s3").ObjectAcl(trigger_bucket, geojson_key).put(
        ACL="public-read"
    )

    # Place CSV version of the geo-bias file in S3. Don't round as this file
    # will not be returned by API and will only be exposed via download links
    # for the user. Note the ACL for this object will be public-read
    place_df_as_csv_in_s3(
        df=temp[cols], bucket=trigger_bucket, key=geo_csv_key,
    )

    ###########################################################
    ########## Calculate demographic bias outputs  ############
    ###########################################################

    # Read in precomputed citywide stats and margins of error
    precomputed_stats = read_in_pkl_from_s3_as_gdf(
        "reference-data/"
        + str(year)
        + "/precomputed_place_stats/"
        + str(place_geoid)
        + ".pkl",
        bucket=data_bucket,
    )

    # set up variables for creating stats df and normal sampling within loop
    list_of_dicts = []

    # Define census variabels we want to compute data implied averages for. This
    # shoud match the list in precomputed_stats['census_var']
    censusvars = [
        "pct_white",
        "pct_black",
        "pct_asian",
        "pct_hisp",
        "pct_all_other_races",
        # Currently this is pct_less_hs_diploma
        "pct_hs_or_higher",
        "pct_seniors",
        "pct_children",
        "pct_veterans",
        "pct_unins",
        "pct_disability",
        "pct_renters",
        "pct_cb_renter_hh",
        "pct_no_internet",
        "pct_limited_eng_hh",
        "pct_bach",
        "pct_under_poverty_line",
        "pct_under_200_poverty_line",
        "pct_unemp",
    ]
    for var in censusvars:
        # Appends a dict, to the list of dicts
        # Each dict corresponds to a census variable of interest
        create_demographic_vars_and_sd(
            df_data=temp,
            df_precomputed=precomputed_stats,
            censusvar=var,
            list=list_of_dicts,
        )

    # Creating dataframe from list of dicts
    stats_df = pd.DataFrame(list_of_dicts)

    # For the census variable pct_hs_or_higher, we are actually interested in
    # displaying the converse of this (the percent of folks with less than a hs
    # diploma). This is just 1 - pct_hs_or_higher so we change the city_value
    # data_value, and census_var below. Note that the sd's don't change as this
    # is a proportion and all we are doing is taking 1 - the propotion
    stats_df.loc[stats_df["census_var"] == "pct_hs_or_higher", "city_value"] = (
        1
        - stats_df.loc[
            stats_df["census_var"] == "pct_hs_or_higher", "city_value"
        ]
    )

    stats_df.loc[stats_df["census_var"] == "pct_hs_or_higher", "data_value"] = (
        1
        - stats_df.loc[
            stats_df["census_var"] == "pct_hs_or_higher", "data_value"
        ]
    )

    stats_df.loc[
        stats_df["census_var"] == "pct_hs_or_higher", "census_var"
    ] = "pct_less_hs_diploma"

    ###########################################################
    ########## Write out demographic bias outputs  ############
    ###########################################################

    # Write out json and CSVs in s3 with public-read ACLs for public download
    demo_json_key = "output-data/demographic-bias/json/" + system_key + ".json"
    place_df_as_json_in_s3(
        df=stats_df, bucket=trigger_bucket, key=demo_json_key
    )

    demo_csv_key = "output-data/demographic-bias/csv/" + system_key + ".csv"
    place_df_as_csv_in_s3(df=stats_df, bucket=trigger_bucket, key=demo_csv_key)

    ###########################################################
    ########## Write out final JSON for API  ##################
    ###########################################################

    # This is the JSON which contains, the demographic data as a json, the
    # geographic data as a geojson, all the messages, download_links, baseline
    # min/mand city bounding box. This is used by the frontend to generate the
    # interactive dashboard.

    geojson_dict = json.loads(temp[cols].round(5).to_json())
    # Note we replace NA's with nulls as pandas to_dict doesn't automatically
    # convert NaNs to the json friendly nulls. Note that stats_df should not
    # have any null values and if it does this is a sign of an error. Only have
    # in now for testing.
    # TODO: Remove fillna null as stats_df should not have null values
    demo_json_dict = stats_df.round(5).fillna("null").to_dict(orient="records")

    # Note that when converting using to_dict, NaNs stay as nan. So we need to
    # replace with null

    # Calculate mins/maxes of baseline diff columns.
    diff_cols = ["diff_" + x.replace("_prop", "") for x in baseline_cols]

    all_min_max_cols = diff_cols + baseline_cols + ["data_prop"]
    baseline_cols + ["data_prop"]

    mins = temp[all_min_max_cols].min().round(5).to_dict()
    # Add _min as suffix to all keys in the dictionary
    mins = {str(key) + "_min": val for key, val in mins.items()}

    maxes = temp[all_min_max_cols].max().round(5).to_dict()
    # Add _min as suffix to all keys in the dictionary
    maxes = {str(key) + "_max": val for key, val in maxes.items()}

    # Combine keys of mins and maxes dicts. There are no duplicate keys across
    # these two dicts, so this is essentially just adding two dicts together.
    # Below syntax is only available as of Python 3.5+
    bounds = {**mins, **maxes}

    # Construct API response JSON
    full_result = {
        "geo_bias_data": geojson_dict,
        "demographic_bias_data": demo_json_dict,
        "messages": status_json,
        "download_links": {
            "geo_bias_geojson": "https://"
            + trigger_bucket
            + ".s3.amazonaws.com/"
            + geojson_key,
            "geo_bias_csv": "https://"
            + trigger_bucket
            + ".s3.amazonaws.com/"
            + geo_csv_key,
            "demographic_bias_csv": "https://"
            + trigger_bucket
            + ".s3.amazonaws.com/"
            + demo_csv_key,
        },
        "bounds": bounds,
        "bbox": temp.total_bounds.tolist(),
    }

    # Upload final API response json to S3 with private acl so only API can
    # access this file
    s3 = boto3.client("s3", region_name="file_bucket_region")
    s3.put_object(
        Body=(bytes(json.dumps(full_result).encode("UTF-8"))),
        Bucket=trigger_bucket,
        Key="output-data/api-response/" + system_key + ".json",
        ACL="private",
    )

    status_json["updates"]["finished"] = True
    update_status_json(
        system_key=system_key, status_json=status_json, bucket=trigger_bucket
    )

    ###########################################################
    ########## Create and write out log data  #################
    ###########################################################

    cdp_name = place_df.cleaned_name.iloc[0]
    logdata = pd.DataFrame(
        {
            "num_rows_file": status_json["updates"]["num_rows_file"],
            "s3_input_bucket": trigger_bucket,
            "s3_input_key": file_key,
            "request_id": request_id,
            "cdp_name": cdp_name,
            "place_geoid": place_geoid,
        },
        index={1},
    )

    log_data_fname = "output-data/logs/" + system_key + ".json"
    place_df_as_json_in_s3(
        df=logdata, bucket=trigger_bucket, key=log_data_fname
    )

    # Print statement for debugging lambda function in Cloudwatch logs
    print(
        "Finished request - "
        + cdp_name
        + "; "
        + str(status_json["updates"]["num_rows_file"])
        + " points geocoded "
        + "; "
    )

    # Have Lambda function return some dummy data to help debugging effort.
    data = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "geo_bias": full_result["download_links"],
        "log_data": log_data_fname,
        "trigger_bucket": trigger_bucket,
        "ref-data-year": year,
    }

    return {
        "statusCode": 200,
        "body": json.dumps(data),
        "headers": {"Content-Type": "application/json"},
    }
