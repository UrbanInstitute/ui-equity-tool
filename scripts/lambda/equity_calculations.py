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
import fiona.crs
import fiona
import math
import time

data_bucket = os.environ["DATA_BUCKET"]
file_bucket_region = os.environ["FILE_BUCKET_REGION"]
data_bucket_region = os.environ["DATA_BUCKET_REGION"]

################ Helper Functions ###################


def create_and_write_status_json(
    system_key, bucket, file_bucket_region=file_bucket_region
):
    """
    Creates and writes update JSON to a bucket on S3. This function is only
    used once in this script for the initial creation of the status JSON.
    Updating the status JSOn is done with the `update_status_json` function.
    This updated json will be pulled by the getstatus endpoint when users check
    in on the status of their fileids.

    Args:
        system_key (str): system file key on S3.
        bucket (str): bucket to write status JSON to.
        file_bucket_region (str, optional): AWS region that the bucket file is
            located. Defaults to file_bucket_region.

    Returns:
        update_json (dict, json): A json (ir python dict) that has already been
            written to S3 with the appropriate system_key.
    """

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
            "num_sub_geo_total": None,
            "num_sub_geo_data": None,
            "g_disp": None,
            "sjoin_started": False,
            "finished": False,
            "error-messages": False,
            "tool_geo":None,
            "tool_sub_geo": None,
            # This is FIPS code
            "g_disp_fips": None,
            "total_time": None
        },
        "warnings": {
            "multiple_geographies_flag": False,
            "num_null_latlon_rows_dropped": None,
            "num_null_filter_rows_dropped": None,
            "num_null_weight_rows_dropped": None,
            "num_out_of_geography_rows_dropped": None,
            "multiple_geographies_list": None,
            "few_sub_geos_flag": False
        },
        "error-messages": {
            "form-data-parameter-validation-failed": False,
            "data_readin_error": False,
            "df_conversion_to_gdf_failed": False,
            "filter_coltypes_mismatch": False,
            "weight_coltypes_mismatch": False,
            "filter_column_not_in_data": False,
            "all_rows_filtered": False,
            "pts_not_in_any_geography": False,
            "sjoin_failed": False
        },
    }

    # Attach public-read ACL to JSONS so API can read them
    # appropriate permissions to still read in the data
    s3.put_object(
        Body=(bytes(json.dumps(update_json).encode("UTF-8"))),
        Bucket=bucket,
        Key=update_key,
        ACL="public-read",
    )

    return update_json

def update_status_json(system_key, status_json, bucket):
    """
    Places status JSON, which contains update, warning, and error msgs, into
    the `update-data/` folder of a S3 bucket. Before running this function, the
    status json should already be modified and ready to upload to S3 with
    relevant warning/update messsages.

    Args:
        system_key (str): system file key on .
        status_json (dict): The status JSON to write to S3.
        bucket (str): bucket to write status JSON to.
    """

    # Create S3 key from file_key
    update_key = "update-data/" + system_key + ".json"
    s3 = boto3.client("s3", region_name=file_bucket_region)

    s3.put_object(
        Body=(bytes(json.dumps(status_json).encode("UTF-8"))),
        Bucket=bucket,
        Key=update_key,
        ACL="public-read",
    )

def read_in_json_from_s3(bucket, bucket_region, file_key):
    """Read in JSON file from S3

    Args:
        bucket (str): name of S3 bucket where JSON file lives
        bucket_region (str): AWS region where buckets
        file_key (str): file key for JSON file inside the bucket.

    Returns:
        js (dict,json): json object stored as a python dict
    """

    s3 = boto3.client('s3', region_name= bucket_region)
    result = s3.get_object(Bucket=bucket, Key=file_key)
    text = result["Body"].read().decode('utf-8')
    js = json.loads(text)

    return js

def read_in_point_data(
        file_key,
        lon_name,
        lat_name,
        status_json,
        system_key,
        bucket,
    ):
    """Read in user uploaded point data from `input-data` folder in S3

    Args:
        file_key (str): File key on S3 (should be parsed directly from S3
            event).
        lon_name (str): Name of the longitude column in the CSV.
        lat_name (str): Name of the latitude column in the CSV.
        status_json (dict): Status JSON which will be updated and uploaded to S3
            as the function operates.
        system_key (str): system file key on S3
        bucket (str): bucket to write status_json to, and to read the CSV file
            from.

    Raises:
        ValueError: Data Import from S3 not successful, when none of the 3 CSV
            encodings tried in the `read_in_csv_from_s3_as_gdf` function work
            to read in the file.

    Returns:
        point_data (pd.df): A geopandas geodataframe containing the user
        uploaded point data
    """

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
    """Converts column types of a df into the provided column types

    Args:
        df (gpd.gdf): Geodataframe of point data.
        col_name (str): Name of column.
        col_type (str): Type for column. Must be one of "number", "string", or
            "date".
        status_json (dict): Status JSON to update with warnings/errors.
        system_key (str): System key to use for updating status_json.
        bucket (str): Bucket to write status_json to.

    Raises:
        ValueError: When filter columns cannot be converted to the provided
            types.
        KeyError: When filter columns are not in the data.

    Returns:
        df (gpd.gdf): A geodataframe with the provided column types converted.
    """

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
        # If ValueError, then throw
        # filter_coltypes_mismatch_error, record name of problematic column,
        # and turn error flag to True in update json
        status_json["error-messages"]["filter_coltypes_mismatch"] = True
        status_json["error-messages"][
            "name_filter_coltypes_mismatch"
        ] = col_name
        status_json["updates"]["error-messages"] = True
        update_status_json(system_key, status_json, bucket=bucket)

        raise ValueError("Filter column types cant be converted")

    except KeyError:
        # If KeyError, then throw filter_column_not_in_data error, record
        # name of the problematic column, and turn error flag to True in
        # the update json
        status_json["error-messages"]["filter_column_not_in_data"] = True
        status_json["error-messages"][
            "name_filter_column_not_in_data"
        ] = col_name
        status_json["updates"]["error-messages"] = True
        update_status_json(system_key, status_json, bucket=bucket)

        raise KeyError("Filter column is not in the data")


def convert_coltypes_for_filter_and_weights(
    df, filters, weight, system_key, status_json, bucket
    ):
    """Convert column types as needed for filters and weights

    Converts weight column to numeric, and converts any columns used in the
    filtering operations to the appropriate column types as determined by the
    frontend. This function wraps the `convert_coltypes` function above to
    actually perform the column type conversion for the filter columns.

    Args:
        df (gpd.gdf): A geodataframe of point data.
        filters (list): Filters provided by frontend, as a list of dictionaries.
        weight (str): Name of weight column in df.
        system_key (str): System key to use for updating status_json.
        status_json (dict): Status JSON to update with warnings/errors.
        bucket (str): Bucket to write status_json to.

    Raises:
        ValueError: When weight column can't be converted to numeric.

    Returns:
        df (gpd.gdf): A geodataframe with the column types converted for filter
            and weight columns.
    """

    try:
        df[weight] = pd.to_numeric(df[weight])
    except:
        status_json["error-messages"]["weight_coltypes_mismatch"] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(system_key, status_json, bucket=bucket)
        raise ValueError("Weight column type cant be converted to numeric")

    # Try converting the filter columns into the selected column types. If we
    # can't for any of the filters, error out and record in status_json.
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
    """Drop null values in filter and weight columns

    When we read in the point data csv into our tool, we use the pandas default
    NA coercer, which treats many values as NA. These values are noted in the
    Tool FAQ. If these NA values are in the weight or filter columns, they will
    be dropped using this function.

    Args:
        df (gpd.gdf): A geodataframe of point data.
        filters (list): Filters from the frontend formatted as a list of dicts.
        weight (str): Name of weight column in df.
        system_key (str): System key to use fo updating status_json.
        status_json (dict): Status JSON to update with warnings/errors.
        bucket (str): bucket to write status_json to.

    Returns:
        df (gpd.gdf): A geodataframe where the null filter and weight columns
            are dropped.
    """

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
    """Read in geojson from S3 as a gdf

    Args:
        bucket (str): S3 bucket to read in geojson from
        key (str): full S3 key to geojson.

    Returns:
        gdf (gpd.gdf): A geodataframe.

    Notes:
        This function is not used in the production version of the tool as its
        for v3.0 where we allow users to upload geojsons to the tool.
    """

    s3 = boto3.client("s3", region_name=file_bucket_region)
    obj = s3.get_object(Bucket=bucket, Key=key)
    json_g = json.loads(obj["Body"].read().decode("utf-8"))
    gdf = gpd.GeoDataFrame.from_features(json_g["features"])
    gdf = gdf[["geometry"]]
    return gdf


def convert_date_string(string):
    """Convert date string in mm/dd/yy format to YYYYmmdd format

    Frontend uploads date in mm/dd/yy format. Byt python needs dates in YYYYmmdd
    format to be use with `query` for filtering.

    Args:
        string (str): date string (or date time string).

    Returns:
        str: A string with date in YYYYmmdd format.
    """
    return datetime.datetime.strptime(string, "%m/%d/%Y").strftime("%Y%m%d")


def multi_text_constructor(filter_comp):
    """Construct pandas text filters from frontend comparison operators

    Frontend uses comparison operators == and !=, which don't work in pandas
    when there are multiple pieces of text to filter on (ie col in c(x1, x2,x3))
    So we convert to "in" and "not in" respectively to be used with the pandas
    query operator.

    Args:
        filter_comp (str): Filter comparison operator from frontend, either
        `==` or `!=`.

    Raises:
        ValueError: When text filter does not contain valid comparison operator.

    Returns:
        str: "in" or "not in".
    """

    if filter_comp == "==":
        return "in"
    elif filter_comp == "!=":
        return "not in"
    else:
        raise ValueError("Text Filter does not contain valid comparison")


def wrap_multi_text_in_quotes(filter_val):
    """Wrap comma separated text in quotes to work with pd.query

    Need to be deliberate about using single quote marks vs double
    # quote marks this so that the quoting will work with pd.query.

    Args:
        filter_val (str): filter value from frontend.

    Returns:
        quoted_text (str): single string with comma separated text values in
        appropriate quotes.
    """

    quoted_text = ",".join(['"' + x + '"' for x in filter_val.split(",")])
    return quoted_text


def generate_filter_strings(filter_list):
    """Generate string list of filters for use in pd.query

    Args:
        filter_list (list): List of dictionaries of filters from the frontend.

    Returns:
        all_filter_queries (list): list of filters correctly formatted for
            into pd.query.

    Notes:
        We enclose all column names in backticks in case user columns have
        spaces/operators in them.
    """

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
    """Apply filters to a dataframe

    Args:
        df (gpd.gdf): geodataframe of point data.
        filter_list_string (list): Modified list of filters to feed into
            pd.query .
        system_key (str): System key to use for updating status_json.
        status_json (dict): Status JSON to update with warnings/errors.
        bucket (str): Bucket to write status_json to.

    Raises:
        ValueError: When filtering removes all rows from df.

    Returns:
        df (gpd.gdf): a geodataframe with user provided filters applied.

    Notes:
        Implicit order of filters applied is numeric, string, text-separeted
        string, singleDate, and dateRange.
    """

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


def get_poly_info(df, all_poly_data, system_key, status_json, bucket, geo):
    """Compute the geography a geodataframe is from using a spatial join

    Compute the geography (ie place, county, state, national) a geodataframe
    is mainly from using a spatial join. If there are more than 50 rows in the
    gdf, this function will take a 5% sample, before performing the spatial
    join. If there is more than 1 geography in the data, this function will
    return the most frequently appearing geography in the sample.

    Args:
        df (gpd.gdf): A geodataframe of point data.
        all_poly_data (gpd.gdf): A geodataframe that lists polygons of all
            potential Census places/counties/states that df could be from.
        system_key (str): System key to use for updating status_json.
        status_json (dict): Status JSON to update with warnings/errors.
        bucket (str): Bucket to write status_json to.
        geo (str): Name of geography level selected for tool (e.g "national",
       "state", {"county"}, or "city" ).

    Raises:
        ValueError: [description]

    Returns:
        result (gpd.gdf): One row geodataframe which is the all_poly_data but
            filtered to the most frequently occurring geography in df.

    """

    # If geo is national, there is only one polygon it could be, which is the
    # entire US
    if geo == "national":
        result = pd.DataFrame(
            data={"GEOID": [1], "cleaned_name": "United States"}
        ) 
        
        # For just the US, insert "the" in front of the country name so that the 
        # frontend can use this value appropriately in sentences
        status_json["updates"]["g_disp"] = "the United States"
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )

        return result

    if df.shape[0] > 50:
        # 5% sample of points to reduce spatial join processing time
        npnts = int(round(0.05 * df.shape[0]))
        dfs = df.sample(npnts)
    else:
        dfs = df

    # Spatially join df to all_poly_data and count the joined polygons,
    # sorted in desc order so we can easily identify the most frequently
    # occurring polygon in the data
    x = (
        gpd.sjoin(dfs, all_poly_data, op="within", how="left")
        .groupby("GEOID")["GEOID"]
        .count()
        .sort_values(ascending=False)
    )

    if len(x) == 0:
        # If 0 points are matched to a geography, most likely no points in data
        # are within the all_polygons shapefile for that geography.
        # Before erroring out, lets make sure data wasn't badly coded. One
        # common data entry error is that lon/lat columns are flipped. So we
        # flip and try again
        df.geometry = df.geometry.map(
            lambda point: shapely.ops.transform(lambda x, y: (y, x), point)
        )

        x = (
            gpd.sjoin(dfs, all_poly_data, op="within", how="left")
            .groupby("GEOID")["GEOID"]
            .count()
            .sort_values(ascending=False)
        )

    if len(x) == 1:
        # If there was only one geography, then record it in the update_json
        # and return the geography
        top_geoid = x.index[0]
        result = all_poly_data[all_poly_data.GEOID.isin([top_geoid])]

        used_polygon_name = result.cleaned_name.values[0]

        status_json["updates"]["g_disp"] = used_polygon_name
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )

        return result
    elif len(x) == 0:
        # If there were still no cities even after switching lat/lon columns,
        # record error in json and raise ValueError
        status_json["updates"]["error-messages"] = True
        status_json["error-messages"]["pts_not_in_any_geography"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("Points not in a US city with population over 50,000")
    else:
        print(
            "There were multiple polygon geoids in the data, returning the most common one"
        )
        top_geoid = x.index[0]

        result = all_poly_data[all_poly_data.GEOID.isin([top_geoid])]

        used_polygon_name = result.cleaned_name.values[0]
        all_polygons = all_poly_data[
            all_poly_data.GEOID.isin(x.index)
        ].cleaned_name.values.tolist()

        # Add multiple cities flag to JSON, list the cities, and write out main
        # city used
        status_json["warnings"]["multiple_geographies_flag"] = True
        status_json["warnings"]["multiple_geographies_list"] = all_polygons
        status_json["updates"]["g_disp"] = used_polygon_name
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )

        return result


def read_in_pkl_from_s3_as_gdf(key, bucket=data_bucket):
    """Reads in a pickled python file from S3 as a gdf

    Args:
        key (str): Full s3 key.
        bucket (str, optional): S3 bucket where pkl is located. Defaults to
            data_bucket.

    Returns:
        data (gpd.gdf): A geodataframe.
    """

    s3 = boto3.client("s3", region_name=data_bucket_region)
    response = s3.get_object(Bucket=bucket, Key=key)
    data = pickle.loads(response["Body"].read())
    return data


def try_readin_with_diff_encodings(s3_object):
    """Try reading in csv file with 3 encodings

    Try reading in csv with utf-8, utf-16, and latin1 endcoding. If none of
    those work, then raise a ValueError.

    Args:
        s3_object (str): An s3 object returned by s3.get_object().

    Raises:
        ValueError: Data read in error if none of the 3 CSV encodings work.

    Returns:
        df (pd.df): A dataframe.
    """
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
    key,
    system_key,
    status_json,
    bucket,
    lon_column="lon",
    lat_column="lat",
    ):
    """Read in CSV file from S3 as a geodataframe

    Args:
        key (str): full s3 key to the point data file.
        system_key (str): system key to use fo updating status_json.
        status_json (dict): status json to update with warnings/errors.
        bucket (str): bucket to write status_json to.
        lon_column (str, optional): Name of longitude column in data. Defaults
            to "lon".
        lat_column (str, optional): Name of latitude column in data. Defaults to
            "lat".

    Raises:
        ValueError: "No appropriate CSV encoding. CSV can't be read in", when
            of the 3 default CSV encodings work to read in the file.
        ValueError: "provided lat/lon columns not in data", when given lat/lon
            are not in the data.
        ValueError: "lat/lon columns cannot be converted to numeric", when given
            lat/lon columns can't be converted to a numeric column.
        ValueError: "df_converstion_to_gdf_failed", when the read in df can't be
            converted to a gdf.

    Returns:
        gdf (gpd.gdf): A geodataframe of the point data.
    """
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
        raise ValueError("df_converstion_to_gdf_failed")



def read_in_poly_by_geoid_from_s3(bucket, geoid, denom_geo, num_geo, year):
    """Read in a polygon's tracts from S3

    Reads in all tracts for a given geoid as a gdf. Each row in the returned gdf
    will be a Census tract within the geography.

    Args:
        bucket (str): Bucket where tract level polygon data is stored
        geoid (str): Full geoid to reade data in for. If geo = "place" then
            7 digit full geoid of Census place (2 digit state fips + 5 digit
            place geoid). if geo = "county", then 5 digit geoid. If geo =
            "state", then 2 digit geoid. If geo = "national", then geoid = 1.
        denom_geo (str): Name of denominator geographic level (e.g. national,
            state, county, city).
        num_geo (str): Name of numerator geographic level (e.g. state, county,
            tract, tract).
        year (str): Year to pull data for.

    Returns:
        data (gpd.gdf): Geodataframe of all the tracts within the specified
            geoid.
    """

    s3 = boto3.client("s3", region_name=data_bucket_region)

    response = s3.get_object(
        Bucket=bucket,
        Key=(
            "reference-data/"
            + year
            + "/"
            + denom_geo
            + "/"
            + num_geo
            + "/"
            + str(geoid)
            + ".pkl"
        ),
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
    loop_cutoff=80000
    ):
    """Spatially join a gdf of points to a gdf of polygons in chunks

    We perform a chunked spatial join between a point gdf and a poly gdf in
    order to remain within the RAM limits of AWS lambda. We settled on a default
    value of 80000 points in each loop after some manual testing.

    Args:
        point_df (pd.df): Geodataframe of user uploaded point data.
        poly_df (gpd.gdf): Geodataframe of all census tracts in the geography.
        weight_col (str): Name of weight column.
        system_key (str): System key to use fo status_json.
        status_json (dict): Status json to update with warnings/errors.
        bucket (str): Bucket to write status_json to.
        loop_cutoff (int, optional): limit for how many points to join in each
            chunk. Defaults to 80000.

    Raises:
        ValueError: If spatial join is unsuccessful.

    Returns:
        temp (gpd.gdf): A geodataframe, which has the same structure of poly_df,
            but with an additional column named `weighted_count` that
            the count of points that fell within each tract.
    """

    # After this point all you need is the weight column, and the geometry
    # column, We've already finished useing the filters and lat/lon columns
    point_df = point_df[[weight_col, "geometry"]]

    # Create pandas series with index being the tract geoid and all values being
    # 0. This is the series we will add our weighted tract counts to, from every
    # iteration of the spatial join loop
    temp = pd.Series(data=0, index=poly_df["GEOID"].unique())

    # Record number of points not in any geography. This is a counter that will be
    # added to in every iteration of the loop
    pts_not_in_any_geography = 0
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
                    poly_df.loc[:, ["GEOID", "geometry"]],
                    op="within",
                    how="left",
                )
            )

            # Update number of rows processed in status json
            nrow_processed = nrow_processed + joined_pts.shape[0]

            status_json["updates"]["num_rows_processed"] = nrow_processed
            update_status_json(
                system_key=system_key, status_json=status_json, bucket=bucket
            )

            # Add points without a joined GEOID (ie pts not in a geography) to
            # the total count
            pts_not_in_any_geography = (
                pts_not_in_any_geography
                + joined_pts[joined_pts.GEOID.isnull()].shape[0]
            )

            # Group joined points by GEOID and sum across weight column to get
            # weighted count in each tract.
            temp1 = joined_pts.groupby("GEOID")[weight_col].sum()

            # Add weighted counts to the temp pandas series. if the item is
            # missing or None, it treats it as 0 and adds 0 for that tract
            temp = temp.add(temp1, fill_value=0)

            # Delete the points that were spatially joined
            point_df = point_df[loop_cutoff:]

        # After the loop, temp is a pandas series where the indices are all the
        # unique geoids in the denominator geography, and the values are the
        # sum of the weighted counts within each numerator geography. We
        # convert that to a dataframe and sort values
        temp = pd.DataFrame(
            {"GEOID": temp.index, "weighted_counts": temp.values}
        ).sort_values(by=["weighted_counts"], ascending=False)

        # If no points are matched to a tract, raise pts_not_in_any_geography 
        # error
        if (temp.weighted_counts.sum() == 0):
            status_json['updates']['error-messages'] = True
            status_json['error-messages']['pts_not_in_any_geography'] = True
            update_status_json(
                system_key=system_key, status_json=status_json, bucket=bucket
            )
            raise ValueError("Points not in any tract")

            
        # Perform left join with poly_df to retain demographic and baseline
        # information in poly_df
        temp = poly_df.merge(temp, on="GEOID")

        # Write out number of pts not in the geography and final row count in
        # status_json
        status_json["warnings"][
            "num_out_of_geography_rows_dropped"
        ] = pts_not_in_any_geography

        status_json["updates"]["num_rows_final"] = (
            status_json["updates"]["num_rows_for_processing"]
            - pts_not_in_any_geography
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
    """Place a df as a CSV in S3

    Args:
        df (gpd.gdf or pd.df): A pandas dataframe or geopandas geodataframe.
        bucket (str): S3 bucket to write out to.
        key (str): Full S3 key.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client("s3", region_name=file_bucket_region)
    s3.put_object(
        Body=csv_buffer.getvalue(), Bucket=bucket, Key=key, ACL="public-read"
    )


def place_df_as_json_in_s3(df, bucket, key, acl="public-read"):
    """Place a dataframe as a json in S3

    Args:
        df (gpd.gdf of pd.df): A dataframe of geodataframe.
        bucket (str): S3 bucket to write out to.
        key (str): Full S3 key.
        acl (str, optional): ACL to attach to the json. Defaults to
            "public-read".
    """

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
        ACL=acl,
    )


def append_geo_disparity_score_and_sig_diff(temp_df, baseline_name):
    """Append geographic disparity score and significance for a baseline column

    For a given baseline column, add a column with the geo-disparity score
    (diff_{baseline_name}) and a column for whether that score is significant
    (sig_diff_{baseline_name}). The significance calculation is done at the 95%
    confidence level to take into account variation in the baseline proportions
    from the ACS

    Args:
        temp_df (gpd.gdf): A geodatframe where every row = a census tract. Must
            contain a data_prop column, the baseline_name column and the
            baseline_name_sd column.
        baseline_name (str): Name of baseline column to calculate geo disparity
            score for.

    Returns:
        temp_df (gpd.gdf): Modified version of input `temp_df` with 2 additional
            columns: diff_baseline and sig_diff_baseline
    """
    diff_col_name = "diff_" + baseline_name.replace("prop_", "")
    sig_diff_col_name = "sig_" + diff_col_name

    # Add column which is difference between data_prop and baseline_prop column,
    # or the geo-disparity score
    temp_df[diff_col_name] = temp_df.apply(
        lambda row: round(row["data_prop"], 3) - round(row[baseline_name], 3), axis=1
    )

    # Calculate whether the geo disparity score is significant, ie whether the
    # 95% confidence interval for baseline_prop contains data_prop. We use the
    # critical value of 1.96 for the 95% confidence interval as outlined by
    # Chapter 7 of the ACS handbook.
    temp_df[sig_diff_col_name] = np.where(
        (
            temp_df["data_prop"]
            <= (
                temp_df[baseline_name]
                - 1.960 * temp_df[baseline_name + "_margin"]
            )
        )
        | (
            temp_df["data_prop"]
            >= (
                temp_df[baseline_name]
                + 1.960 * temp_df[baseline_name + "_margin"]
            )
        ),
        "TRUE",
        "FALSE",
    )

    return temp_df


def calculate_demo_disparity_score_and_sig_diff(
    df_data,
    df_precomputed,
    censusvar,
    list,
    geo,
    geo_fips,
    geo_display,
    geo_mo):
    """Calculate demographic disparity scores and significance for a censusvar

    Calculates data-implied values from df_data, gets geography-wide values
    from df_precomputed, and then calculates demographic disparity scores and
    significance.

    Args:
        df_data (gpd.gdf): Geodataframe of tract level data used for computing
            data-implied statistics and margins. Must be set to `temp` in the
            context of this script.
        df_precomputed (pd.df): Dataframe of geography-wide statistics and
            margins.
        censusvar (str): Name of census variable to calculate demo-disparity
            score for.
        list (list): List of dictionaries to append to. After this fxn is run on
            all census variables, this list will be converted to a dataframe
        geo (str): Name of user supplied geography (e.g. city, county, state, or
            national).
        geo_fips (str): FIPS code for geography.
        geo_display (str): Display name of geography used in takeaway text in
            frontend.
        geo_mo (str): Name of geography used for mouseover in frontend.

    Notes:
        This fxn has no outputs but appends a dict to `list`, which contains all
        the variables needed to construct a `stats_df`, which contains
        demographic scores and significance for census variables.
    """

    # The df_precomputed dataframe already contains the geography wide values
    # and margins so we extract those.
    precomputed_row = df_precomputed[df_precomputed["var"] == censusvar]
    summary_value = precomputed_row["value"].iloc[0]
    summary_value_margin = precomputed_row["value_margin"].iloc[0]


    # Calculate data_value
    data_value = (df_data["data_prop"] * df_data[censusvar]).sum()

    # Compute margin for citywide estimates using Census formulas for
    # count variables (ie the numerator and denominator that make up the
    # censusvar). Note that row['data_prop'] is treated as a constant so we just
    # multiply that by the margin. See formula 1 here:
    # https://www.census.gov/content/dam/Census/library/publications/2018/acs/acs_general_handbook_2018_ch08.pdf
    data_value_margin = ((df_data["data_prop"] * df_data[censusvar + "_margin"]) ** 2).sum()

    # To get sd, divide margin by 1.645
    data_value_sd = (math.sqrt(data_value_margin)) / 1.645

    # Compute significance using Census formulas for significance (a Z-test).
    # See formula 3 here:
    # https://www.census.gov/content/dam/Census/library/publications/2018/acs/acs_general_handbook_2018_ch07.pdf

    test_statistic_num = data_value - summary_value
    test_statistic_den = math.sqrt(
        # check this in R data update scripts
        data_value_sd ** 2 + summary_value_margin ** 2
    )
    test_stat = test_statistic_num / test_statistic_den

    # We set confidence level at 95%, so critical test-stat value is 1.96
    sig_diff = abs(test_stat) > 1.96

    vals = {
        "census_var": censusvar,
        "data_value": data_value,
        "summary_value": summary_value,
        "diff_data_city": data_value - summary_value,
        "data_value_sd": data_value_sd,
        "summary_value_margin": summary_value_margin,
        "sig_diff": sig_diff,
        "geo": geo,
        "geo_fips": geo_fips,
        "geo_display": geo_display,
        "geo_mo": geo_mo
    }
    list.append(vals)


def read_in_all_polygons(geo, year, bucket=data_bucket):
    """Read in gdf of all_polygons, which is used to identify the appropriate
    geography in the data

    Args:
        geo (str): User specified geography (e.g. city, county, state or
            national).
        year (int): year of data to read in. Currently supports 2018 and 2019.
        bucket (str, optional): Bucket which holds infrastructure data. Default
            to data_bucket.

    Returns:
        results (gpd.gdf): A geodataframe with all the potential polygons for
            the geo
    """
    if geo == "national":
        return None
    else:
        result = read_in_pkl_from_s3_as_gdf(
            ("reference-data/" + year + "/" + geo + "/" + "all_polygons.pkl"),
            bucket=bucket,
        )
        # Convert all_poly_data to 4326. This should be redundant as pickle in S3
        # should already have CRS = 4326. But we do this just to be safe.
        result = result.to_crs("EPSG:4326")
        return result


def read_in_precomputed_data_for_geoid(
    geo,
    year,
    polygon_geoid,
    bucket=data_bucket):
    """Read in precomputed demographic data for a geoid as a gdf

    Args:
        geo (str): User specified geography (e.g. city, county, state, or
            national).
        year (int): Year of data to read in. Currently supports 2018 or 2019.
        polygon_geoid (str): geoid to read in data for. Depending on the geo,
            length of the geoid will be different
        bucket (str, optional): Bucket which contains infrastructure data.
            Defaults to data_bucket

    Returns:
        precomputed_stats (gpd.gdf): Geodataframe of precomputed stats

    Notes:
        This output is used in demographic disparity score calculations for the
        geography-wide statistics.
    """
    precomputed_stats = read_in_pkl_from_s3_as_gdf(
        "reference-data/"
        + str(year)
        + "/"
        + geo
        + "/precomputed_stats/"
        + str(polygon_geoid)
        + ".pkl",
        bucket=bucket,
    )

    return precomputed_stats


def calculate_dem_sub_geo(
    sub_geo,
    geo_disp_geo,
    temp,
    list_of_dicts,
    censusvars,
    year,
    bucket):
    """Calculate demo disparity scores for sub-geographies

    For some geographies (state and national), we also calculate demographic
    disparity scores for sub-geographies (state, and county respectively).

    Args:
        sub_geo (str): Sub-geography FIPS code
        geo_disp_geo (str): Geography level of calculation ("county" for state
        tool or "state" for national tool)
        temp (gpd.gdf): Geodataframe of joined points and tracts
        list_of_dicts (list): list to store results
        censusvars (list): list of census variables to calculate demographic
        disparity scores for
        year (int): Year of data to pull
        bucket (str): S3 bucket to pull data from
    """

    precomputed_stats_sub_geo = read_in_precomputed_data_for_geoid(
        geo= geo_disp_geo, year=year, bucket=bucket, polygon_geoid=sub_geo
        )

    # disp_name and disp_name_abbv columns all same so obtain first value
    sub_geo_name = precomputed_stats_sub_geo['disp_name'].iloc[0]
    sub_geo_mo = precomputed_stats_sub_geo['disp_name_abbv'].iloc[0]

    temp_sub_geo = temp[temp["sub_geo"] == sub_geo]

    # Recalculate the data_prop variable so that it reflects the proportion of
    # points within the sub-geography
    tot_weighted_counts_sub_geo = temp_sub_geo["weighted_counts"].sum()
    temp_sub_geo["data_prop"] = temp_sub_geo["weighted_counts"] / tot_weighted_counts_sub_geo

    for var in censusvars:
        print(var)
        # Appends a dict, to the list of dicts
        # Each dict corresponds to a census variable of interest
        calculate_demo_disparity_score_and_sig_diff(
            df_data=temp_sub_geo,
            df_precomputed=precomputed_stats_sub_geo,
            censusvar=var,
            list=list_of_dicts,
            geo = geo_disp_geo,
            geo_fips = sub_geo,
            geo_display = sub_geo_name,
            geo_mo = sub_geo_mo
        )

def create_temp_geo(temp, geo, geo_disp, n_char, bucket, polygon_geoid, year):
    """Aggregate temp gdf up to a higher geography for calculating geographic
    disparity scores. Used only when geo = "state" or "national"

    Args:
        temp (gpd.gdf): Geodataframe that is the output of a spatial join using
            perfrom_chunked_spatial_join.
        geo (str): User specified geography (e.g. national, state, county, or
            city).
        geo_disp (str): Name of geography used in the map (that temp needs to be
            aggregated up to). Should be one of "county" or "state"
        n_char (str): Number of characters in GEOID for geo_disp.
        bucket (str): Name of bucket which contains infrastructure data.
        polygon_geoid (str): GEOID of focus geography (ie of geo).
        year (int): Year of ACS data to use.

    Returns:
        temp_geo (gpd.gdf): Geodataframe of aggregated statistics.

    Notes:
        Since the spatial joins are done at the tract level for all geographies,
        but the state and national level tool have non tract level maps, we need
        to read in the appropriate geogarphies, and aggregate up the weighted
        counts from tract to the appropriate geography.
    """

    # poly_data will be the county-level or state-level polygons used in
    # the state and national level tools respectively.
    poly_data = read_in_poly_by_geoid_from_s3(
        denom_geo=geo, num_geo = geo_disp, bucket=bucket, geoid=polygon_geoid, year=year
    )
    temp["agg_geo"] = temp["GEOID"].str[:n_char]

    # Calculate number of weighted counts in each aggregated geography
    temp_geo = temp[["agg_geo", "weighted_counts"]].groupby("agg_geo").sum()
    temp_geo = poly_data.merge(temp_geo, left_on = "GEOID", right_on = "agg_geo", how = "left")

    # of rows in poly_data[[polygon_geoid]]

    return temp_geo

def minify(df, df_name):
    """Shorten column names to minify file size for API

    Args:
        df (pd.df): Dataframe with column names to be shortened.
        df_name (str): name of dataframe, either "geo_bias_data" or
        "demographic_bias_data".

    Returns:
        df (pd.df): Dataframe with columns renamed.

    """

    # Define old and new column names for renaming
    minify_dict = {
        "geo_bias_data": {
            "disp_name": "m_disp",
            "data_prop": "da_prop",
            "diff_cb_renter_hh": "d_cbr",
            "diff_no_internet": "d_int",
            "diff_pop": "d_pop",
            "diff_under_200_poverty_line": "d_pov2",
            "diff_pov": "d_pov",
            "diff_seniors": "d_sen",
            "diff_children": "d_chi",
            "GEOID": "num_geoid", #would we rather this just be geoid
            "prop_cb_renter_hh": "p_cbr",
            "prop_no_internet": "p_int",
            "prop_pop": "p_pop",
            "prop_under_200_poverty_line": "p_pov2",
            "prop_pov": "p_pov",
            "prop_seniors": "p_sen",
            "prop_children": "p_chi",
            "sig_diff_cb_renter_hh": "s_cbr",
            "sig_diff_no_internet": "s_int",
            "sig_diff_pop": "s_pop",
            "sig_diff_under_200_poverty_line": "s_pov2",
            "sig_diff_pov": "s_pov",
            "sig_diff_seniors": "s_sen",
            "sig_diff_children": "s_chi"
            },
        "demographic_bias_data": {
            "census_var": "c_var",
            "diff_data_city": "d_score",
            "sig_diff": "s_diff",
            "geo": "g",
            "geo_fips": "g_fips",
            "geo_display": "g_disp",
            "baseline_pop": "b_pop",
            "geo_mo": "g_mo" #added in field for mouseover name
        }
    }

    df.rename(columns = minify_dict[df_name], inplace = True)

    # For demographic bias data, just keep the fields we rename. This
    # effectively removes data_value, summary_value, data_value_sd, and
    # summary_value_sd. This is done to reduce file size even further and we
    # confirmed that these values aren't use on the frontend
    if (df_name == "demographic_bias_data"):
        df = df[list(minify_dict[df_name].values())]

    return(df)

def make_unique_sub_geo_json(temp_geo, geo):
    """Create list of unique geographies that demographic disparity scores were
    calculated for

    Args:
        temp_geo (gpd.gdf): Geodataframe of joined data at sub-geography level.
        geo (str): User specified geography. Should be "state" or "national" as
            those are the two geographies for which this fxn should be run.

    Returns:
        unique_sub_geo_json (dict): JSON dictionary of unique geographies and
            whether they should be grayed out.

    Notes:
        Geographies are greyed out when no datapoints fall within that geography
    """

    sub_geo_counts = temp_geo[["weighted_counts", "disp_name", "GEOID"]].rename(
        columns={"GEOID": "g_fips"}
    )

    sub_geo_counts['disp'] = sub_geo_counts['disp_name'].str.split(',').str[0]
    sub_geo_counts[["grey"]] = sub_geo_counts[["weighted_counts"]] == 0
    unique_sub_geo_list = sub_geo_counts[["disp", "g_fips", "grey"]]

    # process regions for national level tool
    if geo == "national":
        region_counts = temp_geo[["region_name", "weighted_counts"]].groupby("region_name").sum().reset_index()
        region_counts[["grey"]] = region_counts[["weighted_counts"]] == 0
        region_counts["g_fips"]  = "00"
        region_list = region_counts[["region_name", "g_fips", "grey"]].rename(columns = {"region_name": "disp"})
        unique_sub_geo_list = pd.concat([unique_sub_geo_list, region_list])

    unique_sub_geo_json = unique_sub_geo_list.to_dict(orient="records")

    return unique_sub_geo_json

def use_generalized_state_geometries(df, year, bucket=data_bucket):
    """Replace state geometries with generalized state geometries (20m)

    Args:
        df (gpd.gdf): Geodataframe of state level geometries, should always be
            = temp_geo_cols in the context of this script
        year (str): Year to use
        bucket (str, optional): Bucket which contains infrastructure data. Defaults to
            data_bucket

    Notes:
        We use generalied state geometries to limit API response size and not go
        over AWS API Gateway's built in limit of 5 MB JSON payloads. The default
        state geometries were really detailed and routinely over 5 MB in size.
    """

    # Read in generalized state geometry file
    generalized_state_geometries = read_in_pkl_from_s3_as_gdf(
            ("reference-data/" + year + "/" + "state" + "/" + "all_polygons_20m_generalized.pkl"),
            bucket=bucket
        )
    # Convert gdf to 4326. This should be redundant as pickle in S3
    # should already have CRS = 4326. But we do this just to be safe.
    generalized_state_geometries = generalized_state_geometries.to_crs("EPSG:4326")

    # Drop geometry column from temp_geo_col, and add geometry column from
    # generalized_state_geometries
    df = pd.DataFrame(df.drop(columns='geometry'))

    x = generalized_state_geometries[['num_geoid', 'geometry']].merge(df, on = "num_geoid")

    return(x)

def generate_bbox(geo, temp_df):
    """Generate bounding box for frontend map

    Args:
        geo (str): User selected geography
        temp_df (gpd.gdf): Geopandas geodataframe with tract level data
    """    
    if (geo == "national"):
        # Bc the national tool inludes Alaska which crosses the [-180, 180], 
        # the default bounds include almost the entire earth. We manually create
        # our own bbox which exclues a few tracts in the long tail of Alaska but
        # makes the bbox more usable for all other states. Users will still be
        # able to see these Alaska tracts if they scroll to the right end of the
        # map.
        bbox = [-179.148909, 18.9103648, -66.94989, 71.365162]
        return(bbox)
    else:
        bbox = temp_df.total_bounds.tolist()
        return(bbox)

def handler(event, context):

    ######################################
    ########## Setup parameters ##########
    ######################################

    # Capture start time
    start_time = time.perf_counter()

    # Hardcode year as 2019
    year = "2019"

    # special chars like `(` and `)` are URL encoded weirdly by the S3 event 
    # object causing s3 key not found errors. We fix this by using unquote_plus
    file_key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
    trigger_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    request_id = event["Records"][0]["responseElements"]["x-amz-request-id"]

    # Read in geography crosswalk, used for identifying appropriate
    # subgeogarand and display names for the user selected geo.
    geo_xwalk = read_in_json_from_s3(
        bucket=data_bucket,
        bucket_region=data_bucket_region,
        file_key = f"reference-data/{year}/geo_xwalk.json"
    )

    # Get system key from filename
    # We conservatively replace first and last occurrence in case user
    # has "input-data" or ".csv" in the file name. Note this should never
    # happen as proxy API renames files using unix timestamps but we do this
    # to just be safe.

    # s = file_key.replace("sample-data/", "", 1)
    s = file_key.replace("input-data/", "", 1)
    system_key = "".join(s.rsplit(".csv", 1))

    # Get form data key
    form_key = "form-data/" + system_key + ".json"

    # Create and write out update JSON which we will be writing future updates,
    # warnings, and errors to
    status_json = create_and_write_status_json(
        system_key, bucket=trigger_bucket
    )

    # Read in form-data and extract filters, weights, and lat/lon names
    client = boto3.client("s3")
    result = client.get_object(Bucket=trigger_bucket, Key=form_key)
    form_data = json.loads(result["Body"].read().decode())

    try:
        lat_name = form_data["lat_column"]
        lon_name = form_data["lon_column"]
        filters = form_data["filters"]
        weight_name = form_data["weight"]
        geo = form_data["geo"]

    except:
        status_json["error-messages"][
            "form-data-parameter-validation-failed"
        ] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(
            system_key=system_key,
            status_json=status_json,
            bucket=trigger_bucket
        )
        raise ValueError("Form data parameter validation failed")

    if not all([lat_name, lon_name, geo]) or geo not in ["national", "state", "county", "city"]:
        status_json["error-messages"][
            "form-data-parameter-validation-failed"
        ] = True
        status_json["updates"]["error-messages"] = True
        update_status_json(
            system_key=system_key,
            status_json=status_json,
            bucket=trigger_bucket
        )
        raise ValueError("Form data parameter validation failed")


    print("Got form data")

    ######################################
    ########## Read in Data ##############
    ######################################

    # Read in point data from S3
    point_data = read_in_point_data(
        file_key,
        lon_name,
        lat_name,
        status_json,
        system_key,
        bucket=trigger_bucket
    )

    read_in_point_data_time = time.perf_counter()
    print("Read in point data")

    # Frontend will send blank strings if there are no selected
    # weight columns. If that's the case, create a weight column and assign
    # weight of 1 for all datapoints
    if weight_name == "":
        point_data["weight_column_urbaninstitute"] = 1
        weight_name = "weight_column_urbaninstitute"

    # Convert coltypes of filters and weight columns
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

    # define geographies for demographic and geographic analysis
    dem_disp_geo = geo_xwalk[geo]["dem_disp"]
    geo_disp_geo = geo_xwalk[geo]["geo_disp"]
    n_char = geo_xwalk[geo]["n_char"]


    # Read in gdf of all polygons for a given geography
    all_poly_data = read_in_all_polygons(geo=geo, year=year, bucket=data_bucket)


    #############################################
    ########## Spatially join data ##############
    #############################################

    # Find the Census geography that the data is from. Below fxn will take a
    # random sample of the data, spatially join it to the appropriate geography
    # file, and extract the Census geography most frequently occurring in the
    # sample. It also tries to correct flipped lat/lon columns. The output of
    # this fxn is a one row dataframe with the appropriate Census geography for
    # the point_data
    poly_df = get_poly_info(
        point_data,
        all_poly_data,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket,
        geo=geo,
    )

    polygon_geoid = poly_df.GEOID.values[0]
    full_geoid_name = poly_df.cleaned_name.iloc[0]

    print("Got poly_info")

    # Update status json with details of tool geography used
    status_json['updates']['tool_geo'] = geo
    status_json['updates']['tool_sub_geo'] = geo_disp_geo
    status_json['updates']['g_disp_fips'] = str(polygon_geoid)
    update_status_json(
        system_key=system_key, status_json=status_json, bucket=trigger_bucket
    )

    # Read in all census tracts for the geography
    poly_data = read_in_poly_by_geoid_from_s3(
        denom_geo=geo, num_geo = dem_disp_geo, bucket=data_bucket, geoid=polygon_geoid, year=year
    )

    print("Read in all census tracts")

    # Need to do this bc when we store the city and county poly_data
    # in S3, we have disp name as just "Census Tract X".
    if geo in ['city', 'county']:
        poly_data['disp_name'] = poly_data['disp_name'].astype(str) + ", " + full_geoid_name

    # Spatially join points to tracts in chunks of 80k points to avoid
    # exceeding RAM limits of lambda. Note this function makes use of the global
    # variables point_data and poly_data to try to conserve RAM and not create
    # copies of the big dataframes
    temp = perform_chunked_spatial_join(
        point_df=point_data,
        poly_df=poly_data,
        weight_col=weight_name,
        system_key=system_key,
        status_json=status_json,
        bucket=trigger_bucket
    )

    performed_spatial_join_time = time.perf_counter()
    print("Performed spatial join")

    # Aggregate up tract-level spatial join for geographic disparity calc
    # for the state (county) and national (state) tool. For county and
    # city tool, geographic disparity also calculated at tract. temp = the
    # data we will use for demographic disparity score calculations. temp_geo =
    # the data we will use for geographic disparity score calculations.
    if geo in ["state", "national"]:
        temp_geo = create_temp_geo(
            temp = temp,
            geo = geo,
            geo_disp = geo_disp_geo,
            n_char = n_char,
            bucket = data_bucket,
            polygon_geoid = polygon_geoid,
            year = year)

        # Calculate number of sub geos in data, number of sub geos total, and flag
        # if less than 50% of subgeos are in the data
        status_json["updates"]["num_sub_geo_total"] = temp_geo.shape[0]
        status_json["updates"]["num_sub_geo_data"] = temp_geo[temp_geo["weighted_counts"] > 0].shape[0]
        pct_sub_geos_in_data = status_json["updates"]["num_sub_geo_data"] / status_json["updates"]["num_sub_geo_total"]
        # Set cutoff to 50% after discussion with research team.
        few_sub_geos_flag = pct_sub_geos_in_data <= 0.5
        status_json["warnings"]["few_sub_geos_flag"] = few_sub_geos_flag

    else:
        temp_geo = temp

    print("Aggregated up tract level spatial join")
    update_status_json(
        system_key=system_key, status_json=status_json, bucket=trigger_bucket
    )

    # Deleteing point_data in order to save RAM and stay within Lambda's RAM
    # limits. We no longer use the individual point_data after this point and
    # instead use the tract summaries stored in the temp and temp_geo dataframes
    del point_data

    #################################################################
    ########## Calculate geo disparity scores  ######################
    #################################################################

    tot_weighted_counts = temp_geo["weighted_counts"].sum()

    # confirm that total number of points is same for geographic and
    # demographic disparity
    if (abs(tot_weighted_counts- temp["weighted_counts"].sum()) >= 0.00001):
        # raise error and stop
        status_json["updates"]["error-messages"] = True
        status_json["error-messages"]["sjoin_failed"] = True
        update_status_json(
            system_key=system_key, status_json=status_json, bucket=bucket
        )
        raise ValueError("Weighted counts different for geographic and demographic disparity")

    # data_prop =  proportion of datapoints within the geo, or # of weighted
    # datapoints within geo (ie weighted_counts) / total number of weighted
    # points in data (ie tot_weighted_counts)
    temp["data_prop"] = temp.apply(
        lambda row: row["weighted_counts"] / tot_weighted_counts, axis=1
    )
    temp_geo["data_prop"] = temp_geo.apply(
        lambda row: row["weighted_counts"] / tot_weighted_counts, axis=1
    )



    # Define baseline columns to calculate geographic disparity scores for
    baseline_cols = [
        "prop_pop",
        "prop_cb_renter_hh",
        "prop_seniors",
        "prop_no_internet",
        "prop_under_200_poverty_line",
        "prop_children",
        "prop_pov",
    ]

    for j in baseline_cols:
        # Adds two columns for every baseline: diff_{baseline} (which is the
        # geo-disparity score) and sig_diff_{baseline} (which is a boolean for
        # whether the score is statistically significant). This is determined by
        # whether the 95% confidence interval for baseline_prop contains
        # data_prop.
        temp_geo = append_geo_disparity_score_and_sig_diff(temp_geo, baseline_name=j)

    calculated_geo_bias_time = time.perf_counter()
    print("Calculated sig_diff_baselines")

    ###########################################################
    ########## Write out geographic disparity outputs #########
    ###########################################################

    # Specify variable names to include in geo-bias geojson. Not including all
    # variables to keep file small and limit data transfer times
    cols = [
        s
        for s in temp_geo.columns
        if s.startswith("diff_")
        or s.startswith("sig_diff_")
        or s
        in (
            [
                "GEOID",
                "disp_name",
                "NAME",
                "weighted_counts",
                "geometry",
                "data_prop",
            ]
            + baseline_cols
        )
    ]

    geojson_key = "output-data/geo-bias/geojson/" + system_key + ".geojson"
    geo_csv_key = "output-data/geo-bias/csv/" + system_key + ".csv"

    # Construct geo-bias geojson filepath
    geo_bias_fname = "s3://" + trigger_bucket + "/" + geojson_key

    # Round to 5 decimal points to reduce file size, and write out geojson
    # to_file() only recently supported s3 filepaths, so may not work on older
    # python pkg versions
    temp_geo[cols].round(5).to_file(geo_bias_fname, driver="GeoJSON")

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
        df=temp_geo[cols],
        bucket=trigger_bucket,
        key=geo_csv_key,
    )
    print("Wrote out geographic bias outputs")

    ###########################################################
    ########## Calculate demographic disparity scores #########
    ###########################################################

    # Read in precomputed geography-wide stats and margins of error
    precomputed_stats = read_in_precomputed_data_for_geoid(
        geo=geo, year=year, bucket=data_bucket, polygon_geoid=polygon_geoid
    )

    # Set up empty list for stats_df, which will be appended to in loop
    list_of_dicts = []

    # Define census variables we want to compute data implied averages for.
    censusvars = precomputed_stats["var"].to_list()

    for var in censusvars:
        # Appends a dict, to list_of_dicts. Each dict = a demographic disparity
        # score for a census variable in the main geography
        calculate_demo_disparity_score_and_sig_diff(
            df_data=temp,
            df_precomputed=precomputed_stats,
            censusvar=var,
            list=list_of_dicts,
            geo = geo,
            geo_fips = polygon_geoid,
            geo_display = precomputed_stats['disp_name'].iloc[0],
            geo_mo = precomputed_stats['disp_name_abbv'].iloc[0]
        )

    print("Calculated demographic bias scores")
    calculated_demo_bias_time = time.perf_counter()


    if geo in ["national", "state"]:
        # Calculates demographic disparity for all sub-geographies (states
        # for national tool, counties for state tool) with at least one data
        # point in them
        temp["sub_geo"] = temp["GEOID"].str[:n_char]
        unique_sub_geos = temp_geo[temp_geo["weighted_counts"] > 0]["GEOID"].unique()

        # Appends a dict to list_of_dicts. Each dict = a demographic disparity
        # score for a census variable in the sub-geography
        for sub_geo in unique_sub_geos:
            print(sub_geo)
            calculate_dem_sub_geo(
                sub_geo = sub_geo,
                geo_disp_geo = geo_disp_geo,
                temp = temp,
                list_of_dicts = list_of_dicts,
                censusvars = censusvars,
                year = year,
                bucket = data_bucket)

    calculated_demo_bias_subgeo_time = time.perf_counter()
    print("Calculated demographic bias scores for sub-geographies")

    # Creating dataframe from list of dicts which has demographic disparity
    # scores for all relevant geogarphies (and sub-geographies)
    stats_df = pd.DataFrame(list_of_dicts)


    # Add baseline pop column to help Ben allocate the vars to each graph
    stats_df["baseline_pop"] = np.select(
        [
            stats_df["census_var"].str.contains("_pov_"),
            stats_df["census_var"].str.contains("_under18_"),
        ],
        ["pov_pop", "under18_pop"],
        default="total_pop",
    )

    ###########################################################
    ########## Write out demographic disparity outputs  #######
    ###########################################################

    # Write out JSON and CSVs in s3 with public-read ACLs for public download
    demo_json_key = "output-data/demographic-bias/json/" + system_key + ".json"
    place_df_as_json_in_s3(
        df=stats_df, bucket=trigger_bucket, key=demo_json_key, acl="public-read"
    )

    demo_csv_key = "output-data/demographic-bias/csv/" + system_key + ".csv"
    place_df_as_csv_in_s3(df=stats_df, bucket=trigger_bucket, key=demo_csv_key)

    ###########################################################
    ########## Write out final JSON for API  ##################
    ###########################################################

    # This is the JSON which contains, the demographic data as a json, the
    # geographic data as a geojson, all the messages, download_links, baseline
    # min/maxes, and bounding boxes. This is used by the frontend to generate
    # the interactive disparity charts and maps.

    # Note we remove the weighted_counts as this column can get really large and
    # trigger a mapbox error on frontend: `Uncaught Error: Given variant doesn't
    # fit into 10 bytes`. We still keep the weighted_count column in the csv and
    # geojson data downloads for user. This is safe to do here bc this is
    # the last time we are using the cols object
    cols.remove("weighted_counts")

    # Minify column names to limit size of API response
    temp_geo_cols = minify(temp_geo[cols], "geo_bias_data")

    # If geo = national, use generalized state geometries to lower size of API
    # response to be within API Gateway's built in limit of 5 MB payloads
    if geo == "national":
        temp_geo_cols = use_generalized_state_geometries(
            temp_geo_cols,
            year=year,
            bucket=data_bucket
            )

    geojson_dict = json.loads(temp_geo_cols.round(5).to_json())
    # Note we replace NA's with nulls as pandas to_dict doesn't automatically
    # convert NaNs to the json friendly nulls. stats_df should not
    # have any null values so we do this just to be safe
    stats_df = minify(stats_df, "demographic_bias_data")
    demo_json_dict = stats_df.round(5).fillna("null").to_dict(orient="records")


    # Calculate mins/maxes of baseline diff columns.
    all_min_max_cols = [col for col in temp_geo_cols if (col.startswith('p_') or col.startswith('d'))]


    mins = temp_geo_cols[all_min_max_cols].min().round(5).to_dict()
    # Add _min as suffix to all keys in the dictionary
    mins = {str(key) + "_min": val for key, val in mins.items()}

    maxes = temp_geo_cols[all_min_max_cols].max().round(5).to_dict()
    # Add _max as suffix to all keys in the dictionary
    maxes = {str(key) + "_max": val for key, val in maxes.items()}

    # Combine keys of mins and maxes dicts. There are no duplicate keys across
    # these two dicts, so this is essentially just adding two dicts together.
    # Below syntax is only available as of Python 3.5+
    bounds = {**mins, **maxes}

    # Construct sub_geo list with flag for whether that list should be grayed
    # out for state and national, empty list otherwise
    if geo in ["state", "national"]:
        unique_sub_geo_json = make_unique_sub_geo_json(temp_geo, geo)
    else:
        unique_sub_geo_json = []

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
        # Hardcode us continental bounds for now
        # "bbox": [-86.510291, 32.449727, -86.465376, 32.505159],
        "bbox": generate_bbox(geo = geo, temp_df = temp),
        "demographic_bias_sub_geo_list": unique_sub_geo_json,
    }

    # Upload final API response json to S3 with private acl so only API can
    # access this file
    s3 = boto3.client("s3", region_name=file_bucket_region)
    s3.put_object(
        Body=(bytes(json.dumps(full_result).encode("UTF-8"))),
        Bucket=trigger_bucket,
        Key="output-data/api-response/" + system_key + ".json",
        ACL="private",
    )

    end_time = time.perf_counter()

    status_json["updates"]["finished"] = True
    status_json["updates"]["total_time"] = str(round(end_time - start_time,2))

    update_status_json(
        system_key=system_key, status_json=status_json, bucket=trigger_bucket
    )

    ###########################################################
    ########## Create and write out log data  #################
    ###########################################################

    logdata = pd.DataFrame(
        {
            "num_rows_file": status_json["updates"]["num_rows_file"],
            "s3_input_bucket": trigger_bucket,
            "s3_input_key": file_key,
            "request_id": request_id,
            "full_geoid_name": full_geoid_name,
            "polygon_geoid": polygon_geoid,
            "total_time": str(round(end_time - start_time,2)),
            "data_readin_time":
                str(round(read_in_point_data_time - start_time,2)),
            "performed_sjoin_time":
                str(round(performed_spatial_join_time - start_time,2)),
            "calculated_geo_bias_time":
                str(round(calculated_geo_bias_time - start_time,2)),
            "calculated_demo_bias_time":
                str(round(calculated_demo_bias_time - start_time,2)),
            "calculated_demo_bias_subgeo_time":
                str(round(calculated_demo_bias_subgeo_time - start_time,2))
        },
        index={1},
    )

    log_data_fname = "output-data/logs/" + system_key + ".json"
    place_df_as_json_in_s3(
        df=logdata, bucket=trigger_bucket, key=log_data_fname, acl="private"
    )

    # Print statement with summary of request. Can also be used to help with
    # debugging as these print statements are writted out to Cloudwatch logs.
    print(
        "Finished request -  "
        + geo
        + "; "
        + full_geoid_name
        + "; "
        + str(status_json["updates"]["num_rows_file"])
        + " points geocoded"
        + "; "
        + str(round(end_time - start_time,2))
        + " seconds elapsed"
    )

    # Have lambda function return some data to help with debugging.
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
