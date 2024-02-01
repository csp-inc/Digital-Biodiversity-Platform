# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import List, Union

import numpy as np
import xarray as xr


# In order to compute an accurate yearly aggregates we need to weight months according
# to the number of days in each month
def compute_monthly_weights(ds: xr.Dataset):
    """
    compute weights for each month of the year according to the number of days per month
    so we can compute accurate
    annual aggregates from monthly data

    Parameters
    ----------
    variable : xr.DataArray
        xarray Dataset representing monthly values for which we want to compute
        an annual aggregate

    Returns
    -------
    xr.DataArray
        weight for each month of the year for an accurate annual
    """

    days_per_month = ds.time.dt.days_in_month
    w = days_per_month.groupby("time.year") / days_per_month.groupby("time.year").sum()

    return w


# aggregate from monthly to annual according to agg_dict
def compute_annual_aggregate(
    variable: xr.DataArray, agg_func: str, monthly_weights: xr.DataArray
):
    """
    aggregate monthly dataset to annual according to agg_func

    Parameters
    ----------
    variable : xr.DataArray
        xarray Dataset variable representing monthly values for which we want to
        compute an annual aggregate
    agg_func: str
        aggregation function along the time axis, e.g. 'mean', 'sum'
    monthly_weights: xr.DataArray
        weight for each month of the year based on number of days for each month

    Returns
    -------
    xr.Dataset
        annual aggregate
    """

    isnull = variable.isnull()
    mask = xr.where(isnull, 0.0, 1.0)
    mask_sum = (mask * monthly_weights).resample(time="AS").sum(dim="time")
    temporal_sum = (variable * monthly_weights).resample(time="AS").sum(dim="time")

    if agg_func == "mean":
        return temporal_sum / mask_sum
    else:
        return temporal_sum


# Compute required aggregate for each variable
def terra_climate_annuals(
    dataset: xr.Dataset, aggregation_dict: dict, monthly_weights: xr.DataArray
):
    """
    aggregate monthly dataset to annual according to aggregation_dict

    Parameters
    ----------
    dataset : xr.Dataset
        xarray Dataset representing monthly values for which we want to
        compute an annual aggregate
    aggregation_dict: dict
        mapping of each variable in dataset (keys) to the proper agg function
    monthly_weights: xr.DataArray
        weight for each month of the year based on number of days for each month

    Returns
    -------
    xr.Dataset
        annual aggregate Dataset
    """

    annuals = {}
    k = None
    for key, v in aggregation_dict.items():
        k = key.lower()
        annuals[k] = compute_annual_aggregate(dataset[k], v, monthly_weights)

    # since all variables have the same coords
    ds = xr.Dataset(data_vars=annuals, coords=annuals[k].coords)

    return ds


# Since the terraClimate dataset has a very coarse resolution we take a shapefile
# centroid as the point of interest
# IMPORTANT Note: If the shapefile represents a large area, comparable to the
# resolution of terraClimate, this will
# need to be revisited
def get_nearest_point(xarr_dataset: xr.Dataset, query_point: List[Union[int, float]]):
    """
    compute nearest (lon, lat) climate data sample to input query point

    Parameters
    ----------
    xarr_dataset : xr.Dataset
        xarray Dataset representing monthly values from which we want to compute
        an annual aggregate
    query_point: list[Union[int, float]]
        query point coordinates in (lon, lat) for which we want to compute
        the annual aggregate

    Returns
    -------
    xr.Dataset
        nearest (lon, lat) climate data sample to query point
    """

    lon, lat = query_point
    lats = xarr_dataset.coords["lat"].values
    lons = xarr_dataset.coords["lon"].values
    i_lat = np.argmin(np.abs(lats - lat))
    i_lon = np.argmin(np.abs(lons - lon))
    subset = xarr_dataset.isel(lat=[i_lat], lon=[i_lon])
    return subset


def transform_dataset(ds: xr.Dataset) -> xr.Dataset:
    """
    Transforms multiple TerraClimate variables into one multi-dimensional and rechunks
    this variable.
    Input: 1 x 1 x 120 Dataset with 14 variables, output: 14 x 1 x 1 x 120 Dataset
    Parameters
    ----------
    ds : xr.Dataset
        TerraClimate DataSet

    Returns
    -------
    xr.Dataset
        Transformed
    """

    variables = []
    vars_names = []
    drop_vars = []
    for key in ds.variables.keys():
        # only consider those variables that have lon/lat/time dimensions;
        # necessary to avoid loading other variables such as CRS
        if key not in ["lon", "lat", "time", "band"]:
            drop_vars.append(key)
            if len(ds.variables[key].dims) == 3:
                variables.append(ds.variables[key])
                vars_names.append(key)

    variables = xr.Variable.concat(variables, dim="band")

    variables = xr.DataArray(
        variables,
        dims=("band", "time", "lat", "lon"),
        coords={
            "time": ds.coords["time"],
            "band": vars_names,
            "lat": ds.coords["lat"],
            "lon": ds.coords["lon"],
        },
    )

    new_ds = ds.drop_vars(drop_vars)
    new_ds.coords["band"] = vars_names
    new_ds["data"] = variables

    return new_ds


def preprocess_for_aggregation(ds: xr.Dataset) -> xr.Dataset:
    """
    Adjusts coords names and dims order of the TerraClimate chips to correspond
    to the following format:
    time x band x Y x X

    Parameters
    ----------
    ds : xr.Dataset
        TerraClimate chip, containing time, band, Y, X coords in any order

    Returns
    -------
    xr.Dataset
        Processed TerraClimate chip (time x band x Y x X)
    """
    rename_dict = {"lat": "y", "lon": "x"}
    if "feature" in ds.keys():
        rename_dict["feature"] = "band"
    return (
        ds.rename_dims(rename_dict)
        .rename(rename_dict)
        .set_index({"time": "time", "band": "band", "x": "x", "y": "y"})
        .transpose("time", "band", "y", "x")
        .set_coords(["time", "band", "x", "y"])
    )
