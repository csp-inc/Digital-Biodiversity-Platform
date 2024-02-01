# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Optional

import numpy as np
import xarray as xr
from aggregation import _aggregate_chip_spatially
from xrspatial import aspect
from xrspatial import slope as calculate_slope


def preprocess(xarr: xr.DataArray) -> xr.DataArray:
    """
    Preprocesses elevation data for further processing.
        * Squeezes 3D array into 2D array

    Parameters
    ----------
    xarr : xr.DataArray
        Elevation data aggregate from Planetary Computer

    Returns
    -------
    xr.DataArray
        2D array of elevation data
    """
    # Which means that a chip is separated in two
    if ("time" in xarr.dims) and (xarr.sizes["time"]) >= 2:
        xarr = xarr.max(dim="time", skipna=True)
    return xarr.squeeze()


def calculate_aspect(xarr: xr.DataArray, transform: Optional[str] = None) -> xr.DataArray:
    """
    Calculates the aspect value of a given elevation data aggregate.
    Сalculates, for all cells in the array, the downward slope direction of each cell
    based on the elevation of its neighbors in a 3x3 grid. The value is measured
    clockwise in degrees with 0 (due north), and 360 (again due north).
    Values along the edges are not calculated.

    Direction of the aspect can be determined by its value:
        From 0 to 22.5: North
        From 22.5 to 67.5: Northeast
        From 67.5 to 112.5: East
        From 112.5 to 157.5: Southeast
        From 157.5 to 202.5: South
        From 202.5 to 247.5: West
        From 247.5 to 292.5: Northwest
        From 337.5 to 360: North

    Note that values of -1 denote flat areas.

    Parameters
    ----------
    xarr : xr.DataArray
        2D array of elevation data
    transform : str, optional
        Transformation to be applied to the aspect array (sin, cos), by default None
        Transform parameter allows conversion of aspect from degrees into a
        continuous variable by calculating cosine ("northness", a value from 2 to 0)
        and sine ("eastness", a value also from 2 to 0).

    Returns
    -------
    xr.DataArray
        2D aggregate array of calculated aspect values in degrees
    """
    aspect_xr = aspect(xarr)
    if transform is None:
        return aspect_xr
    elif transform == "sin":
        transform_xr = np.sin(np.deg2rad(aspect_xr)) + 1
    elif transform == "cos":
        transform_xr = np.cos(np.deg2rad(aspect_xr)) + 1
    else:
        raise ValueError("No transformation defined ")
    result_xr = transform_xr.where(aspect_xr != -1.0, -1.0)
    return result_xr


def center_crop(arr: np.ndarray) -> np.ndarray:
    """
    Center-crops a 2D array based on it's smallest dimension

    Parameters
    ----------
    arr : np.ndarray
        Input array to crop

    Returns
    -------
    np.ndarray
        Center-cropped array

    Raises
    ------
    ValueError
        If the input array is not a 2D array
    """
    if len(arr.shape) != 2:
        raise ValueError("Input should be a 2D array")
    side = np.min(arr.shape)
    y, x = arr.shape
    x_offset = (x - side) // 2
    y_offset = (y - side) // 2
    return arr[y_offset : y - y_offset, x_offset : x - x_offset]


def calculate_features(ds: xr.Dataset) -> dict:
    """
    Calculates NASADEM features based on input elevation array.
    Performs the following steps:
      * Calculates slope and aspect
      * Center-crops chips
      * Aggregates resulting chips using mean

    Parameters
    ----------
    ds : xr.Dataset
        Input elevation array

    Returns
    -------
    dict
        Calculated features as dictionary
    """
    elevation = preprocess(ds.data)
    data = {
        "elevation": elevation,
        "slope": calculate_slope(elevation),
        "aspect": calculate_aspect(elevation, transform="cos"),
    }
    result = {}
    for name, xarr in data.items():
        crop = center_crop(xarr.to_numpy())
        arr = np.expand_dims(crop, axis=[0, 1])
        agg = _aggregate_chip_spatially(arr, "mean")
        result[f"nasadem_{name}_total"] = agg[0][0]
    return result
