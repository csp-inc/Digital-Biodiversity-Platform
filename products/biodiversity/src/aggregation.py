# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from typing import Dict

import numpy as np
import pandas as pd
import xarray as xr
from datasets.multispectral import create_cloud_mask


def _aggregate_chip_spatially(data, agg_func="mean") -> np.ndarray:  # noqa: C901
    """
    Aggregate chip data spatially.

    Parameters
    ----------
    data : np.ndarray
        Contains data for a single chip, expected ndim=4 (time, band, y, x).
    agg_func : str; default='mean'
        Defines the type of aggregation to apply in the spatial dimension.
        Options: mean, median, std, sum, min, max, iqr, mode.

    Returns
    -------
    np.ndarray
        Spatially aggregated data, ndim=2.
    """

    # spatial aggregation
    temp_data = None
    if agg_func == "mean":
        temp_data = np.nanmean(data, axis=(2, 3))
    elif agg_func == "median":
        temp_data = np.nanmedian(data, axis=(2, 3))
    elif agg_func == "std":
        temp_data = np.nanstd(data, axis=(2, 3))
    elif agg_func == "sum":
        temp_data = np.nansum(data, axis=(2, 3))
    elif agg_func == "min":
        temp_data = np.nanmin(data, axis=(2, 3))
    elif agg_func == "max":
        temp_data = np.nanmax(data, axis=(2, 3))
    elif agg_func == "iqr":
        temp_data = np.nanpercentile(data, 75, axis=(2, 3)) - np.nanpercentile(
            data, 25, axis=(2, 3)
        )
    elif agg_func == "mode":
        temp_data = np.zeros(data.shape[:2])
        for i in np.arange(data.shape[0]):
            for j in np.arange(data.shape[1]):
                temp = data[i, j, ...]
                temp = temp[~np.isnan(temp)]
                values, counts = np.unique(temp, return_counts=True)
                try:
                    temp_data[i, j] = values[np.argmax(counts)]
                except ValueError:
                    # input all nan, return nan
                    temp_data[i, j] = np.nan
    return temp_data


def _aggregate_chip_spatially_perc_total(
    data,
    category_dict: Dict = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2},
    num_bins: int = 3,
    threshold: float = 0.05,
):
    """
    Aggregate chip data spatially for categorical data by percent category.
    The percent category is binned according to category_dict.
    Input data should be of form (time, band, y, x) with only one band dimension.

    Parameters
    ----------
    data : np.ndarray
        Contains data for a single chip, expected ndim=4 (time, band, y, x).
    category_dict : Dict, default suited for surface water dataset
        Defines category bins in form {value: categorical_bin_index}.
        Every value in the input data must be represented as a key in category_dict
    num_bins: int
        Number of category bins (i.e. unique values in category_dict.values())
    threshold: int
        Fraction of np.nans to accept. Input scenes with more than this fraction of nans
        will be ignored

    Returns
    -------
    np.ndarray
        Spatially aggregated data, ndim=2.
    """

    result = np.zeros(shape=(data.shape[0], num_bins))
    for t_idx in np.arange(data.shape[0]):
        val_cnts = np.unique(data[t_idx], return_counts=True)
        total = val_cnts[1].sum()

        # check against threshold
        total_nan = np.isnan(data[t_idx]).sum()
        total_pxls = data.shape[-1] * data.shape[-2]
        if (total_nan / total_pxls) >= threshold:
            result[t_idx, :] = np.nan
            continue

        # for each unique value, compute perc_total and
        # add into corresponding category_bin
        for val, cnt in zip(*val_cnts):
            perc = cnt / total
            try:
                # returns appropriate category as bin_idx, else None if val is NaN
                bin_idx = category_dict.get(val)
                if bin_idx is None:
                    continue
            except KeyError:
                raise KeyError(
                    f"Caterogical value {val} from data input did not have"
                    " an associated categorical bin in category_dict"
                )

            result[t_idx, bin_idx] += perc

    return result


def _aggregate_chip_temporally(data, indexes, agg_func="mean"):  # noqa: C901
    """
    Aggregate chip data temporally.

    Parameters
    ----------
    data : np.ndarray
        Spatially aggregated data for a single chip (output of aggregate_chip_spatially),
        expected ndim=2 (time, band).
    indexes : list of tuples of int
        Array indexes defining temporal slices of data. An index tuple of (-1, -1)
        indicates the time range not being present in the data.
    agg_func : str; default='mean'
        Defines the type of aggregation to apply in the temporal dimension.
        Options: mean, median, std, sum, min, max, iqr, mode.

    Returns
    -------
    np.ndarray
        Temporally aggregated data, ndim=2.
    """

    temp_data = []
    for idx in indexes:
        idx1, idx2 = idx
        if idx1 != -1:
            if agg_func == "mean":
                temp_data.append(np.nanmean(data[idx1:idx2, ...], axis=0))
            elif agg_func == "median":
                temp_data.append(np.nanmedian(data[idx1:idx2, ...], axis=0))
            elif agg_func == "std":
                temp_data.append(np.nanstd(data[idx1:idx2, ...], axis=0))
            elif agg_func == "sum":
                temp_data.append(np.nansum(data[idx1:idx2, ...], axis=0))
            elif agg_func == "min":
                temp_data.append(np.nanmin(data[idx1:idx2, ...], axis=0))
            elif agg_func == "max":
                temp_data.append(np.nanmax(data[idx1:idx2, ...], axis=0))
            elif agg_func == "iqr":
                temp_data.append(
                    np.nanpercentile(data[idx1:idx2, ...], 75, axis=0)
                    - np.nanpercentile(data[idx1:idx2, ...], 25, axis=0)
                )
            elif agg_func == "mode":
                modes = []
                for i in np.arange(data.shape[1]):
                    temp = data[idx1:idx2, i, ...]
                    temp = temp[~np.isnan(temp)]
                    values, counts = np.unique(temp, return_counts=True)
                    try:
                        modes.append(values[np.argmax(counts)])
                    except ValueError:
                        # all nans
                        modes.append(np.nan)
                temp_data.append(np.array(modes))
        else:
            # NaN output if time range not present in dataset
            arr = np.empty((data.shape[-1]))
            arr[:] = np.nan
            temp_data.append(arr)
    return np.stack(temp_data, axis=0)


def subset_chip(ds, chip_size):
    """
    Subsets center portion of a chip.

    Parameters
    ----------
    ds : xarray Dataset or DataArray
        Contains the chip data.
    chip_size : float
        Center portion of the chip size that is retained in both x- and y-direction.
        Interpreted as a fraction of the full size.

    Returns
    -------
    xarray Dataset or DataArray
        Subset version of the input.
    """

    if chip_size < 1.0:
        x_size = chip_size * len(ds.indexes["x"])
        y_size = chip_size * len(ds.indexes["y"])
        ds = ds.isel(
            x=slice(int(1 / 2 * x_size), int(-1 / 2 * x_size)),
            y=slice(int(1 / 2 * y_size), int(-1 / 2 * y_size)),
        )

    return ds


def create_features_from_chip(  # noqa: C901
    xarr,
    name,
    temporal_aggregation="seasonal",
    spatial_agg_func="mean",
    temporal_agg_func="mean",
    check_dims=False,
    return_as_dict=True,
    perc_total_kwargs: dict = {},
):
    """
    Aggregate chip data to a reduced vector input.
    NOTE: does not apply any masking, uses NumPy nan-functions (np.nanmean etc.).

    Parameters
    ----------
    xarr : xarray.Dataset
        Contains data for a single chip (dimensions: time, band, y, x).
    name : str
        Name of the dataset (used as prefix in feature names).
    temporal_aggregation : str; default='seasonal'
        Defines how to aggregate data along the temporal dimension.
        Options: total, yearly, seasonal, monthly.
    spatial_agg_func : str; default='mean'
        Defines the type of aggregation to apply in the spatial dimension.
        Options: mean, median, std, sum, min, max, iqr.
        Categorical options: perc_total. perc_total_kwargs parameter required.
    temporal_agg_func : str; default='mean'
        Defines the type of aggregation to apply in the temporal dimension.
        Options: mean, median, std, sum, min, max, iqr.
    check_dims : bool; default=False
        If True, checks the correct order of dims (time, band, y, x) and ensures
        that the time dimension is organized in ascending order.
    return_as_dict : bool; default=True
        If True, returns results as dict with keys=feature names, values=feature values.
        Otherwise returns an np.ndarray.
    perc_total_kwargs : Dict
        Only applies for spatial_agg_fun of "perc_total"
        Input keyword args (num_bins, category_dict, bin_names) for percent total calulation
        on categorical input data.
         num_bins: int - number of categorical bins in output
         category_dict: Dict - Defines category bins in form {value: categorical_bin_index}.
         bin_names: List[str] - String names corresponding to bins
         threshold: int - Threshold for ignoring cloud covered scenes
        Every value in the input data must be represented as a key in category_dict
        Example: {'num_bins': 4,
                  'category_dict': {0: 0, 1: 1, 2: 1, 3: 2, 4: 2},
                  'bin_names' : "nowater water wetland".split(),
                  'threshold': 0.05}.

    Returns
    -------
    return_as_dict=True
        dict
            Contains aggregated feature values.
    return_as_dict=False
        np.ndarray
            Contains aggregated feature values.
    """

    # handle xr.DataArray and xr.Dataset
    if isinstance(xarr, xr.Dataset):
        da = xarr.data
    else:
        da = xarr

    # ensure correct order of time dimension
    if check_dims:
        if "time" in da.dims:
            logging.info("Data will be sorted along time dimension.")
            da = da.sortby(["time"], ascending=True)
    data = np.array(da.data)

    # check dimensionality of input data, prepare band_names and time_coords
    # band_names: names of bands to use in output dict keys
    # time_coords: time coordinates in data
    band_names = []
    time_coords = []
    dims = tuple(da.sizes.keys())
    if data.ndim == 2:
        logging.info(
            "Provided dataset does not contain a time dimension."
            "Temporal aggregation will be skipped."
        )
        if check_dims:
            if dims[0] != "y" or dims[1] != "x":
                raise KeyError(
                    f"Dimensions do not match expected pattern (time, band, y, x): {dims}"
                )
        data = data[None, None, ...]
        band_names = ["none"]
        time_coords = ["none"]
    elif data.ndim == 3:
        if "band" not in da.dims:
            logging.info("Provided dataset does not contain a band dimension.")
            if check_dims:
                if dims[0] != "time" or dims[1] != "y" or dims[2] != "x":
                    raise KeyError(
                        "Dimensions do not match expected pattern"
                        f"(time, band, y, x): {dims}"
                    )
            data = data[:, None, ...]
            band_names = ["none"]
            time_coords = da.coords["time"].values
        elif "time" not in da.dims:
            logging.info(
                "Provided dataset does not contain a time dimension."
                "Temporal aggregation will be skipped."
            )
            if check_dims:
                if dims[0] != "band" or dims[1] != "y" or dims[2] != "x":
                    raise KeyError(
                        "Dimensions do not match expected pattern"
                        f"(time, band, y, x): {dims}"
                    )
            data = data[None, ...]
            band_names = da.coords["band"].values
            time_coords = ["none"]
    else:
        if check_dims:
            if dims[0] != "time" or dims[1] != "band" or dims[2] != "y" or dims[3] != "x":
                raise KeyError(
                    f"Dimensions do not match expected pattern(time, band, y, x): {dims}"
                )
        band_names = da.coords["band"].values
        time_coords = da.coords["time"].values

    # spatial aggregation. Special case for perc_total
    if spatial_agg_func == "perc_total":
        band_names = perc_total_kwargs.pop("bin_names")

        temp_data = _aggregate_chip_spatially_perc_total(data, **perc_total_kwargs)

    else:
        temp_data = _aggregate_chip_spatially(data, agg_func=spatial_agg_func)

    # preparing settings for temporal aggregation
    # coords: temporal coordinates preprocessed for different kinds of aggregation
    # intervals: the min/max values in coords for determining temporal slices
    # time_names: names of temporal slices to use in output dict keys
    intervals = []
    time_names = []
    if time_coords[0] != "none":
        if data.shape[0] > 1:
            if temporal_aggregation == "total":
                time_coords = time_coords.astype("datetime64[M]").astype(
                    int
                )  # pyright: reportGeneralTypeIssues=false
                intervals = [(np.min(time_coords), np.max(time_coords))]
                time_names = ["total"]
            elif temporal_aggregation == "yearly":
                time_coords = (
                    time_coords.astype("datetime64[Y]").astype(str).astype(int)
                )  # pyright: reportGeneralTypeIssues=false
                intervals = [(y, y) for y in np.unique(time_coords)]
                time_names = [f"y_{i+1}" for i in np.arange(len(np.unique(time_coords)))]
            elif temporal_aggregation == "seasonal":
                time_coords = (
                    time_coords.astype("datetime64[M]").astype(int) % 12 + 1
                )  # pyright: reportGeneralTypeIssues=false
                intervals = [(1, 3), (4, 6), (7, 9), (10, 12)]
                time_names = ["s_jfm", "s_amj", "s_jas", "s_ond"]
            elif temporal_aggregation == "monthly":
                time_coords = (
                    time_coords.astype("datetime64[M]").astype(int) % 12 + 1
                )  # pyright: reportGeneralTypeIssues=false
                intervals = [(i, i) for i in np.arange(1, 13, 1)]
                time_names = [f"m_{m}" for m in np.arange(1, 13, 1)]
        else:
            time_coords = time_coords.astype("datetime64[M]").astype(
                int
            )  # pyright: reportGeneralTypeIssues=false
            intervals = [(np.min(time_coords), np.max(time_coords))]
            time_names = ["total"]
    else:
        time_coords = np.array([0, 1])
        intervals = [(0, 1)]
        time_names = ["total"]

    # temporal aggregation
    # assumes the time dimension to be ordered ascending which is normally the case
    # for chips created from STAC APIs by default
    indexes = []
    for i, interval in enumerate(intervals):
        idx = np.argwhere((time_coords >= interval[0]) & (time_coords <= interval[1]))

        if len(idx) == 0:
            idx = (-1, -1)
        elif len(idx) == 1:
            idx = (int(idx[0]), int(idx[0] + 1))
        elif len(idx) == 2:
            idx = (int(idx[0]), int(idx[1] + 1))
        elif len(idx) > 2:
            idx = (int(idx[0]), int(idx[-1] + 1))
        indexes.append(idx)
    out_data = _aggregate_chip_temporally(temp_data, indexes, agg_func=temporal_agg_func)

    # organizing data in dictionary
    if return_as_dict:
        agg_data = {}
        for i in np.arange(out_data.shape[0]):
            for j in np.arange(out_data.shape[1]):
                agg_data[f"{name}_{band_names[j]}_{time_names[i]}"] = out_data[i, j]
        return agg_data
    else:
        return out_data


def dask_spectral_aggregation_wrapper(gbifid, path, account_name, sas_token):  # noqa: C901
    """
    A wrapper for Landsat chip data aggregation on Dask.

    Parameters
    ----------
    gbifid : str/int
        The GBIF ID of the chip to be processed.
    path : str
        Path to the zarr file on ABS.
    account_name : str
        Name of the storage account.
    sas_token : str
        The SAS token to use for access to ABS.

    Returns
    -------
    bool
        Specifies if process was successful or not.
    pd.Series or tuple of str
        If process is successful, returns the aggregated vector as a pd.Series
        that can be added to a DataFrame.
        If process failed, returns the GBIF ID and the error message.
    """

    logger = logging.getLogger("distributed.worker")

    try:
        # loading zarr file
        ds = xr.open_zarr(
            path, storage_options=dict(account_name=account_name, sas_token=sas_token)
        )
        da = ds.data
        da.data = np.array(da.data)
    except Exception as e:
        msg = f"Error while opening/loading {gbifid}.zarr | {type(e).__name__}: {e}"
        logger.error(msg)
        return False, (gbifid, msg)

    try:
        # masking clouds
        cmask = create_cloud_mask(
            da, sensor="l8", cloud_confidence="high", cirrus_confidence=None
        )
        da = da.where(cmask[:, None, ...] == 0)
    except Exception as e:
        msg = f"Error while masking clouds in {gbifid}.zarr | {type(e).__name__}: {e}"
        logger.error(msg)
        return False, (gbifid, msg)

    try:
        # rescale DNs to actual surface reflectance
        da.data = da.data * 0.0000275 - 0.2
    except Exception as e:
        msg = (
            f"Error while transforming DN to reflectance in {gbifid}.zarr"
            f"| {type(e).__name__}: {e}"
        )
        logger.error(msg)
        return False, (gbifid, msg)

    try:
        # creating features (dict)
        features = create_features_from_chip(
            da,
            "l8",
            temporal_aggregation="seasonal",
            spatial_agg_func="mean",
            temporal_agg_func="median",
            check_dims=False,
            return_as_dict=True,
        )
    except Exception as e:
        msg = (
            f"Error while producing feature vector for {gbifid}.zarr"
            f"| {type(e).__name__}: {e}"
        )
        logger.error(msg)
        return False, (gbifid, msg)

    # dropping qa_pixel stats
    for key in list(features.keys()):  # type: ignore
        if "qa_pixel" in key or "lwir11" in key:
            features.pop(key)  # type: ignore

    # adding new row
    features["gbifid"] = int(gbifid)
    row = pd.Series(features)
    return True, row


def dask_soil_aggregation_wrapper(gbifid, path, account_name, sas_token):  # noqa: C901
    """
    A wrapper for gNATSGO soil raster chip data aggregation on Dask.
    TODO: the dask_x_aggregation_wrapper functions can be refactored

    Parameters
    ----------
    gbifid : str/int
        The GBIF ID of the chip to be processed.
    path : str
        Path to the zarr file on ABS.
    account_name : str
        Name of the storage account.
    sas_token : str
        The SAS token to use for access to ABS.

    Returns
    -------
    bool
        Specifies if process was successful or not.
    pd.Series or tuple of str
        If process is successful, returns the aggregated vector as a pd.Series
        that can be added to a DataFrame.
        If process failed, returns the GBIF ID and the error message.
    """

    logger = logging.getLogger("distributed.worker")

    try:
        # loading zarr file
        ds = xr.open_zarr(
            path, storage_options=dict(account_name=account_name, sas_token=sas_token)
        )
        da = ds.data
        da.data = np.array(da.data)
    except Exception as e:
        msg = f"Error while opening/loading {gbifid}.zarr | {type(e).__name__}: {e}"
        logger.error(msg)
        return False, (gbifid, msg)

    # clean nodata vals in chip
    da = da.where(da.sel(band="soc0_100") > -9000)

    try:
        # creating features (dict) for mukey (spatial aggr by mode)
        features = create_features_from_chip(
            da.sel(band=["mukey"]),
            name="soil",
            spatial_agg_func="mode",
            check_dims=False,
            return_as_dict=True,
        )

        features_therest = create_features_from_chip(
            da.sel(
                band=[
                    "aws0_100",
                    "soc0_100",
                    "tk0_100a",
                    "tk0_100s",
                    "musumcpct",
                    "musumcpcta",
                    "musumcpcts",
                ]
            ),
            name="soil",
            spatial_agg_func="mean",
            check_dims=False,
            return_as_dict=True,
        )

        features.update(features_therest)  # type: ignore

    except Exception as e:
        msg = (
            f"Error while producing feature vector for {gbifid}.zarr"
            f"| {type(e).__name__}: {e}"
        )
        logger.error(msg)
        return False, (gbifid, msg)

    # adding new row
    features["gbifid"] = int(gbifid)
    row = pd.Series(features)
    return True, row
