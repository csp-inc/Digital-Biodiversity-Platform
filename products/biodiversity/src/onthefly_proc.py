# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import uuid

import numpy as np
import pandas as pd
import xarray as xr

from aggregation import create_features_from_chip
from azure_logger import AzureLogger
from chips_utils import create_chip_bbox
from datasets.indices import calculate_multispectral_indices
from datasets.multispectral import create_cloud_mask
from datasets.nasadem import calculate_features
from datasets.surfacewater import dswe_tests, interpret_dswe_from_tests

# LANDSAT 8 FEATURES


def dask_landsat8_features_wrapper_onthefly(  # noqa: C901
    gbif_items: pd.DataFrame,
    xarr: xr.DataArray,
    buffer_size: float,
    temporal_aggregation: str = "seasonal",
    spatial_agg_func: str = "mean",
    temporal_agg_func: str = "mean",
    correlation_id: uuid.UUID = uuid.uuid4(),
):
    """
    Wrapper for creating features from Landsat-8 multispectral data to use
    in parallelized (distributed/delayed) Dask execution.
    Extracts chip for each entry in dataframe, masks clouds, converts data to
    reflectance values and calculates features.

    Parameters
    ----------
    gbif_items : pandas.DataFrame
        Contains the information about the GBIF datasets/locations.
        Subset of the larger GBIF dataframe.
    xarr: xarray.DataArray
        Contains data from which chips are produced (output of STACDataLoader.load_data()).
    buffer : float
        Buffer size in meters (UTM). Used to create buffer bounding box to
        extract chips.
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

    Returns
    -------
    pd.DataFrame
        Contains all aggregated features for each GBIF point in input.
    """

    feature_rows = []
    xarr = xarr.compute()

    for i, gbif_item in gbif_items.iterrows():
        gbifid = gbif_item["gbifid"]

        # create bbox
        bounds = create_chip_bbox(
            (gbif_item["decimallongitude"], gbif_item["decimallatitude"]),
            buffer_size,
            return_wgs84=False,
        )

        # indexing xarray
        if np.min(xarr.data.shape) > 0:
            xarr_ = xarr.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[3], bounds[1]))
        else:
            raise ValueError(f"Cannot subset xarray of shape {xarr.data.shape}.")

        # masking clouds
        cmask = create_cloud_mask(
            xarr_, sensor="l8", cloud_confidence="high", cirrus_confidence=None
        )
        xarr_ = xarr_.where(cmask[:, None, ...] == 0)

        # rescale DNs to actual surface reflectance
        xarr_.data = xarr_.data * 0.0000275 - 0.2

        # creating features (dict)
        features = create_features_from_chip(
            xarr_,
            "l8",
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
            check_dims=False,
            return_as_dict=True,
        )

        # dropping qa_pixel stats
        for key in list(features.keys()):  # pyright: reportGeneralTypeIssues=false
            if "qa_pixel" in key or "lwir11" in key:
                features.pop(key)

        # adding new row
        if isinstance(gbifid, str):
            features["gbifid"] = gbifid
        else:
            features["gbifid"] = int(gbifid)
        row = pd.Series(features)

        feature_rows.append(row)

    df_features = pd.DataFrame(feature_rows)
    if df_features["gbifid"].dtypes == "float64":
        df_features["gbifid"] = df_features["gbifid"].astype(np.int64)
    df_features = df_features.set_index("gbifid")

    return df_features


# NASA DEM FEATURES


def dask_nasadem_features_wrapper_onthefly(  # noqa: C901
    gbif_items,
    xarr,
    buffer_size,
    correlation_id: uuid.UUID = uuid.uuid4(),
):
    """
    Wrapper for creating features from NASA DEM data to use
    in parallelized (distributed/delayed) Dask execution.
    Extracts chip for each entry in dataframe, calculates DEM features
    in a standard.

    Parameters
    ----------
    gbif_items : pandas.DataFrame
        Contains the information about the GBIF datasets/locations.
        Subset of the larger GBIF dataframe.
    xarr: xarray.DataArray
        Contains data from which chips are produced (output of STACDataLoader.load_data()).
    buffer : float
        Buffer size in meters (UTM). Used to create buffer bounding box to
        extract chips.

    Returns
    -------
    pd.DataFrame
        Contains all aggregated features for each GBIF point in input.
    """

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    xarr = xarr.compute()

    feature_rows = []
    for i, gbif_item in gbif_items.iterrows():
        gbifid = gbif_item["gbifid"]

        try:
            # create bbox
            bounds = create_chip_bbox(
                (gbif_item["decimallongitude"], gbif_item["decimallatitude"]),
                buffer_size,
                return_wgs84=False,
            )

            # indexing xarray
            if np.min(xarr.data.shape) > 0:
                xarr_ = xarr.sel(
                    x=slice(bounds[0], bounds[2]), y=slice(bounds[3], bounds[1])
                )
            else:
                raise ValueError(f"Cannot subset xarray of shape {xarr.data.shape}.")

            # calculate DEM features
            if not isinstance(xarr_, xr.Dataset):
                xarr_ = xarr_.to_dataset(name="data")
            features = calculate_features(xarr_)

            # adding new row
            if isinstance(gbifid, str):
                features["gbifid"] = gbifid
            else:
                features["gbifid"] = int(gbifid)
            row = pd.Series(features)

            feature_rows.append(row)

        except Exception as ex:
            azure_logger.exception(ex)
            azure_logger.log(
                "Exception in dask_nasadem_features_wrapper_onthefly, skipping row",
                **{"gbifid": {gbifid}},
            )

    df_features = pd.DataFrame(feature_rows)

    if len(feature_rows) > 0:
        if df_features["gbifid"].dtypes == "float64":
            df_features["gbifid"] = df_features["gbifid"].astype(np.int64)
        df_features = df_features.set_index("gbifid")

    return df_features


# INDICES FEATURES


def dask_index_features_wrapper_onthefly(  # noqa: C901
    gbif_items,
    xarr,
    buffer_size,
    index_formulas,
    temporal_aggregation="seasonal",
    spatial_agg_func="mean",
    temporal_agg_func="mean",
    convert_to_reflectance=True,
):
    """
    Wrapper for creating features from indices calculated from Landsat-8 multispectral
    data to use in parallelized (distributed/delayed) Dask execution.
    Extracts chip for each entry in dataframe, masks clouds, (optionally) converts data to
    reflectance values, calculates indices according to formulas and calculates features.

    Parameters
    ----------
    gbif_items : pandas.DataFrame
        Contains the information about the GBIF datasets/locations.
        Subset of the larger GBIF dataframe.
    xarr: xarray.DataArray
        Contains data from which chips are produced (output of STACDataLoader.load_data()).
    buffer : float
        Buffer size in meters (UTM). Used to create buffer bounding box to
        extract chips.
    index_formulas : dict
        Contains the names (keys) and formulas (values) of the indices
        to be calculated, with band names in square brackets, e.g.:
        {'ndvi': '([nir08]-[red]) / ([nir08]+[red])'}.
        Each entry in the dictionary will be an item along the 'band'
        dimension of the output array.
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
    convert_to_reflectance : bool; default=True
        If True, multispectral data is converted from DN to reflectance.
        This is recommended to ensure correct index calculation etc.
        Currently only Landsat-8 is implemented.

    Returns
    -------
    pd.DataFrame
        Contains all aggregated features for each GBIF point in input.
    """

    xarr = xarr.compute()

    feature_rows = []
    for i, gbif_item in gbif_items.iterrows():
        gbifid = gbif_item["gbifid"]

        # create bbox
        bounds = create_chip_bbox(
            (gbif_item["decimallongitude"], gbif_item["decimallatitude"]),
            buffer_size,
            return_wgs84=False,
        )

        # indexing xarray
        if np.min(xarr.data.shape) > 0:
            xarr_ = xarr.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[3], bounds[1]))
        else:
            raise ValueError(f"Cannot subset xarray of shape {xarr.data.shape}.")

        # masking clouds
        cmask = create_cloud_mask(
            xarr_, sensor="l8", cloud_confidence="high", cirrus_confidence=None
        )
        xarr_ = xarr_.where(cmask[:, None, ...] == 0)

        if convert_to_reflectance:
            # rescale DNs to actual surface reflectance
            xarr_.data = xarr_.data * 0.0000275 - 0.2

        # calculate indices
        xarr_ = calculate_multispectral_indices(xarr_, index_formulas)

        # creating features (dict)
        features = create_features_from_chip(
            xarr_,
            "indices",
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
            check_dims=False,
            return_as_dict=True,
        )

        # adding new row
        if isinstance(gbifid, str):
            features["gbifid"] = gbifid
        else:
            features["gbifid"] = int(gbifid)
        row = pd.Series(features)

        feature_rows.append(row)

    df_features = pd.DataFrame(feature_rows)
    if df_features["gbifid"].dtypes == "float64":
        df_features["gbifid"] = df_features["gbifid"].astype(np.int64)
    df_features = df_features.set_index("gbifid")

    return df_features


# WATER FEATURES


def dask_water_features_wrapper_onthefly(  # noqa: C901
    gbif_items,
    xarr,
    buffer_size,
    temporal_aggregation="seasonal",
    spatial_agg_func="mean",
    temporal_agg_func="mean",
    dt_params=None,
    perc_total_kwargs=None,
    correlation_id: uuid.UUID = uuid.uuid4(),
):
    """
    Wrapper for creating features from surface water extent calculated from
    Landsat-8 multispectral
    data to use in parallelized (distributed/delayed) Dask execution.
    Extracts chip for each entry in dataframe, calculates necessary indices,
    generates surface water
    masks, masks clouds and calculates features.

    Parameters
    ----------
    gbif_items : pandas.DataFrame
        Contains the information about the GBIF datasets/locations.
        Subset of the larger GBIF dataframe.
    xarr: xarray.DataArray
        Contains data from which chips are produced (output of STACDataLoader.load_data()).
    buffer : float
        Buffer size in meters (UTM). Used to create buffer bounding box to
        extract chips.
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
    dt_params : dict; default=None
        Dictionary of diagnostic test parameters.
        If None, resorts to default values.
    perc_total_kwargs : dict; default=None
        Defines settings for spatial_agg_func 'perc_total'.
        If None, resorts to default values.
        Input keyword args (num_bins, category_dict, bin_names) for percent total calulation
        on categorical input data:
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
    pd.DataFrame
        Contains all aggregated features for each GBIF point in input.
    """

    if dt_params is None:
        dt_params = {
            # Test 1, 2, 3
            "wigt": 124,  # Wetness index threshold, default 0.124
            # (index values are multipled by 1e5)
            "awgt": 0,  # Automated water extent shadow threshold
            # Test 4
            "pswt_1_mndwi": -4400,  # Partial surface water test-1 MNDWI threshold
            # default -4400
            "pswt_1_swir1": 9000,  # Partial surface water test-1 SWIR1 threshold
            # default 9000
            "pswt_1_nir": 15000,  # Partial surface water test-1 NIR threshold
            # default 15000
            "pswt_1_ndvi": 0.7 * 10000,  # Partial surface water test-1 NDVI threshold
            # Test 5
            "pswt_2_mndwi": -5000,  # Partial surface water test-3 MNDWI threshold
            "pswt_2_blue": 1000,  # Partial surface water test-3 Blue threshold
            "pswt_2_nir": 2500,  # Partial surface water test-3 NIR threshold, default 2500
            "pswt_2_swir1": 3000,  # Partial surface water test-3 SWIR1 threshold
            "pswt_2_swir2": 1000,  # Partial surface water test-3 SWIR2 threshold
        }

    if perc_total_kwargs is None:
        perc_total_kwargs = {
            "num_bins": 3,
            "category_dict": {0: 0, 1: 1, 2: 1, 3: 2, 4: 2},
            "bin_names": "nowater water wetland".split(),
            "threshold": 0.05,
        }

    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    xarr = xarr.compute()

    feature_rows = []
    for i, gbif_item in gbif_items.iterrows():
        gbifid = gbif_item["gbifid"]

        try:
            # create bbox
            bounds = create_chip_bbox(
                (gbif_item["decimallongitude"], gbif_item["decimallatitude"]),
                buffer_size,
                return_wgs84=False,
            )

            # indexing xarray
            if np.min(xarr.data.shape) > 0:
                xarr_ = xarr.sel(
                    x=slice(bounds[0], bounds[2]), y=slice(bounds[3], bounds[1])
                )
            else:
                raise ValueError(f"Cannot subset xarray of shape {xarr.data.shape}.")

            # masking clouds
            cmask = create_cloud_mask(
                xarr_, sensor="l8", cloud_confidence="high", cirrus_confidence=None
            )

            # calculate required indices
            indices = calculate_multispectral_indices(
                xarr_,
                {
                    "mndwi": "([green]-[swir16]) / ([green]+[swir16])",
                    "ndvi": "([nir08]-[red]) / ([nir08]+[red])",
                    "mbsr": "([green]+[red]) - ([nir08]+[swir16])",
                    "awesh": (
                        "[blue] + (2.5*[green]) - "
                        "(1.5*([nir08]+[swir16])) - (0.25*[swir22])"
                    ),
                },
            )

            # create xarray of required bands
            bands = xarr_.sel(band=["blue", "nir08", "swir16", "swir22"])

            # update band coords for backwards compatibility with DSWE functions
            compat_band_names = list(bands.coords["band"].values)
            compat_band_names[compat_band_names.index("nir08")] = "nir"
            compat_band_names[compat_band_names.index("swir16")] = "swir1"
            compat_band_names[compat_band_names.index("swir22")] = "swir2"
            bands = bands.assign_coords(band=compat_band_names)

            # drop superfluous vars, concat to one x
            indices = indices.drop_vars(
                [c for c in indices.coords if c not in ["x", "y", "time", "band"]]
            )
            bands = bands.drop_vars(
                [c for c in bands.coords if c not in ["x", "y", "time", "band"]]
            )
            indices = xr.concat(
                [indices, bands], dim="band", coords="minimal", compat="override"
            )

            # calculate surface water extent
            tests_xr = dswe_tests(indices, dt_params)
            dswe = interpret_dswe_from_tests(tests_xr)

            # mask out clouds
            dswe = dswe.expand_dims(dim={"band": ["dswe"]}, axis=1)
            dswe = dswe.where(cmask[:, None, ...] == 0)

            # creating features (dict)
            features = create_features_from_chip(
                dswe,
                "water",
                temporal_aggregation=temporal_aggregation,
                spatial_agg_func=spatial_agg_func,
                temporal_agg_func=temporal_agg_func,
                check_dims=False,
                return_as_dict=True,
                # copy necessary since code pops elements from list
                perc_total_kwargs=perc_total_kwargs.copy(),
            )

            # adding new row
            if isinstance(gbifid, str):
                features["gbifid"] = gbifid
            else:
                features["gbifid"] = int(gbifid)
            row = pd.Series(features)

            feature_rows.append(row)

        except Exception as ex:
            azure_logger.exception(ex)
            azure_logger.log(
                "Exception in dask_water_features_wrapper_onthefly, skipping row",
                **{"gbifid": {gbifid}},
            )

    df_features = pd.DataFrame(feature_rows)

    if len(feature_rows) > 0:
        if df_features["gbifid"].dtypes == "float64":
            df_features["gbifid"] = df_features["gbifid"].astype(np.int64)
        df_features = df_features.set_index("gbifid")

    return df_features


# GNATSGO


def dask_gnatsgo_features_wrapper_onthefly(  # noqa: C901
    gbif_items,
    xarr,
    buffer_size,
    spatial_agg_func="mean",
):
    """
    Wrapper for creating features from Landsat-8 multispectral data to use
    in parallelized (distributed/delayed) Dask execution.
    Extracts chip for each entry in dataframe, masks clouds, converts data to
    reflectance values and calculates features.
    NOTE: aggregation settings apply to all layers except 'mukey'.

    Parameters
    ----------
    gbif_items : pandas.DataFrame
        Contains the information about the GBIF datasets/locations.
        Subset of the larger GBIF dataframe.
    xarr: xarray.DataArray
        Contains data from which chips are produced (output of STACDataLoader.load_data()).
    buffer : float
        Buffer size in meters (UTM). Used to create buffer bounding box to
        extract chips.
    spatial_agg_func : str; default='mean'
        Defines the type of aggregation to apply in the spatial dimension.
        Options: mean, median, std, sum, min, max, iqr.
        Categorical options: perc_total. perc_total_kwargs parameter required.

    Returns
    -------
    pd.DataFrame
        Contains all aggregated features for each GBIF point in input.
    """

    xarr = xarr.compute()

    feature_rows = []
    for i, gbif_item in gbif_items.iterrows():
        gbifid = gbif_item["gbifid"]

        # create bbox
        bounds = create_chip_bbox(
            (gbif_item["decimallongitude"], gbif_item["decimallatitude"]),
            buffer_size,
            return_wgs84=False,
        )

        # indexing xarray
        if np.min(xarr.data.shape) > 0:
            xarr_ = xarr.sel(x=slice(bounds[0], bounds[2]), y=slice(bounds[3], bounds[1]))
        else:
            raise ValueError(f"Cannot subset xarray of shape {xarr.data.shape}.")

        # clean nodata vals in chip
        xarr_ = xarr_.where(xarr_.sel(band="soc0_100") > -9000)

        # creating features (dict)

        # MUKEY
        features_mukey = create_features_from_chip(
            xarr_.sel(band=["mukey"]),
            name="soil",
            temporal_aggregation="total",
            spatial_agg_func="mode",
            check_dims=False,
            return_as_dict=True,
        )

        # OTHER FEATURES
        features = create_features_from_chip(
            xarr_.sel(
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
            "soil",
            temporal_aggregation="total",
            spatial_agg_func=spatial_agg_func,
            check_dims=False,
            return_as_dict=True,
        )

        features = {**features, **features_mukey}

        # adding new row
        if isinstance(gbifid, str):
            features["gbifid"] = gbifid
        else:
            features["gbifid"] = int(gbifid)
        row = pd.Series(features)

        feature_rows.append(row)

    df_features = pd.DataFrame(feature_rows)
    if df_features["gbifid"].dtypes == "float64":
        df_features["gbifid"] = df_features["gbifid"].astype(np.int64)
    df_features = df_features.set_index("gbifid")

    return df_features


# TERRACLIMATE FEATURES


def dask_terraclimate_features_wrapper_onthefly(  # noqa: C901
    gbif_items,
    xarr,
    temporal_aggregation="seasonal",
    spatial_agg_func="mean",
    temporal_agg_func="mean",
):
    """
    Wrapper for creating features from TerraClimate raster data to use
    in parallelized (distributed/delayed) Dask execution.
    Extracts nearest pixel for each entry in dataframe and calculates features.

    Parameters
    ----------
    gbif_items : pandas.DataFrame
        Contains the information about the GBIF datasets/locations.
        Subset of the larger GBIF dataframe.
    xarr: xarray.DataArray
        Contains data from which chips are produced (output of STACDataLoader.load_data()).
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

    Returns
    -------
    pd.DataFrame
        Contains all aggregated features for each GBIF point in input.
    """

    xarr_ = xarr.compute()

    feature_rows = []
    for i, gbif_item in gbif_items.iterrows():
        gbifid = gbif_item["gbifid"]

        # indexing xarray
        if np.min(xarr.data.shape) > 0:
            xarr_ = xarr_.sel(
                x=[gbif_item["decimallongitude"]],
                y=[gbif_item["decimallatitude"]],
                method="nearest",
            )
        else:
            raise ValueError(f"Cannot subset xarray of shape {xarr.data.shape}.")

        # creating features (dict)
        features = create_features_from_chip(
            xarr_,
            "climate",
            temporal_aggregation=temporal_aggregation,
            spatial_agg_func=spatial_agg_func,
            temporal_agg_func=temporal_agg_func,
            check_dims=False,
            return_as_dict=True,
        )

        # adding new row
        if isinstance(gbifid, str):
            features["gbifid"] = gbifid
        else:
            features["gbifid"] = int(gbifid)
        row = pd.Series(features)

        feature_rows.append(row)

    df_features = pd.DataFrame(feature_rows)
    if df_features["gbifid"].dtypes == "float64":
        df_features["gbifid"] = df_features["gbifid"].astype(np.int64)
    df_features = df_features.set_index("gbifid")

    return df_features
