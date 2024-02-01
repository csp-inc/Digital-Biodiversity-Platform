# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to support data processing of spectral rasters to produce dynamic
surface water extent.

Dynamic surface water extent (DSWE) shows detected water in areas with
vegetation over the water.
The algorithm to produce DSWE operate over Landsat products at 30m resolution.

Inputs:
- Landsat level 2 pixel QA band
- Landsat surface reflectance

Outputs:
- Interpreted band with each pixel assigned to a water category
- Diagnostic band with water detection test outputs

References:
- https://www.usgs.gov/media/files/landsat-dynamic-surface-water-extent-add
- https://github.com/bendv/eedswe
"""


import numpy as np
import xarray as xr

CLOUD_THRESHOLD_DEFAULT = 0.05
SPATIAL_AGGR_DEFAULTS = {
    "num_bins": 3,
    "category_dict": {0: 0, 1: 1, 2: 1, 3: 2, 4: 2},
    "bin_names": "nowater water wetland".split(),
    "threshold": CLOUD_THRESHOLD_DEFAULT,
}


# band lookup names for landsat collection 2 level 2 (newly added to Planetary Computer)
BAND_LOOKUP = {
    "blue": "blue",
    "green": "green",
    "red": "red",
    "nir": "nir08",
    "swir1": "swir16",
    "swir2": "swir22",
}

# diagnostic test code mapping
# DSWE water presence values come from:
# https://www.usgs.gov/media/files/landsat-collection-2-level-3-dynamic-surface-water-extent-algorithm-description
#  0 -> Not Water
#  1 -> Water - High Confidence
#  2 -> Water - Moderate Confidence
#  3 -> Potential Wetland
#  4 -> Low Confidence Water or Wetland
DT_CODING = {
    "00000": 0,
    "00001": 0,
    "00010": 0,
    "00100": 0,
    "01000": 0,  # No water
    "01111": 1,
    "10111": 1,
    "11011": 1,
    "11101": 1,
    "11110": 1,
    "11111": 1,  # (Water - High Confidence)
    "00111": 2,
    "01011": 2,
    "01101": 2,
    "01110": 2,
    "10011": 2,
    "10101": 2,
    "10110": 2,
    "11001": 2,
    "11010": 2,
    "11100": 2,  # (Water - Moderate Confidence)
    "11000": 3,  # (Potential Wetland)
    "00011": 4,
    "00101": 4,
    "00110": 4,
    "01001": 4,
    "01010": 4,
    "01100": 4,
    "10000": 4,
    "10001": 4,
    "10010": 4,
    "10100": 4,  # (Low Confidence Water or Wetland)
}

# Diagnostic test parameter defaults
# Can be modified as an input to `derive_surface_water_extent()`
DT_PARAMS = {
    # Test 1, 2, 3
    "wigt": 124,  # Wetness index threshold, default 0.124
    # (index values are multipled by 1e5)
    "awgt": 0,  # Automated water extent shadow threshold
    # Test 4
    "pswt_1_mndwi": -4400,  # Partial surface water test-1 MNDWI threshold, default -4400
    "pswt_1_swir1": 9000,  # Partial surface water test-1 SWIR1 threshold, default 9000
    "pswt_1_nir": 15000,  # Partial surface water test-1 NIR threshold, default 15000
    "pswt_1_ndvi": 0.7 * 10000,  # Partial surface water test-1 NDVI threshold
    # Test 5
    "pswt_2_mndwi": -5000,  # Partial surface water test-3 MNDWI threshold
    "pswt_2_blue": 1000,  # Partial surface water test-3 Blue threshold
    "pswt_2_nir": 2500,  # Partial surface water test-3 NIR threshold, default 2500
    "pswt_2_swir1": 3000,  # Partial surface water test-3 SWIR1 threshold
    "pswt_2_swir2": 1000,  # Partial surface water test-3 SWIR2 threshold
}


def derive_surface_water_extent(
    full_raster_xr,
    dt_params=DT_PARAMS,
) -> xr.DataArray:
    """
    Derives DSWE categorical values by running diagnostic tests
    on the landsat raster.
    Parameters
    ----------
    full_raster_xr : xr.DataArray
        Landsat timeseries raster with coords: (time, band, y, x)
    dt_params : Dict
        Dictionary of diagnostic test parameters.

    Returns
    -------
    xr.DataArray
        Timeseries xarray of np.uint8s with the following categorical encoding:
        0 -> Not Water
        1 -> Water - High Confidence
        2 -> Water - Moderate Confidence
        3 -> Potential Wetland
        4 -> Low Confidence Water or Wetland
    """
    # Input xarray quality check Is 4D and contains expected bands.
    if len(full_raster_xr.shape) != 4:
        raise ValueError(
            "Expected input argument `full_raster_xr` to be a 4D array (time, band, y, x)"
        )

    if not all([coord in full_raster_xr.coords.keys() for coord in "time band".split()]):
        raise ValueError(
            "Expected input argument `full_raster_xr` to contain 'time' and 'band'"
            " coordinates."
        )

    # Derive dswe
    indicies_xr = calculate_indicies(full_raster_xr)
    tests_xr = dswe_tests(indicies_xr, dt_params)
    dswe = interpret_dswe_from_tests(tests_xr)

    # Cleanup the resulting xarray.Dataset
    dswe["time"] = full_raster_xr["time"]
    dswe["y"] = full_raster_xr["y"]
    dswe["x"] = full_raster_xr["x"]
    dswe.attrs = full_raster_xr.attrs

    return dswe


def calculate_indicies(full_raster_xr: xr.DataArray) -> xr.DataArray:
    """
    Computes indicies using unscaled Landsat surface refelctance bands
    to be used for DSWE diagnostic testing.

    Parameters
    ----------
    full_raster_xr : xr.DataArray
        Landsat raster

    Returns
    -------
    xr.DataArray
        xarray containing 9 index bands

    """
    mndwi = calculate_normalized_index(full_raster_xr, "MNDWI")
    mbsr = calculate_mbsr(full_raster_xr)
    ndvi = calculate_normalized_index(full_raster_xr, "NDVI")
    awesh = calculate_awesh(full_raster_xr)
    blue, nir, swir1, swir2 = [
        full_raster_xr.sel(band=BAND_LOOKUP[band_str])
        for band_str in "blue nir swir1 swir2".split()
    ]

    bands = [mndwi, mbsr, ndvi, awesh, blue, nir, swir1, swir2]

    indicies_xr = xr.DataArray(bands, dims="band time y x".split())
    indicies_xr["band"] = "mndwi mbsr ndvi awesh blue nir swir1 swir2".split()

    return indicies_xr


def calculate_normalized_index(
    full_raster_xr: xr.DataArray, index_str: str
) -> xr.DataArray:
    """
    Calculate one of two normalized indicies:  MNDWI or NDVI.


    # Modified Normalized Difference Water Index

    The Modified Normalized Difference Water Index (MNDWI) uses green and SWIR bands for
    the enhancement of open water features. It also diminishes built-up area features
    that are often correlated with open water in other indices.

        MNDWI = (Green - SWIR) / (Green + SWIR)

    Green = pixel values from the green band
    SWIR = pixel values from the short-wave infrared band

    https://www.space4water.org/taxonomy/term/1246

    ---
    # Normalized Different Vegetation Index (NDVI)

    The normalized difference vegetation index (NDVI) is a standardized index
    allowing you to generate an image displaying greenness, also known as relative
    biomass. This index takes advantage of the contrast of characteristics between
    two bands from a multispectral raster dataset—the chlorophyll pigment absorption
    in the red band and the high reflectivity of plant material in the near-infrared
    (NIR) band.

        NDVI = (NIR - Red) / (NIR + Red)

    https://www.space4water.org/taxonomy/term/1239


    Parameters
    ----------
    full_raster_xr : xr.DataArray
        Landsat raster

    index_str: str
        Specifies which index to calclate between MNDWI or NDVI

    Returns
    -------
    xr.DataArray
        xarray with single band representing MNDWI

    """

    if index_str.lower() == "mndwi":
        b1 = full_raster_xr.sel(band=BAND_LOOKUP["green"])
        b2 = full_raster_xr.sel(band=BAND_LOOKUP["swir1"])

    elif index_str.lower() == "ndvi":
        b1 = full_raster_xr.sel(band=BAND_LOOKUP["nir"])
        b2 = full_raster_xr.sel(band=BAND_LOOKUP["red"])

    else:
        raise ValueError("Expected 'MNDWI' or 'NDVI' as index_str argument.")

    norm_indx = (((b1 - b2) / (b1 + b2)) * 10000).rename(index_str.upper())

    return norm_indx


def calculate_mbsr(full_raster_xr: xr.DataArray) -> xr.DataArray:
    """
    Multi-band Spectral Relationship

        MBSR = (green + red) - (nir + swir)


    Parameters
    ----------
    full_raster_xr : xr.DataArray
        Landsat raster that includes green, red, nir, and swir bands

    Returns
    -------
    xr.DataArray
        xarray with single band representing MBSR

    """
    green, red, nir, swir = [
        full_raster_xr.sel(band=BAND_LOOKUP[band_str])
        for band_str in "green red nir swir1".split()
    ]

    mbsr = (green + red) - (nir + swir)

    return mbsr.rename("MBSR")


def calculate_awesh(full_raster_xr: xr.DataArray) -> xr.DataArray:
    """
    Automated Water Extent Shadow (AWESH)

        AWESH = blue + (2.5 * green) – (1.5 * MBSRN) – (0.25 * SWIR2)

    Parameters
    ----------
    full_raster_xr : xr.DataArray
        Landsat raster that includes NIR and Red bands

    Returns
    -------
    xr.DataArray
        xarray with single band representing AWESH
    """
    blue, green, nir, swir1, swir2 = [
        full_raster_xr.sel(band=BAND_LOOKUP[band_str])
        for band_str in "blue green nir swir1 swir2".split()
    ]

    awesh = blue + (2.5 * green) - (1.5 * (nir + swir1)) - (0.25 * swir2)

    return awesh.rename("AWESH")


def dswe_tests(indicies_xr: xr.DataArray, dt_params: dict) -> xr.DataArray:
    """
    Conducts DSWE tests using preprocessed index values. Returns a diagnostic test
    band

    Parameters
    ----------
    indicies_xr : xr.DataArray
        Xarray output from .calculate_indicies() function.

    dt_params : Dict
        Dictionary of input parameters to diagnostic tests.

    Returns
    -------
    xr.DataArray
        xarray with single band representing diagnostic test results
        represented as binary string of length 5. Each position of the string
        represents the result of one of the 5 diagnostic tests.
    """

    t1 = xr.where(indicies_xr.sel(band="mndwi") > dt_params["wigt"], 1, 0)
    t2 = xr.where(indicies_xr.sel(band="mbsr") > 0, 1, 0)
    t3 = xr.where(indicies_xr.sel(band="awesh") > dt_params["awgt"], 1, 0)

    t4_ = (
        xr.where(indicies_xr.sel(band="mndwi") > dt_params["pswt_1_mndwi"], 1, 0)
        + xr.where(indicies_xr.sel(band="swir1") < dt_params["pswt_1_swir1"], 1, 0)
        + xr.where(indicies_xr.sel(band="nir") < dt_params["pswt_1_nir"], 1, 0)
        + xr.where(indicies_xr.sel(band="ndvi") < dt_params["pswt_1_ndvi"], 1, 0)
    )
    t4 = xr.where(t4_ == 4, 1, 0)

    t5_ = (
        xr.where(indicies_xr.sel(band="mndwi") > dt_params["pswt_2_mndwi"], 1, 0)
        + xr.where(indicies_xr.sel(band="swir2") < dt_params["pswt_2_swir2"], 1, 0)
        + xr.where(indicies_xr.sel(band="nir") < dt_params["pswt_2_nir"], 1, 0)
        + xr.where(indicies_xr.sel(band="blue") < dt_params["pswt_2_blue"], 1, 0)
        + xr.where(indicies_xr.sel(band="swir1") < dt_params["pswt_2_swir1"], 1, 0)
    )
    t5 = xr.where(t5_ == 5, 1, 0)

    result = t1 + 10 * t2 + 100 * t3 + 1000 * t4 + 10000 * t5
    result["band"] = "test"
    return result


def interpret_dswe_from_tests(tests_xr: xr.DataArray) -> xr.DataArray:
    """
    Interprets test results from dswe_tests into categorical surface water extent data.
    Note, this function does not fill in novals for cloud cover or poor pixel quality.

    Parameters
    ----------
    tests_xr : xr.DataArray
        Xarray output from .dswe_tests() function.

    Returns
    -------
    xr.DataArray
        xarray with single band representing interpreted DSWE as intergers mapping to
        the following categories:
            - 0 -> Not Water
            - 1 -> Water - High Confidence
            - 2 -> Water - Moderate Confidence
            - 3 -> Potential Wetland
            - 4 -> Low Confidence Water or Wetland
    """

    def dict_lookup_helper(test_code_int: int) -> int:
        """Helper function for DT_CODING lookup.  Returns integer dswe encoding"""
        test_code_str = "{:05}".format(test_code_int)
        return DT_CODING[test_code_str]

    df = tests_xr.to_dataframe(name="test_val")
    return (
        df["test_val"].apply(dict_lookup_helper).to_xarray().astype(np.uint8).rename("dswe")
    )
