# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from typing import Optional

import numpy as np
import xarray


def create_cloud_mask_from_qa_band(
    qa_band: xarray.DataArray,
    sensor: str = "l8",
    cloud_confidence: str = "high",
    cirrus_confidence: Optional[str] = None,
):  # noqa: C901
    """
    Creates a cloud mask from Landsat Level-1 QA band.

    Parameters
    ----------
    qa_band : np.ndarray
        The Landsat-8 QA band (integer values).
    sensor : str; default='l8'
        The sensor type. Options: 'l4', 'l5', 'l7', 'l8'.
    cloud_confidence : str; default='high'
        The cloud confidence level from which to consider a pixel as cloudy.
        Options: low, medium, high.
    cirrus_confidence : str; default=None
        The cirrus confidence level from which to consider a pixel as cloudy.
        If None, cirrus clouds are ignored.
        Options: low, medium, high.
        Only supported for Landsat-8 data.

    Returns
    -------
    np.ndarray
        Binary mask with clouds=1, cloud-free=0.
    """

    # specify the cloud confidence bit settings to consider
    consider_cloud_conf_bits = []
    if cloud_confidence == "low":
        consider_cloud_conf_bits = ["01", "10", "11"]
    elif cloud_confidence == "medium":
        consider_cloud_conf_bits = ["10", "11"]
    elif cloud_confidence == "high":
        consider_cloud_conf_bits = ["11"]

    # specify the cirrus confidence bit settings to consider
    consider_cirrus_conf_bits = []
    if sensor == "l8":
        if cirrus_confidence == "low":
            consider_cirrus_conf_bits = ["01", "10", "11"]
        elif cirrus_confidence == "medium":
            consider_cirrus_conf_bits = ["10", "11"]
        elif cirrus_confidence == "high":
            consider_cirrus_conf_bits = ["11"]
        elif cirrus_confidence is None:
            consider_cirrus_conf_bits = []
    else:
        logging.info("Cirrus confidence only available for Landsat-8. Will be ignored.")

    # create cloud mask
    unique_values = np.unique(np.array(qa_band))
    cmask = np.zeros_like(qa_band)
    for qa_value in unique_values:
        qa_binary = f"{int(qa_value):016b}"[::-1]
        cloud_conf_bits = qa_binary[8:10]
        cirrus_conf_bits = qa_binary[14:16]

        if cloud_conf_bits in consider_cloud_conf_bits:
            cmask = np.where(qa_band == qa_value, 1, cmask)
        if cirrus_conf_bits in consider_cirrus_conf_bits:
            cmask = np.where(qa_band == qa_value, 1, cmask)

    return cmask


def create_cloud_mask_from_scl_band(
    scl, cloud_confidence="high", cirrus: bool = False
) -> np.ndarray:
    """
    Creates a cloud mask from Sentinel-2 SCL band.

    Parameters
    ----------
    scl : np.ndarray
        The Sentinel-2 SCL band.
    cloud_confidence : str; default='high'
        The cloud confidence level from which to consider a pixel as cloudy.
        Options: medium, high.
    cirrus : bool; default=False
        Defines if cirrus clouds are masked out as well.

    Returns
    -------
    np.ndarray
        Binary mask with clouds=1, cloud-free=0.
    """

    cmask = np.zeros_like(scl)

    if cloud_confidence == "medium":
        cmask = np.where((scl == 8) | (scl == 9), 1, cmask)
    elif cloud_confidence == "high":
        cmask = np.where((scl == 9), 1, cmask)

    if cirrus:
        cmask = np.where((scl == 10), 1, cmask)

    return cmask


def create_cloud_mask(
    data: xarray.DataArray,
    sensor: str = "l8",
    cloud_confidence: str = "high",
    cirrus_confidence: Optional[str] = None,
) -> np.ndarray:  # noqa: C901
    """
    Creates a cloud mask for an entire chip single scene based on the local
    cloud mask layer.
    NOTE 1: behavior currently only validated for L8 and S2.

    Parameters
    ----------
    data : xarray.DataArray
        The scene for which cloud cover percentage is calculated.
    sensor : str; default='l8'
        The sensor type. Options: 'l4', 'l5', 'l7', 'l8', 's2'.
    cloud_confidence : str; default='high'
        The cloud confidence level from which to consider a pixel as cloudy.
        Options: low, medium, high.
    cirrus_confidence : str; default=None
        The cirrus confidence level from which to consider a pixel as cloudy.
        If None, cirrus clouds are ignored.
        Options: low, medium, high.
        Only supported for Landsat-8 and Sentinel-2 data. For Sentinel-2, no
        confidence levels are available, all values other than None will include
        cirrus clouds in the mask.

    Returns
    -------
    np.ndarray
        Cloud mask (clouds=1, no clouds=0).
    """

    bands_in_ds = [b.lower() for b in data.indexes["band"]]
    if sensor in ["l4", "l5", "l7", "l8"]:
        if "qa_pixel" in bands_in_ds:
            try:
                temp_mask = data.sel(band="QA_PIXEL").astype(int)
            except KeyError:
                temp_mask = data.sel(band="qa_pixel").astype(int)
        elif "bqa" in bands_in_ds:
            try:
                temp_mask = data.sel(band="BQA").astype(int)
            except KeyError:
                temp_mask = data.sel(band="bqa").astype(int)
        else:
            temp_mask = data.sel(band="cloud_mask").astype(int)

        cmask = create_cloud_mask_from_qa_band(
            temp_mask,
            sensor=sensor,
            cloud_confidence=cloud_confidence,
            cirrus_confidence=cirrus_confidence,
        )
    elif sensor == "s2":
        if "scl" in bands_in_ds:
            try:
                temp_mask = data.sel(band="SCL").astype(int)
            except KeyError:
                temp_mask = data.sel(band="scl").astype(int)
        else:
            temp_mask = data.sel(band="cloud_mask").astype(int)

        use_cirrus_confidence = False
        if cirrus_confidence is not None:
            use_cirrus_confidence = True
        else:
            use_cirrus_confidence = False
        cmask = create_cloud_mask_from_scl_band(
            temp_mask, cloud_confidence=cloud_confidence, cirrus=use_cirrus_confidence
        )

    return cmask  # pyright: reportUnboundVariable=false
