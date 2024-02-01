"""
Utilities for segmenting shp file into spatiotemporal segments
to use for inference engine
"""
import itertools
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from pyproj import Transformer


def _spatial_segmentation(
    bbox: Tuple[float, float, float, float],
    stride_x: float,
    stride_y: float,
    add_spatial_buffer: bool = False,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Identify equi-distant points (based on respective x and y strides)
    within bounding box region

    Parameters
    ----------
    bbox: Tuple[float]
        Bounding box in which equidistant points will be identified.
        Bbox is assumed to be in (meters). Must be of the form (minx, miny, maxx, maxy)

    stride_x: float
        Stride along x dimension in meters

    stride_y: float
        Stride along y dimension in meters

    add_spatial_buffer: bool
        Adds stride size buffer to x and y dimensions

    Returns
    -------
    Tuple[np.array]
        Two np.arrays representing x ticks and y ticks according to stride segmentation
    """
    minx, miny, maxx, maxy = bbox

    if add_spatial_buffer:
        minx -= stride_x / 2
        miny -= stride_y / 2
        maxx += stride_x / 2
        maxy += stride_y / 2

    # start: end: increments
    x_ = np.arange(minx, maxx, stride_x)
    y_ = np.arange(miny, maxy, stride_y)

    return x_, y_


def get_spatiotemporal_segments(
    bbox_utm: Tuple[float, float, float, float],
    stride_x: float,
    stride_y: float,
    start_time: str,
    end_time: str,
    temporal_stride: str = "y",
    include_id_col: bool = True,
    add_spatial_buffer: bool = True,
    add_wgs84_coords: bool = False,
    utm_epsg: Optional[int] = None,
    id_col_prefix: str = "",
) -> pd.DataFrame:
    """
    Build spatiotemporal segments from an input bounding box (UTM) and temporal range.
    Returns pandas dataframe of all segments.

    Parameters
    ----------
    bbox_utm: Tuple[float]
        Bounding box in which equidistant points will be identified.
        Bbox is in UTM (meters).
        Must be of the form (minx, miny, maxx, maxy)

    stride_x: float
        Stride along x dimension in meters

    stride_y: float
        Stride along y dimension in meters

    start_time: str
        Start time of temporal range of interest,  string input to pd.date_range()

    start_time :str
        End time of temporal range of interest, string input to pd.date_range()

    temporal_stride: str
        Temporal range of each datetime tuple. This window is applied backwards
        from the target year

    include_id_col: bool
        If True, includes a string column of IDs derived from index column.

    add_spatial_buffer: bool
        Adds stride size buffer to x and y dimensions

    add_wgs84_coords: bool
        Adds latlon coords to output dataframe

    utm_epsg: int
        EPSG code for bbox projection.  Required for wgs84 coords

    id_col_prefix: str
        Prefix to append to ID col if include_id_col is True

    Returns
    -------
    pd.DataFrame
        Dataframe with cols: x, y, time, lat, lon, id
        x, y coords in UTM projection.  lat, lon are wgs84

    """

    x_, y_ = _spatial_segmentation(bbox_utm, stride_x, stride_y, add_spatial_buffer)
    t_ = pd.date_range(start_time, end_time, freq=temporal_stride)

    spatiotemporal_segments_itr = itertools.product(x_, y_, t_)

    df = pd.DataFrame.from_records(
        list(spatiotemporal_segments_itr), columns="x y time".split()
    )

    if add_wgs84_coords:
        # add in latlon_wgs84 columns
        transformer = Transformer.from_crs(f"EPSG:{utm_epsg}", "EPSG:4326")
        latlon = df[["x", "y"]].apply(lambda x: transformer.transform(x[0], x[1]), axis=1)
        df["decimallatitude"] = latlon.apply(lambda x: x[0])
        df["decimallongitude"] = latlon.apply(lambda x: x[1])

    if include_id_col:
        digits = len(str(len(df))) + 1
        df["id"] = pd.Series(df.index.map(lambda x: f"{id_col_prefix}{x:0{digits}.0f}"))

    return df
