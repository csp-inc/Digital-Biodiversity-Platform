# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Tuple

from pyproj import Transformer


def convert_bbox_to_crs(
    bbox: Tuple[float, float, float, float],
    current_crs: str,
    new_crs: str,
    always_xy: bool = True,
) -> Tuple[float, float, float, float]:
    """
    Converts a bounding box to a new CRS.

    Parameters
    ----------
    bbox: Tuple[float]
        Bounding box of form (minx, miny, maxx, maxy).
    current_crs: str
        String representation of bbox's CRS.  e.g. "EPSG:4326".
    new_crs: str
        String representation of output bbox's CRS.
    always_xy: bool
        Parameter passed into pyproj.Transformer.  Set
        to True to return coordinates in standard GIS order.

    Returns
    -------
    Tuple[float]
        Output bounding box in new_crs projection.
    """
    t = Transformer.from_proj(current_crs, new_crs, always_xy=always_xy)
    minx, miny, maxx, maxy = bbox
    minx, miny = t.transform(minx, miny)
    maxx, maxy = t.transform(maxx, maxy)
    return minx, miny, maxx, maxy
