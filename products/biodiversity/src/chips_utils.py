# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
import time
from typing import Tuple

import numpy as np
import utm

# import xarray as xr
from pyproj import CRS, Transformer  # pyright: reportPrivateImportUsage=false
from shapely.geometry import shape


def create_chip_bbox(
    point: Tuple[float, float], buffer: float, return_wgs84: bool = True
) -> Tuple[float, float, float, float]:
    """
    Creates a rectangular box around a point.

    Parameters
    ----------
    point : Tuple[float, float]
        Coordinates of chip center point (lon, lat).
    buffer : float
        Buffer size in meters (UTM).
    return_wgs84 : bool; default=True
        If True, converts UTM coordinates to WGS84.

    Returns
    -------
    Tuple[float]
        Contains the buffered chip box of format
        (xmin, ymin, xmax, ymax).
    """
    lon, lat = point
    x, y, zone, _ = utm.from_latlon(lat, lon)

    _ymin, _ymax = y - buffer, y + buffer
    _xmin, _xmax = x - buffer, x + buffer

    if return_wgs84:
        utm_epsg = CRS.from_dict({"proj": "utm", "zone": zone}).to_epsg()
        transformer = Transformer.from_crs(f"EPSG:{utm_epsg}", "EPSG:4326", always_xy=True)
        left, bottom = transformer.transform(_xmin, _ymin)
        right, top = transformer.transform(_xmax, _ymax)
        return (left, bottom, right, top)
    else:
        return (_xmin, _ymin, _xmax, _ymax)


def create_bbox_from_gbif_points(
    df, as_polygon: bool = True
) -> Tuple[float, float, float, float]:
    """
    Creates a bounding box based on points in a (subset of)
    the GBIF dataframe.

    Parameters
    ----------
    df : pandas DataFrame
        Contains the GBIF data points.
    as_polygon : bool; default=True
        If True, returns a shapely.geometry.Polygon, otherwise
        a tuple of coordinates.

    Returns
    -------
    as_polygon=True:
        shapely.geomtery.Polygon
            Represents the bounding box as a geometry.
    as_polygon=False:
        tuple of float
            Represents the bounding box as a set of coordinates
            (xmin, ymin, xmax, ymax).
    """

    lons = df["decimallongitude"]
    lats = df["decimallatitude"]
    minlon = np.min(lons)
    maxlon = np.max(lons)
    minlat = np.min(lats)
    maxlat = np.max(lats)

    if as_polygon:
        bbox = [
            (
                (minlon, maxlat),
                (maxlon, maxlat),
                (maxlon, minlat),
                (minlon, minlat),
                (minlon, maxlat),
            )
        ]
        bbox = shape({"type": "Polygon", "coordinates": bbox})
    else:
        bbox = (minlon, minlat, maxlon, maxlat)
    return bbox


# def xarr_to_zarr(xarr: xr.DataArray, file_name: str, mode: str = "w") -> None:
#     """
#     Saves xarray in zarr format

#     Parameters
#     ----------
#     xarr : xr.DataArray
#         Xarray to be saved in zarr format
#     file_name : str
#         Name of the file to write zarr to
#     mode : str, optional
#         Persistence mode: “w” means create (overwrite if exists);
#         “w-” means create (fail if exists);
#         “a” means override existing variables (create if does not exist);
#         “r+” means modify existing array values only, by default "w"
#     """
#     if "spec" in xarr.attrs:
#         xarr.attrs["spec"] = str(xarr.attrs["spec"])
#     for k, v in xarr.coords.items():
#         if k.startswith("proj:"):
#             xarr.coords[k] = str(v)
#     xarr = xarr.rename({k: k.replace(":", "_") for k in xarr.coords})
#     ds = xarr.to_dataset(name="data", promote_attrs=True)
#     ds.to_zarr(file_name, mode=mode)


def reflectance_xarr_to_zarr(xarr, path, mode="w") -> None:
    """
    Saves a reflectance chip xarray.DataArray to zarr.

    Parameters
    ----------
    xarr : xarray.DataArray
        The dataset to be saved.
    path : str
        Output path to which the data is saved.
    mode : str; default='w'
        Persistence mode: 'w' means create (overwrite if exists);
        'w-' means create (fail if exists);
        'a'means override existing variables (create if does not exist);
        'r+' means modify existing array values only.

    Returns
    -------
    None
    """

    new_name_dict = {}

    xarr.attrs["spec"] = str(xarr.attrs["spec"])
    for k, v in xarr.coords.items():
        try:
            xarr.coords[k] = str(v)
        except ValueError:
            pass

        if ":" in k:
            new_name_dict[k] = k.replace(":", "_")
        else:
            new_name_dict[k] = k

    xarr = xarr.rename(new_name_dict)
    ds = xarr.to_dataset(name="data", promote_attrs=True)
    for k, v in ds.attrs.items():
        ds.attrs[k] = str(v)

    # saving to zarr
    ds.to_zarr(path, mode=mode)
    ds = None


def subset_xarr_by_xy_coords(xarr, bbox, x_dim_name="lon", y_dim_name="lat", margin=0):
    """
    Subsets an xarray by x/y coordinates (e.g. for Planetary Computer
    zarr files that are loaded in their entirety).

    Parameters
    ----------
    xarr : xr.Dataset or xr.DataArray
        Contains the data that is to be subset.
    bbox : iterable of int/float
        The bounding coordinates to subset to in format
        (xmin, ymin, xmax, ymax). Must be in same format/system as the
        coordinates of xarr.
    x_dim_name : str; default='lon'
        The name of the dimension in the xarray that describes the
        x-dimension coordinates.
    y_dim_name : str; default='lat'
        The name of the dimension in the xarray that describes the
        y-dimension coordinates.
    margin : int; default=0
        Margin to leave around selected area, i.e. number of steps in
        x- and y-dimension to include around the actual bbox.
        Can help avoid producing empty xarrays ('margin of safety').

    Returns
    -------
    xr.Dataset or xr.DataArray
        Subset of the input xarr.
    """

    # obtain bounding coordinates and coordinates of target xarray
    xmin, ymin, xmax, ymax = bbox
    xs = np.array(xarr.coords[x_dim_name].values)
    ys = np.array(xarr.coords[y_dim_name].values)

    # obtain indices of nearest coordinates matching bounding box
    xmin_idx = np.argmin((xs - xmin) ** 2)
    xmax_idx = np.argmin((xs - xmax) ** 2)
    ymin_idx = np.argmin((ys - ymin) ** 2)
    ymax_idx = np.argmin((ys - ymax) ** 2)

    # arrange slices for subsetting:
    # 1) min/max operations needed because some zarrs on Planetary Computer
    # sort lon cordinates from 90 to -90;
    # 2) make sure at least one element is selected;
    # 3) optionally add margin.

    # x dimension
    x_minmax = [
        min(xmin_idx, xmax_idx),
        max(xmin_idx, xmax_idx),
    ]  # pyright: reportGeneralTypeIssues=false
    if abs(x_minmax[0] - x_minmax[1]) < 5:
        x_minmax[0] -= 1
        x_minmax[1] += 1
    if margin > 0:
        x_minmax[0] -= margin
        x_minmax[1] += margin
    x_slice = slice(*x_minmax)

    # y dimension
    y_minmax = [
        min(ymin_idx, ymax_idx),
        max(ymin_idx, ymax_idx),
    ]  # pyright: reportGeneralTypeIssues=false
    if abs(y_minmax[0] - y_minmax[1]) < 5:
        y_minmax[0] -= 1
        y_minmax[1] += 1
    # apply margin
    if margin > 0:
        y_minmax[0] -= margin
        y_minmax[1] += margin
    y_slice = slice(*y_minmax)

    # subset xarray
    xarr_ = xarr.isel({x_dim_name: x_slice, y_dim_name: y_slice})

    return xarr_


def dask_upload_files(  # noqa: C901
    worker_id,
    chips_output_dir,
    storage_target_dir,
    azcopy,
    max_retries=5,
    az_concurrent=0,
    az_membuffer=0,
    az_maxspeed=0,
    az_timeout=None,
):
    """
    Uploads files to Azure Blob Storage via azcopy.

    Parameters
    ----------
    worker_id : int/str
        The ID of the current Dask worker (only used for logging purposes).
    azcopy : AzCopyWrapper instance
        Used for upload process.
    max_retries : int; default=5
        Maximum number of retries of azcopy in case any issues are encountered
        (see dask_upload_files).
        The upload process is triggered either until max_retries is reached or
        the process finishes with return code 0. This is a safety measure because
        azcopy tends to fail occasionally when many parallel processes are attempting
        to upload files to the same blob simultaneously.
    Rest see dask_reflectence_chips_wrapper.

    Returns
    -------
    None
    """

    logger = logging.getLogger("distributed.worker")

    # setting azcopy concurrency value
    if az_concurrent != "AUTO":
        if az_concurrent > 0:
            os.environ["AZCOPY_CONCURRENCY_VALUE"] = str(az_concurrent)
    if az_membuffer > 0:
        os.environ["AZCOPY_BUFFER_GB"] = str(az_membuffer)

    retry = 0
    # checking if target folder exists
    if os.path.exists(chips_output_dir):
        # attempting upload
        while retry < max_retries:
            try:
                if len(os.listdir(chips_output_dir)) > 0:
                    logger.info(f"[{worker_id}]: Uploading files via azcopy.")
                    result = azcopy.upload(
                        src_path=chips_output_dir,
                        dst_path=storage_target_dir,
                        recursive=True,
                        log_level="ERROR",
                        max_speed=az_maxspeed,
                        timeout=az_timeout,
                    )
                    if result == 0:
                        logger.info(
                            f"[{worker_id}]: Successfully uploaded files. Azcopy response:"
                            f" {result}"
                        )
                        break
                    elif result == -1:
                        logger.info(
                            f"[{worker_id}]: Upload not successful, probably timeout."
                            f" Azcopy response: {result}"
                        )
                        break
                    else:
                        logger.info(
                            f"[{worker_id}]: Upload not successful. Retrying. Azcopy"
                            f" response: {result}"
                        )
            except FileNotFoundError:
                logger.info(f"[{worker_id}]: No files remaining.")
                break
            except Exception as e:
                logger.error(
                    f"[{worker_id}]: Unknown error in azcopy. | {type(e).__name__}: {e}"
                )
                break
            retry += 1
            time.sleep(0.5)
    else:
        logger.info(f"[{worker_id}]: No chips found.")


def adaptive_grid_clustering(
    locs, grid_size=(0.1, 0.1), min_cell_size=0.001, max_cluster_size=1000
):  # noqa: C901
    """
    Clusters given points into an adaptive grid of varying size grid cells.

    Parameters
    ----------
    locs : list/tuple/array/pd.Series
        Contains the lon/lat coordinates of all points to be clustered.
        Can be any format that is convertible into an np.ndarray of (X, 2).
    grid_size : tuple of float; default=(0.1, 0.1)
        The initial grid lon/lat size to start the process from in WGS84 decimal degrees.
    min_cell_size : int; default=0.001
        Minimum allowed size of any grid cell (i.e. area of the cell, lon*lat).
    max_cluster_size : int; default=1000
        Maximum desired number of points per grid cell.

    Returns
    -------
    dict
        Contains the number of points in grid cells (keys: grid cell names, values: counts).
    dict
        Contains the coordinates of grid cells (keys: grid cell names,
        values: (xmin, ymin, xmax, ymax)).

    Notes
    -----
    The process works in the following way:
    1) The points are overlayed with a rectangular grid of grid_size.
    2) The number of points in each grid cell is determined, empty ones are discarded.
    3) Clusters are iteratively broken up into 2x2 subcells until either all clusters
    are <= max_cluster_size or no cells can be further broken up without violating
    min_cell_size requirement or the mean size of all clusters exceeding max_cluster_size
    is not decreasing anymore.
    """

    # setting up the lon/lat coordinates of initial grid
    locs = np.array(locs)
    lon_min, lat_min = np.min(locs, axis=0)
    lon_max, lat_max = np.max(locs, axis=0)

    lon_grid = np.arange(lon_min - 1e-4, lon_max + grid_size[0], grid_size[0])
    lat_grid = np.arange(lat_min - 1e-4, lat_max + grid_size[1], grid_size[1])

    # obtaining number of points and bounding box coordinates (xmin, ymin, xmax, ymax)
    # of each grid cell;
    # creating two dicts containing point counts and coordinates, respectively
    logging.info("Creating initial grid.")
    grid_counts = {}
    grid_coords = {}
    for i, lon in enumerate(lon_grid[:-1]):
        for j, lat in enumerate(lat_grid[:-1]):
            pts = np.sum(
                np.where(
                    (locs[:, 0] > lon_grid[i])
                    & (locs[:, 0] <= lon_grid[i + 1])
                    & (locs[:, 1] > lat_grid[j])
                    & (locs[:, 1] <= lat_grid[j + 1]),
                    1,
                    0,
                )
            )
            # ignoring empty grid cells
            if pts > 0:
                grid_counts[f"{i}_{j}"] = pts
                grid_coords[f"{i}_{j}"] = (
                    lon_grid[i],
                    lat_grid[j],
                    lon_grid[i + 1],
                    lat_grid[j + 1],
                )

    it = 1
    prev_mean_large_cluster_size = -1
    too_small = []
    # refine the grid as long as:
    # 1) the largest cluster contains more points than max_cluster_size,
    # 2) there are still cells remaining that can be broken up further without
    #    being smaller than min_cell_size,
    # 3) the mean size of of all clusters exceeding max_cluster_size is decreasing
    #    after each iteration.
    logging.info("Refining grid.")
    while (np.max(list(grid_counts.values())) > max_cluster_size) and (
        len(too_small) < len(grid_counts.keys())
    ):
        add_counts = {}
        add_coords = {}
        remove_keys = []
        for key, value in grid_counts.items():
            # only consider grid cells that were not previously determined to
            # be too small to be
            # further split up and contain more points than the allowed max_cluster_size
            if key not in too_small and value > max_cluster_size:
                cell_size = abs(grid_coords[key][2] - grid_coords[key][0]) * abs(
                    grid_coords[key][3] - grid_coords[key][1]
                )
                if cell_size > min_cell_size:
                    remove_keys.append(key)
                    # split grid cell into four subcells
                    orig_coords = grid_coords[key]
                    lon_grid = np.linspace(orig_coords[0], orig_coords[2], 3)
                    lat_grid = np.linspace(orig_coords[1], orig_coords[3], 3)
                    # determine new coords and point counts
                    for i, lon in enumerate(lon_grid[:-1]):
                        for j, lat in enumerate(lat_grid[:-1]):
                            pts = np.sum(
                                np.where(
                                    (locs[:, 0] > lon_grid[i])
                                    & (locs[:, 0] <= lon_grid[i + 1])
                                    & (locs[:, 1] > lat_grid[j])
                                    & (locs[:, 1] <= lat_grid[j + 1]),
                                    1,
                                    0,
                                )
                            )
                            if pts > 0:
                                add_counts[f"{key}__{i}{j}"] = pts
                                add_coords[f"{key}__{i}{j}"] = (
                                    lon_grid[i],
                                    lat_grid[j],
                                    lon_grid[i + 1],
                                    lat_grid[j + 1],
                                )
                else:
                    too_small.append(key)

        # merge the old dicts with the ones for the new, smaller grid subcells
        # remove the original grid cells from dicts
        grid_counts = {**grid_counts, **add_counts}
        grid_coords = {**grid_coords, **add_coords}
        for key in remove_keys:
            grid_counts.pop(key)
            grid_coords.pop(key)

        logging.info(
            f"Iteration {it}: {len(grid_counts.keys())} clusters (mean:"
            f" {np.mean(list(grid_counts.values())):.1f},            min:"
            f" {np.min(list(grid_counts.values()))}, max:"
            f" {np.max(list(grid_counts.values()))})"
        )
        it += 1

        # if the mean size of all clusters exceeding max_cluster_size has not decreased,
        # stop the process
        cluster_sizes = np.array(list(grid_counts.values()))
        mean_large_cluster_size = np.mean(cluster_sizes[cluster_sizes > max_cluster_size])
        if (
            mean_large_cluster_size >= prev_mean_large_cluster_size
            and prev_mean_large_cluster_size > 0
        ):
            break
        prev_mean_large_cluster_size = mean_large_cluster_size

    logging.info(
        f"Final result: {len(grid_counts.keys())} clusters (mean:"
        f" {np.mean(list(grid_counts.values())):.1f},            min:"
        f" {np.min(list(grid_counts.values()))}, max: {np.max(list(grid_counts.values()))})"
    )

    return grid_counts, grid_coords
