# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import os
import warnings
from datetime import datetime, timedelta

import backoff
import fsspec
import geojson
import geopandas as gpd
import planetary_computer as pc
import stackstac
import utm
import xarray as xr
from pyproj import CRS, Transformer
from pystac_client import Client
from pystac_client.exceptions import APIError
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from shapely.geometry import Point, shape
from shapely.ops import transform

# suppressing warnings from shapely, pyproj and geopandas (deprecation mesages
# etc.)
warnings.simplefilter("ignore")


def get_query_max_retry_time() -> int:
    """
    Returns the maximum time in seconds that retries of the query
    method in the STACDataLoader class will be attempted.

    Paramters
    ----------
    None

    Returns
    ----------
    int
        Maximum retry time for query method in seconds.
    """
    return int(os.getenv("QUERY_MAX_RETRY_TIME", "300"))


class STACDataLoader:
    """
    Provides simplified access to STAC queries.

    Parameters
    ----------
    endpoint : str
        The URL to the STAC endpoint to query from, e.g.
        'https://planetarycomputer.microsoft.com/api/stac/v1'.
    collections : list of str
        Names of collections to be queried. All regular collections on Planetary
        Computer are supported, but Zarr collections are only supported if they
        have the 'zarr-https' asset.
        NOTE: handling of Zarr and non-Zarr collections is different and mixing
        the two might cause problems. If querying Zarr collections, ideally only
        use a single Zarr collection or multiple Zarr collections with the same
        assets/variables. See also explanation under 'assets'.
    aoi : str, geopandas.geodataframe.GeoDataFrame or tuple of int/float
        Represents the area of interest. Can be provided in three different
        forms:
        1) str: path to a vector file (e.g. Shapefile or GeoJSON);
        2) GeoDataFrame: a GeoDataFrame representing the area of interest
        (e.g. from a previously loaded GeoJSON file);
        3) tuple: a single point represented by a list/tuple of int/float (lon,
        lat).
        NOTE 1: if a file or GeoDataFrame contain multiple features, all of them
        are merged to a single (Multi)Polygon (union).
        NOTE 2: non-Zarr collections will be subset to this AOI, Zarr collections
        remain unchanged and must be subset later manually. The reason for this is
        that the Zarr collections have inconsistent formats, dimension names and
        projection systems, necessitating a sort of hard-coded solution that is
        impractical.
    assets : list of str
        Names of the assets (or variables in case of Zarr) that will be loaded
        from the queried datasets.
        NOTE: Different behavior for Zarr vs non-Zarr collections:
        In non-Zarr collections asset names that are not found in the target
        collection are simply skipped. In Zarr collections, assets that are not
        found will raise an error.
    time_range : str or list/tuple of str/datetime.datetime
        Represents the time of interest. Can be provided in two forms:
        1) a single string of format 'yyyy-mm-dd/yyyy-mm-dd';
        2) a list/tuple of strings of format 'yyyy-mm-dd' or datetime.datetime
        objects.
    spatial_buffer : int/float; default=0
        A spatial buffer to be applied to the provided AOI (symmetrical in
        x-/y-directions).
        The spatial buffer is interpreted in meters in the corresponding UTM
        zone.
        Use negative values to shrink the AOI (does not apply to single point
        AOIs).
        NOTE: in case a single point is given as AOI, buffer is applied
        symmetrically to x- and y-dimensions (not circular).
    temporal_buffer : int; default=0
        The number of days to buffer around the observation time.
        Ignored if time_range is provided as a single string.
    custom_headers : dict of str; default=None
        Custom headers to pass to STAC API client (e.g. for API authorization).
    verbose : bool; default=False
        If True, prints messages on progress, query results etc.
    kwargs:
        Further takes any keyword arguments compatible with the
        pystac_client.Client.search method and includes them in the query.
    """

    def __init__(
        self,
        endpoint,
        collections,
        aoi,
        assets,
        time_range=None,
        spatial_buffer=0.0,
        temporal_buffer=0,
        custom_headers=None,
        verbose=False,
        **kwargs,
    ):
        self.endpoint = Client.open(endpoint, headers=custom_headers)
        self.collections = collections
        self.assets = assets
        self.verbose = verbose
        self._query_kwargs = kwargs
        self._prepare_aoi(aoi, spatial_buffer)
        self._prepare_time_range(time_range, temporal_buffer)

    def _apply_spatial_buffer(self, aoi, spatial_buffer):
        """
        Applies a spatial buffer to a given AOI in UTM projection.

        Parameters
        ----------
        aoi : shapely Polygon
            Feature representing AOI (see _prepare_aoi()) in WGS84.
        spatial_buffer
            See class init.

        Returns
        -------
        GeoDataFrame
            Buffered AOI.
        """

        wgs_to_utm = Transformer.from_crs(
            "EPSG:4326", f"EPSG:{self.utm_epsg}", always_xy=True
        )
        utm_to_wgs = Transformer.from_crs(
            f"EPSG:{self.utm_epsg}", "EPSG:4326", always_xy=True
        )
        aoi = transform(wgs_to_utm.transform, aoi)
        aoi = aoi.buffer(spatial_buffer)
        aoi = transform(utm_to_wgs.transform, aoi)
        return aoi

    def _get_utm_epsg(self, feature):
        """
        Obtains the corresponding UTM zone EPSG of a feature
        in WGS84.
        Uses the centroid of the feature to detec the correct
        UTM zone. This may cause issues for very large study areas
        spanning multiple UTM zones.

        Parameters
        ----------
        Shapely geometry
            The geometry for which the corresponding UTM EPSG should be
            obtained.

        Returns
        -------
        int
            EPSG of UTM zone.
        """

        # obtain UTM EPSG
        centroid = list(feature.centroid.coords)
        _, _, zone, _ = utm.from_latlon(centroid[0][1], centroid[0][0])
        utm_epsg = CRS.from_dict({"proj": "utm", "zone": zone}).to_epsg()
        return utm_epsg

    def _prepare_aoi(self, aoi, spatial_buffer):
        """
        Prepares AOI in both WGS84 and the corresponding UTM
        projection for use in later steps (query, masking etc.).

        Parameters
        ----------
        See class init.

        Returns
        -------
        None
        """

        # AOI provided as a list/tuple of coordinates (single point)
        if isinstance(aoi, list) or isinstance(aoi, tuple):
            if self.verbose:
                logging.info("Provided AOI interpreted as single point.")

            lon, lat = aoi
            x, y, zone, _ = utm.from_latlon(lat, lon)

            # Convert point to Mercator
            if spatial_buffer > 0:
                _bottom, _top = y - spatial_buffer, y + spatial_buffer
                _left, _right = x - spatial_buffer, x + spatial_buffer

                # Convert bounds back to Lat/Lon
                self.utm_epsg = self._get_utm_epsg(Point(lon, lat))
                transformer = Transformer.from_crs(
                    f"EPSG:{self.utm_epsg}", "EPSG:4326", always_xy=True
                )
                left, bottom = transformer.transform(_left, _bottom)
                right, top = transformer.transform(_right, _top)

                # create GeoJSON geometry for queries;
                # save AOI centroid and bounding box
                aoi_wgs = {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [left, bottom],
                            [left, top],
                            [right, top],
                            [right, bottom],
                            [left, bottom],
                        ]
                    ],
                }
                aoi_utm = {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [_left, _bottom],
                            [_left, _top],
                            [_right, _top],
                            [_right, _bottom],
                            [_left, _bottom],
                        ]
                    ],
                }

            else:
                _bottom, _top = y - spatial_buffer, y + spatial_buffer
                _left, _right = x - spatial_buffer, x + spatial_buffer

                # create GeoJSON geometry for queries;
                # save AOI centroid and bounding box
                aoi_wgs = {"type": "Point", "coordinates": [lon, lat]}
                aoi_utm = {"type": "Point", "coordinates": [x, y]}

            # store AOIs as Shapely objects and GeoJSON
            self.aoi_wgs = shape(aoi_wgs)
            self.aoi_utm = shape(aoi_utm)
            self.aoi_wgs_json = geojson.Feature(geometry=aoi_wgs)
            self.aoi_utm_json = geojson.Feature(geometry=aoi_utm)

        else:
            # AOI provided as a string (interpreted as file path)
            if isinstance(aoi, str):
                # read file, reproject to WGS84
                aoi_ = gpd.read_file(aoi)
                aoi_wgs = aoi_.to_crs("EPSG:4326")
            # AOI provided as GeoDataFrame
            elif isinstance(aoi, gpd.geodataframe.GeoDataFrame):
                aoi_wgs = aoi.to_crs("EPSG:4326")

            # merge into one (Multi)Polygon;
            if len(aoi_wgs) > 1 and self.verbose:  # pyright: reportUnboundVariable=false
                logging.info("AOI contains multiple features, will be merged.")
            aoi_wgs = aoi_wgs.geometry.unary_union

            # get UTM EPSG
            self.utm_epsg = self._get_utm_epsg(aoi_wgs)

            # optionally apply buffer
            if spatial_buffer > 0:
                aoi_wgs = self._apply_spatial_buffer(aoi_wgs, spatial_buffer)

            # create UTM version of AOI
            wgs_to_utm = Transformer.from_crs(
                "EPSG:4326", f"EPSG:{self.utm_epsg}", always_xy=True
            )
            aoi_utm = transform(wgs_to_utm.transform, aoi_wgs)

            # store AOIs as Shapely objects and GeoJSON
            self.aoi_wgs = shape(aoi_wgs)
            self.aoi_utm = shape(aoi_utm)
            self.aoi_wgs_json = geojson.Feature(geometry=aoi_wgs)
            self.aoi_utm_json = geojson.Feature(geometry=aoi_utm)

    def _prepare_time_range(self, time_range, temporal_buffer):
        """
        Prepares provided time range for use in queries.

        Parameters
        ----------
        See class init.

        Returns
        -------
        None
        """

        if time_range is None:
            self.time_range_nonzarr = None
            self.time_range_zarr = None
        # time range provided as strings of datetime objects
        elif isinstance(time_range, list) or isinstance(time_range, tuple):
            # convert strings to datetime
            start, end = time_range
            if isinstance(start, str):
                start = datetime.strptime(start, "%Y-%m-%d")
            if isinstance(end, str):
                end = datetime.strptime(end, "%Y-%m-%d")

            # optionally apply buffer
            if temporal_buffer > 0:
                start = start - timedelta(days=temporal_buffer)
                end = end + timedelta(days=temporal_buffer)

            # convert to expected format
            start_str = datetime.strftime(start, "%Y-%m-%d")
            end_str = datetime.strftime(end, "%Y-%m-%d")
            self.time_range_nonzarr = f"{start_str}/{end_str}"
            self.time_range_zarr = (start, end)

        # time range provided as a single string (expected to be of
        # correct format)
        elif isinstance(time_range, str):
            start_str, end_str = time_range.split("/")
            start = datetime.strptime(start_str, "%Y-%m-%d")
            end = datetime.strptime(end_str, "%Y-%m-%d")
            self.time_range_nonzarr = time_range
            self.time_range_zarr = (start, end)

    def _get_cloud_mask_asset(self, collection):
        """
        Simply returns the correct cloud mask assets to be queried
        for different collections (currently only L-8 and S-2).

        Parameters
        ----------
        collection : str
            Name of the collection.

        Returns
        -------
        str
            Name of corresponding cloud mask asset.
        """

        if collection == "landsat-8-c2-l2":
            return "QA_PIXEL"
        elif collection == "sentinel-2-l2a":
            return "SCL"
        elif collection == "landsat-c2l2-sr" or collection == "landsat-c2-l2":
            return "qa_pixel"
        else:
            if self.verbose:
                logging.info(f"No cloud mask found for '{collection}'.")
            return None

    @backoff.on_exception(backoff.expo, APIError, max_time=get_query_max_retry_time)
    def query(self):
        """
        Performs query according to previously defined settings.

        Parameters
        ----------
        None

        Returns
        -------
        Depends on the type of collection that is queried:
        Non-Zarr collections:
            list of pystac.item.Items
                Contains the feature response from the STAC API ('raw results').
            GeoDataFrame
                Contains the properties and general information as well
                as geometries of each queried item in a more readable format.
        Zarr collections:
            None
        """

        # performs search based on collections, AOI and time range
        zarr_collections = [
            "terraclimate",
            "daymet-daily-na",
            "daymet-annual-pr",
            "gridmet",
            "daymet-daily-pr",
            "daymet-daily-hi",
            "daymet-monthly-na",
            "daymet-monthly-pr",
            "daymet-annual-na",
            "daymet-monthly-hi",
            "daymet-annual-hi",
            "gpm-imerg-hhr",
        ]
        if any([c in zarr_collections for c in self.collections]):
            logging.info(
                "Provided collection is zarr-based. Behavior differs (for details, see"
                " class docstring)."
            )
            self.signed_items = []
            for c in self.collections:
                asset = self.endpoint.get_collection(c).assets["zarr-https"]
                mapper = fsspec.get_mapper(asset.href)
                self.signed_items.append((asset, mapper))
            if self.verbose:
                logging.info(f"Items found: {len(self.signed_items)}")
            self.results = None

            return None, None
        else:
            search = self.endpoint.search(
                collections=self.collections,
                intersects=self.aoi_wgs_json["geometry"],
                datetime=self.time_range_nonzarr,
                **self._query_kwargs,
            )

            # return search, None
            results = list(search.get_items())
            if self.verbose:
                logging.info(f"Items found: {len(results)}")
            self.results = results

            # sign items for later reading/loading
            self.signed_items = [pc.sign(item).to_dict() for item in self.results]

            # organize raw results in a GeoDataFrame for better overview
            features = []
            for item in results:
                feature = item.properties
                feature["id"] = item.id
                feature["collection_id"] = item.collection_id
                feature["geometry"] = shape(item.geometry)
                features.append(feature)
            self.query_gdf = gpd.GeoDataFrame(features)

            return self.results, self.query_gdf

    def load_data(  # noqa: C901
        self,
        assets=None,
        resolution=None,
        epsg=None,
        resampling=Resampling.nearest,
        bounds=None,
        bounds_latlon=None,
        clip=False,
        **kwargs,
    ):
        """
        Loads data from query to xarray.
        NOTE: most parameters only work with non-Zarr collections, incl.
        resolution, bounds, bounds_latlon, clip.

        Parameters
        ----------
        assets : list of str; default=None
            If None, resorts to values provided on init.
        resolution : int/float or tuple of int/float; default=None
            Resolution for resampling of all products in array.
            If None, resorts to highest available resolution in stack
            (default behavior of stackstac.stack).
        epsg : int; default=None
            The EPSG to use for the loaded data. If None, resorts to the
            default UTM projection obtained during initialization
            (self.utm_epsg).
        resampling : rasterio.enums.Resampling object; default=Resampling.nearest
            The resampling method to use. Can use any method available in
            rasterio.enums.Resampling (see Notes).
        bounds : tuple of float/int; default=None
            Optional bounding box for loading only a subset of the data. Coordinates
            must be in the coordinate system defined in epsg.
            Format: (min_x, min_y, max_x, max_y).
            If None, resorts to using the bounding geometry of the original AOI.
            Mutually exclusive with bounds_latlon.
        bounds_latlon : tuple of float/int; default=None
            Same as bounds but coordinates given in WGS84. Mutually exclusive
            with bounds.
        clip : bool; default=False
            If True, masks the exact boundaries of AOI, otherwise
            only subsets to the bounding box geometry.
            Not available for Zarr collections.
        kwargs:
            Further takes any keyword arguments compatible with stackstac.stack
            and passes them to the class.

        Returns
        -------
        Non-Zarr collections:
            xarray.core.dataarray.DataArray
                Lazily initialized xarray of products, subset to
                bounding box of AOI.
        Zarr collections:
            list of xarray.core.dataset.Dataset
                Lazily initialized xarrays of collections.
                NOTE: the output represents the entire dataset without subsetting.

        Notes
        -----
        For available rasterio resampling methods, see:
        https://rasterio.readthedocs.io/en/latest/api/rasterio.enums.html#rasterio.enums.Resampling
        """

        # resort to default attributes (from init)
        if assets is None:
            assets = self.assets
        if epsg is None:
            epsg = self.utm_epsg
        # unify format for resolution
        if isinstance(resolution, float) or isinstance(resolution, int):
            res = (resolution, resolution)
        else:
            res = resolution

        # prepare bounds
        if bounds is None and bounds_latlon is None:
            bounds_latlon = self.aoi_wgs.bounds
            bounds = None

        # load data to xarray
        if isinstance(self.signed_items[0], dict):
            # FIX BEGIN - check if href is corrupted
            # (https://github.com/microsoft/PlanetaryComputer/issues/203)
            corrupted_href = "https://landsateuwest.blob.core.windows.net/landsat-c2/level-2/standard/oli-tirs/2022/047/028/LC08_L2SP_047028_20221108_20221115_02_T1/LC08_L2SP_047028_20221108_20221115_02_T1_SR_B4.TIF"  # noqa: E501
            excluded = []
            for item in self.signed_items:
                for k, v in item["assets"].items():
                    if corrupted_href in v["href"]:
                        if item not in excluded:
                            excluded.append(item)
                        logging.info(f"Found a corrupted reference for {item['id']}")

            for item in excluded:
                self.signed_items.remove(item)
            # FIX END - check if href is corrupted

            data = stackstac.stack(
                self.signed_items,
                assets=assets,
                resolution=res,
                resampling=resampling,
                epsg=epsg,
                bounds_latlon=bounds_latlon,
                bounds=bounds,
                **kwargs,
            )
            # optionally clip to exact study area
            if clip:
                mask = geometry_mask(
                    [self.aoi_utm],
                    out_shape=(data.shape[2], data.shape[3]),
                    transform=data.transform,
                    invert=True,
                )
                self._clip_mask = mask
                data = data.where(mask)
        # load data to xarray, special case of Zarr files
        elif isinstance(self.signed_items[0], tuple):
            data = []
            for item in self.signed_items:
                # loading Zarr file lazily
                asset, mapper = item
                ds = xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
                # selecting by time and assets
                if self.time_range_zarr is not None:
                    ds = ds.sel(
                        time=slice(self.time_range_zarr[0], self.time_range_zarr[1])
                    )
                if self.assets is not None:
                    ds = ds[[name.lower() for name in self.assets]]
                data.append(ds)
        else:
            raise ValueError("No signed items found or wrong format.")

        return data
