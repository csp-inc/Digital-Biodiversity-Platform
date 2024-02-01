# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime
from functools import wraps
from unittest import TestCase
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import shapely
from pystac import Asset, Item
from pystac_client.exceptions import APIError
from stac_data_loader import STACDataLoader

RANDOM_GEOM = {
    "type": "Polygon",
    "coordinates": [
        [
            [-2.5048828125, 3.8916575492899987],
            [-1.9610595703125, 3.8916575492899987],
            [-1.9610595703125, 4.275202171119132],
            [-2.5048828125, 4.275202171119132],
            [-2.5048828125, 3.8916575492899987],
        ]
    ],
}

RANDOM_BBOX = [
    RANDOM_GEOM["coordinates"][0][0][0],
    RANDOM_GEOM["coordinates"][0][0][1],
    RANDOM_GEOM["coordinates"][0][1][0],
    RANDOM_GEOM["coordinates"][0][1][1],
]


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the query call succeed.
    Most objects are then asserted on in test.
    """

    @patch("stac_data_loader.pc")
    @patch("stac_data_loader.Client")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


def return_input(value):
    return value


class TestStacDataLoader(TestCase):
    @patch_all
    def test_query_with_retry(
        self,
        mock_pystac_client: MagicMock,
        mock_pc: MagicMock,
    ):
        # arrange
        self._initilize_mocks(mock_pystac_client, mock_pc)

        # to complex to mock gdf_aoi, use real one instead with random data
        # Create a GeoDataFrame to use for spatial filtering
        geometries = [
            shapely.geometry.box(-122, 48, -122.5, 48.5),
            shapely.geometry.box(-122.5, 48, -123, 48.5),
            shapely.geometry.box(-122, 48.5, -122.5, 49),
        ]
        key_values = ["value1", "value2", "value3"]
        my_df = pd.DataFrame.from_dict({"my_key": key_values, "geometry": geometries})

        gdf_aoi = gpd.GeoDataFrame(my_df, geometry="geometry", crs="epsg:4326")

        stac_data_loader = STACDataLoader(
            MagicMock(), MagicMock(), gdf_aoi, MagicMock(), kwargs=[]
        )

        # act
        stac_data_loader.query()

        # assert
        # asser that get_items was called 3 times due to retry
        # (see side effect definition below)
        self.assertEqual(self._search.get_items.call_count, 3)

    def _initilize_mocks(self, mock_pystac_client, mock_pc):
        # planetary computer sign does nothing
        # we dont need the token since we wont actually access the data
        mock_pc.sign.side_effect = return_input

        self._endpoint = MagicMock()
        self._search = MagicMock()

        # create fake search results
        item1 = Item(
            id="item1",
            geometry=RANDOM_GEOM,
            bbox=RANDOM_BBOX,
            datetime=datetime.utcnow(),
            properties={},
        )
        item1.add_asset("ortho", Asset(href="/some/ortho.tif"))
        item2 = Item(
            id="item2",
            geometry=RANDOM_GEOM,
            bbox=RANDOM_BBOX,
            datetime=datetime.utcnow(),
            properties={},
        )
        self._test_results = [item1, item2]

        # Define side effects for get_items calls
        # First two calls produce an exception, third one succeeds
        self._search.get_items.side_effect = [APIError(), APIError(), self._test_results]

        self._endpoint.search.return_value = self._search

        mock_pystac_client.open.return_value = self._endpoint
