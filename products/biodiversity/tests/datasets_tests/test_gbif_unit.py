# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime
from functools import wraps
from unittest import TestCase
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import numpy as np
import pandas as pd
from datasets.gbif import read_gbif
from pystac import Asset, Item, ItemCollection
from pystac_client.exceptions import APIError

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

TEST_GBIF_DF = dd.from_pandas(
    pd.DataFrame(
        {
            "decimallongitude": np.linspace(10.1, 15.2, 100),
            "decimallatitude": np.linspace(-30, 41.24, 100),
            "class": "Reptilia",
            "species": "test",
            "month": "12",
            "day": "20",
        }
    ),
    npartitions=8,
)


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the read_gbif call succeed.
    Most objects are then asserted on in test.
    """

    @patch("datasets.gbif.dd")
    @patch("datasets.gbif.pc")
    @patch("datasets.gbif.Client")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


def return_input(value):
    return value


class TestGbif(TestCase):
    @patch_all
    def test_read_gbif_with_retry(
        self,
        mock_pystac_client: MagicMock,
        mock_pc: MagicMock,
        mock_dd: MagicMock,
    ):
        # arrange
        self._initilize_mocks(mock_pystac_client, mock_pc, mock_dd)

        # act
        read_gbif()

        # assert
        # assert that get_all_items was called 3 times due to retry
        # (see side effect definition below)
        self.assertEqual(self._search.get_all_items.call_count, 3)

    def _initilize_mocks(self, mock_pystac_client, mock_pc, mock_dd):
        # planetary computer sign does nothing, we dont need the token
        # since we wont actually access the data
        mock_pc.sign.side_effect = return_input

        # use a real dataframe with garbage data
        # (not important, since we're only testing the retry)
        # avoiding null pointer problems
        mock_dd.read_parquet.return_value = TEST_GBIF_DF

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
        item1.add_asset(
            "data",
            Asset(
                href="/some/data.tif",
                extra_fields={"table:storage_options": 0},
            ),
        )
        item2 = Item(
            id="item2",
            geometry=RANDOM_GEOM,
            bbox=RANDOM_BBOX,
            datetime=datetime.utcnow(),
            properties={},
        )
        self._test_results = ItemCollection([item1, item2])

        # Define side effects for get_all_items calls
        # First two calls produce an exception, third one succeeds
        self._search.get_all_items.side_effect = [
            APIError(),
            APIError(),
            self._test_results,
        ]

        self._endpoint.search.return_value = self._search

        mock_pystac_client.open.return_value = self._endpoint
