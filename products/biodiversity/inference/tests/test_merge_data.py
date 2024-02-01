# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import unittest
from functools import wraps
from unittest.mock import MagicMock, patch

from merge_data import merge_data


def patch_all(f):
    """
    Patches all of the library calls that are required to
    make the merge_data call succeed. Most objects are then
    asserted on in test.
    """

    @patch("merge_data.pd.DataFrame.to_csv")
    @wraps(f)
    def functor(*args, **kwargs):
        return f(*args, **kwargs)

    return functor


class TestMergeData(unittest.TestCase):
    _valid_gold_table_path = "products/biodiversity/inference/tests/data/cherry_pt_gold.csv"
    _valid_cherry_pt_segments = (
        "products/biodiversity/inference/tests/data/cherry_pt_segments.csv"
    )
    _batch_input_path = "products/biodiversity/inference/tests/data"
    _merged_df_path = "products/biodiversity/inference/tests/data"
    _merged_df_file = "merged_df.csv"

    @patch_all
    def test_merge_data_correct_file_paths(self, to_csv: MagicMock):
        # arrange

        # act
        merge_data(
            self._valid_cherry_pt_segments,
            self._valid_gold_table_path,
            self._batch_input_path,
            self._merged_df_path,
        )

        # assert
        self.assertTrue(to_csv.call_count, 2)

        batch_input_output_path = to_csv.call_args_list[0].args[0]
        self.assertTrue(batch_input_output_path.__contains__(self._batch_input_path))

        merged_df_output_file_path = to_csv.call_args_list[1].args[0]
        self.assertTrue(merged_df_output_file_path.__contains__(self._merged_df_path))

        # if this assertion fails because the file name has changed,
        # check the perform_inference script to make sure it's loading the correct file
        self.assertTrue(merged_df_output_file_path.__contains__(self._merged_df_file))
