# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest import TestCase

import numpy as np
import pandas as pd
from models.lgbm import get_parameter_space_lgbm, objective


class TestLgbm(TestCase):
    def test_get_param_space(self):
        params = get_parameter_space_lgbm()

        self.assertIsNotNone(params, None)
        self.assertIsInstance(params, dict)
        self.assertGreater(len(params), 0)

    def test_objective(self):
        features = pd.DataFrame(np.random.rand(100, 4))
        targets = pd.Series(np.random.randint(0, 1, size=(100)))
        parameter_space = {"num_leaves": 30, "objective": "binary"}

        result = objective(parameter_space, features, targets, 2)
        self.assertIsNotNone(result, None)
        self.assertIsInstance(result, dict)
        self.assertGreater(len(result), 0)
        self.assertIn("loss", result.keys())
