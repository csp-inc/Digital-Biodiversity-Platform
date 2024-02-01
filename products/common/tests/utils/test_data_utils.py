from unittest import TestCase

import numpy as np
from utils.data_utils import split_train_test


class TestDataUtils(TestCase):
    _mock_data_features = np.random.random((100, 10))
    _mock_data_targets = np.random.random((100, 1))

    def test_split_train_test_no_validation(self):
        features_train, features_test, targets_train, targets_test = split_train_test(
            self._mock_data_features,
            self._mock_data_targets,
            train_fraction=0.9,
            validation_fraction=None,
            random_seed=42,
        )

        self.assertEqual(len(features_train), 90)
        self.assertEqual(len(targets_train), 90)
        self.assertEqual(len(features_test), 10)
        self.assertEqual(len(targets_test), 10)

    def test_split_train_test_validation(self):
        (
            features_train,
            features_val,
            features_test,
            targets_train,
            targets_val,
            targets_test,
        ) = split_train_test(
            self._mock_data_features,
            self._mock_data_targets,
            train_fraction=0.9,
            validation_fraction=0.05,
            random_seed=42,
        )

        self.assertEqual(len(features_train), 90)
        self.assertEqual(len(targets_train), 90)
        self.assertEqual(len(features_test), 5)
        self.assertEqual(len(targets_test), 5)
        self.assertEqual(len(features_val), 5)
        self.assertEqual(len(targets_val), 5)
