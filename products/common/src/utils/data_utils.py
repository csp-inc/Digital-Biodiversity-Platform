from typing import Tuple, Union

import pandas as pd
from sklearn.model_selection import train_test_split


def split_train_test(
    features_dataframe: pd.DataFrame,
    targets_dataframe: pd.DataFrame,
    train_fraction: float,
    random_seed: float,
    validation_fraction: Union[float, None] = None,
) -> Tuple:
    """
    Splits given dataset into (train, test) or (train, val and test)
    datasets randomly. If `validation_fraction` data is not set (None),
    the dataset will be divided into two parts: train and test.
    If the parameter is set, the dataset will be divided
    into three parts: train, val, test.

    Parameters
    ----------
    features_dataframe : pd.DataFrame
        DataFrame containing features
    targets_dataframe : pd.Series
        Traget variable
    train_fraction : float, optional
        Fraction of training data, by default 0.8
    validation_fraction : float, optional
        Fraction of validation data, by default None
        If fraction of validation data is not set (None),
        the dataset will be divided into two
        parts: train and test. If the parameter is set,
        the dataset will be divided into three parts:
        train, val, test.
    random_seed : int, optional
        Random seed, by default 42

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series,
    pd.Series, pd.Series]
        A tuple containing split datasets:
        (features_train, features_trest, targets_train, targets_test)
        or
        (features_train, features_validation, features_trest,
        targets_train, targets_validation, targets_test)
    """

    features_train, features_test, targets_train, targets_test = train_test_split(
        features_dataframe,
        targets_dataframe,
        train_size=train_fraction,
        random_state=random_seed,
    )

    if validation_fraction is not None:
        validation_size = validation_fraction / (1 - train_fraction)
        features_val, features_test, targets_val, targets_test = train_test_split(
            features_test,
            targets_test,
            train_size=validation_size,
            random_state=random_seed,
        )
        return (
            features_train,
            features_val,
            features_test,
            targets_train,
            targets_val,
            targets_test,
        )

    return features_train, features_test, targets_train, targets_test
