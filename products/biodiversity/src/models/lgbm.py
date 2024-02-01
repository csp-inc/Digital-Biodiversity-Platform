# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import lightgbm as lgb
import numpy as np
import pandas as pd
from hyperopt import STATUS_OK, hp


def objective(
    param_space: dict,
    features: pd.DataFrame,
    targets: pd.Series,
    kfold: int = 5,
    random_seed: int = 42,
    verbose: int = -1,
) -> dict:
    """
    Objective function to be used for hyperopt hyperparameter tuning.
    Please note that all function's arguments besides `param_space` need to be
    provided using `functools.partial`, after that the function can be used
    as input for the hyperopt process (with only `param_space` argument not given)

    Parameters
    ----------
    param_space : dict
        Dictionary of parameters and hyperparameters
    features : pd.DataFrame
        Training dataset
    targets : pd.Series
        Tagret variable
    kfold : int, optional
        Number of folds in KFold Cross Validation, by default 5
    random_seed : int, optional
        Random seed, by default 42
    verbose : int, optional
        Verbosity level, by default -1

    Returns
    -------
    dict
        Dictionary, containing best loss and status
    """
    dataset = lgb.Dataset(features, targets)
    param_space["verbose"] = verbose
    cv_results = lgb.cv(
        param_space,
        dataset,
        nfold=kfold,
        stratified=True,
        seed=random_seed,
    )

    best_loss = cv_results["binary_logloss-mean"][-1]
    return {"loss": best_loss, "status": STATUS_OK}


def get_parameter_space_lgbm() -> dict:
    """
    Generates hyperparameter space for LightGBM model.
    Includes following parameters:
    * num_leaves
    * learning_rate
    * lambda_l1
    * lambda_l2

    Returns
    -------
    dict
        Dictionary with hyperparameters
    """
    return {
        "num_leaves": hp.choice("max_depth", np.arange(40, 80, 1, dtype=int)),
        "learning_rate": hp.loguniform("learning_rate", np.log(0.08), np.log(0.2)),
        "min_data_in_leaf": hp.choice("min_data_in_leaf", np.arange(40, 300, 1, dtype=int)),
        "lambda_l1": hp.uniform("lambda_l1", 0.05, 1),
        "lambda_l2": hp.uniform("lambda_l2", 0.05, 1),
    }
