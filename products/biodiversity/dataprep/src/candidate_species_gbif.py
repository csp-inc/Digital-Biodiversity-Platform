# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import logging
import sys
import time
import uuid

import datasets.gbif as gbif
import numpy as np
import pandas as pd
import utils.key_vault_utils as key_vault_utils
from argument_exception import ArgumentException
from azure_logger import AzureLogger


def candidate_species_gbif(
    gbif_vertebrates_file_path: str,
    candidate_species_list_path: str,
    csv_output_directory: str,
    minimum_number_of_observations: int,
    fraction_of_data_for_pairwise_distance: float,
    correlation_id: uuid.UUID = uuid.uuid4(),
    generate=False,
) -> None:
    """
    This script uses the functionality implemented in src/gbif.py to subsample
    the GBIF data into candidate species. Specifically, this code determines
    the species that meet a threshold level of observation concentration within
    the previously subsampled GBIF dataset that is spatially proximate to
    Cherry Point. Secondly, this query also groups species by their
    mean Euclidean distance from other observations that are of the same type.
    This grouping will allow for later model tests to evaluate the effects of
    observation frequency and spatial distance on species distribution model
    performance. Given that this is primarily a feasibility study, we
    anticipate low observation frequency and low spatial distance
    to result in lower model robustness.

    Parameters
    ----------
    gbif_vertebrates_file_path : str
        Path to the vertebrate file to be read in.
    candidate_species_list_path : str
        Path to the species list file to be read in.
    csv_output_directory: str
        Directory to write the species csv files to. One file per species.
    minimum_number_of_observations: int
        Minimum number of observations required to include a species.
    fraction_of_data_for_pairwise_distance: float
        Fraction of the data to examine pairwise distance.
    correlation_id: uuid
        Optional Application Insights correlation id.

    Returns
    -------
        None.
    """
    azure_logger = AzureLogger(correlation_id=correlation_id, level=logging.DEBUG)

    azure_logger.log(
        "Reading gbif_vertebrates_file_path", **{"path": gbif_vertebrates_file_path}
    )
    gbif_subsampled = pd.read_csv(gbif_vertebrates_file_path)

    azure_logger.log(
        "Subsetting", **{"min number observations": minimum_number_of_observations}
    )
    df = gbif.subset_by_minimum_obs(gbif_subsampled, min_obs=minimum_number_of_observations)

    azure_logger.log("Subsampling gbif")
    gbif_f = gbif_subsampled[
        gbif_subsampled["species"].isin(df["species"])
    ]  # subset gbif to match species subset

    df["quantilerank"] = pd.qcut(
        df["obs"], q=4, labels=False
    )  # use quantile ranks to get appropriate observation density

    df_list = []
    ed_list = []

    for s_query in df["species"]:
        gdf = gbif.get_mean_sample_distance(
            gbif_f, s_query, frac=fraction_of_data_for_pairwise_distance
        )
        df_list.append(gdf)
        ed_list.append(np.mean(gdf["mean_euclidean_dist"]))
        del gdf  # clear memory from cluster

    df["mean_distance"] = ed_list
    df["quantilerank_dist"] = pd.qcut(df["mean_distance"], q=4, labels=False)

    # we want 16 candidate species as a mix of obs and distance.
    df["candidate"] = df.groupby(["quantilerank", "quantilerank_dist"], sort=False).ngroup()

    if generate:
        number_of_candidate_species_per_group = 1  # one candidates per group

        candidates = (
            df.groupby("candidate", group_keys=False)
            .apply(
                lambda x: x.sample(number_of_candidate_species_per_group, random_state=1)
            )
            .reset_index(drop=True)
        )

        azure_logger.log(
            "Writing candidate species",
            **{"candidate_species_list_path": candidate_species_list_path},
        )

        candidates.to_csv(candidate_species_list_path, index=False)

    else:
        azure_logger.log(
            "Reading candidate species",
            **{"candidate_species_list_path": candidate_species_list_path},
        )

        candidates = pd.read_csv(candidate_species_list_path)

    # For each candidate species get list of all other species in same bin
    # From full gbif get all species rows from same bin except candidate species
    # Sample from this subset the same number of observations as candidate species
    # Relabel the species column to candidate species
    # Relabel the 'occurrencestatus' to absent
    # Merge candidate species table with relabeled absence table
    # Write candidate species table to a local csv file.

    # definte columns wanted at output
    outcolumns = [
        "gbifid",
        "species",
        "year",
        "month",
        "occurrencestatus",
        "decimallatitude",
        "decimallongitude",
    ]

    for i in range(len(candidates)):
        # iterate on the candidates
        candidate = candidates.iloc[i]
        bin_query = candidate["candidate"]
        cspecies = candidate["species"]
        obs = candidate["obs"]

        # get all other species that match candidate 'bin'
        species_names = [
            s for s in list(df[df["candidate"] == bin_query]["species"]) if s != cspecies
        ]

        # generate random samples same number as candidate observations
        absence = gbif_subsampled[gbif_subsampled["species"].isin(species_names)].sample(
            n=obs, replace=False, random_state=1
        )
        absence = absence[outcolumns]
        absence["species"] = cspecies
        absence["occurrencestatus"] = "ABSENT"

        if generate:
            presence = gbif_subsampled[gbif_subsampled["species"] == cspecies][outcolumns]
        else:
            presence = gbif_subsampled[gbif_subsampled["species"] == cspecies].sample(
                n=obs, replace=False, random_state=1
            )
            presence = presence[outcolumns]

        # merge with candidate table
        candidate_gold_data = pd.concat([presence, absence]).reset_index(drop=True)
        outfilename = "%s_golddata.csv" % (cspecies).replace(" ", "_").lower()

        azure_logger.log(
            "Writing csv output file", **{outfilename: len(candidate_gold_data)}
        )

        candidate_gold_data.to_csv(csv_output_directory + "/" + outfilename, index=False)


def main(argv) -> None:
    args = __define_arguments(argv)

    if args.key_vault_name is not None:
        key_vault_utils.get_environment_from_kv(args.key_vault_name)

    azure_logger = AzureLogger(correlation_id=args.correlation_id, level=logging.DEBUG)

    azure_logger.event("starting candidate_species_gbif")
    start_time = time.time()
    try:
        candidate_species_gbif(
            gbif_vertebrates_file_path=args.gbif_vertebrates_file_path,
            candidate_species_list_path=args.candidate_species_list_path,
            csv_output_directory=args.csv_output_directory,
            minimum_number_of_observations=args.minimum_number_of_observations,
            fraction_of_data_for_pairwise_distance=args.fraction,
            correlation_id=args.correlation_id,
            generate=args.generate,
        )

    except ArgumentException:
        sys.exit(2)
    except Exception as ex:
        azure_logger.exception(ex, "candidate_species_gbif.py failed")
        sys.exit(1)
    finally:
        end_time = time.time()
        azure_logger.event(
            "completed candidate_species_gbif",
            **{"duration (s)": str((end_time - start_time))},
        )


def __define_arguments(args):
    parser = argparse.ArgumentParser("candidate_species_gbif")

    parser.add_argument(
        "--generate",
        required=False,
        action="store_const",
        const=True,
        default=False,
        help="Generate new list of cancdidate species.",
    )
    parser.add_argument(
        "--gbif_vertebrates_file_path",
        required=True,
        type=str,
        help="Path to file containing the GBIF vertebrates.",
    )
    parser.add_argument(
        "--candidate_species_list_path",
        type=str,
        required=True,
        help="Path to the file containing the candiate species list.",
    )
    parser.add_argument(
        "--csv_output_directory",
        type=str,
        required=True,
        help="Path to directory to write the output files to.",
    )
    parser.add_argument(
        "--minimum_number_of_observations",
        type=int,
        default=500,
        required=False,
        help="Minimum number of observations for a species to be included.",
    )
    parser.add_argument(
        "--fraction",
        type=float,
        default=0.5,
        required=False,
        help="Fraction of the data to examine pairwise distance.",
    )
    parser.add_argument(
        "--correlation_id",
        type=str,
        required=False,
        default=uuid.uuid4(),
        help="Application Insights correlation id if required.",
    )

    parser.add_argument(
        "--key_vault_name",
        type=str,
        required=False,
        help="Key Vault name where to retrieve secrets.",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
