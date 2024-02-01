# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import re
import xarray as xr


def calculate_multispectral_indices(xarr, formulas):
    """
    Calculates indices from xarr band dimension.

    Parameters
    ----------
    xarr : xr.Dataset or xr.DataArray
        Contains multispectral imagery used in the calculations.
    formulas : dict
        Contains the names (keys) and formulas (values) of the indices
        to be calculated, with band names in square brackets, e.g.:
        {'ndvi': '([nir08]-[red]) / ([nir08]+[red])'}.
        Each entry in the dictionary will be an item along the 'band'
        dimension of the output array.

    Returns
    -------
    xr.DataArray
        Contains the calculated indices along 'band' dimension,
        all other dimensions identical to input xarr.
    """

    band_coords = list(xarr.coords["band"].values)

    index_xarrs = []
    for name, formula in formulas.items():
        # interpret formula (add code to load correct bands)
        for item in re.findall(r"\[(.*?)\]", formula):
            formula = formula.replace(
                f"[{item}]", f"xarr.isel(band={band_coords.index(item)})"
            )
        # calculate index
        xarr_ = eval(formula)
        # drop any existing variable to avoid issues during concatenation later
        xarr_ = xarr_.drop("band", errors="ignore")
        index_xarrs.append(xarr_)

    # merge indices into one array
    indices = xr.concat(index_xarrs, dim="band")
    # update band dimension
    indices = indices.assign_coords(band=list(formulas.keys()))
    # reorder dimensions to match common structure
    indices = indices.transpose("time", "band", "y", "x")

    return indices
