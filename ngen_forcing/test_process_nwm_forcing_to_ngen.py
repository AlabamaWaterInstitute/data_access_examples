# import rioxarray as rxr
import xarray as xr
import geopandas as gpd
from rasterstats import zonal_stats

# import rasterio
import pandas as pd

import time

from process_nwm_forcing_to_ngen import (
    get_forcing_dict_newway,
    get_forcing_dict_newway_parallel,
    get_forcing_dict_newway_inverted,
    get_forcing_dict_newway_inverted_parallel,
)

from pathlib import Path
import warnings

warnings.simplefilter("ignore")

# Read forcing files
# Generate List of files

# TODO: Add looping through lists of forcing files
# consider looking at the "listofnwmfilenames.py" in the data_access_examples repository.
# Integer values for runinput, varinput, etc. are listed at the top of the file
# and an example is given in the `main` function.

# import listofnwmfilenames
#     create_file_list(
#         runinput,
#         varinput,
#         geoinput,
#         meminput,
#         start_date,
#         end_date,
#         fcst_cycle,
#     )

"""
A set of test files can be generated downloading these files
wget -P data -c https://storage.googleapis.com/national-water-model/nwm.20220824/forcing_medium_range/nwm.t12z.medium_range.forcing.f001.conus.nc
wget -P data -c https://storage.googleapis.com/national-water-model/nwm.20220824/forcing_medium_range/nwm.t12z.medium_range.forcing.f002.conus.nc
wget -P data -c https://storage.googleapis.com/national-water-model/nwm.20220824/forcing_medium_range/nwm.t12z.medium_range.forcing.f003.conus.nc
wget -P 03w -c https://nextgen-hydrofabric.s3.amazonaws.com/v1.2/nextgen_03W.gpkg
"""


def get_forcing_dict(
    feature_index,
    feature_list,
    folder_prefix,
    filelist,
    var_list,
):
    reng = "rasterio"
    sum_stat = "mean"

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=feature_index)

    ds_list = []
    for _nc_file in filelist:
        # _nc_file = ("nwm.t00z.medium_range.forcing.f001.conus.nc")
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    for _i, _nc_file in enumerate(filelist):
        _xds = ds_list[_i]
        print(f"{_i}, {round(_i/len(filelist), 5)*100}".ljust(40), end="\r")
        if 1 == 1:
            for _v in var_list:
                _src = _xds[_v]
                _aff2 = _src.rio.transform()
                _arr2 = _src.values[0]

                _df_zonal_stats = pd.DataFrame(
                    zonal_stats(feature_list, _arr2, affine=_aff2)
                )
                # if adding statistics back to original GeoDataFrame
                # gdf3 = pd.concat([gpkg_divides, _df_zonal_stats], axis=1)
                df_dict[_v][_xds.time.values[0]] = _df_zonal_stats[sum_stat]

    [_xds.close() for _xds in ds_list]

    return df_dict


# TODO: Convert the output to CSV with something like
# `gdf3.to_csv`


def main():

    folder_prefix = Path("data")
    list_of_files = [
        f"nwm.t12z.medium_range.forcing.f{_r:03}.conus.nc" for _r in range(1, 241)
    ]

    # Read basin boundary file
    f_03 = "03w/nextgen_03W.gpkg"
    gpkg_divides = gpd.read_file(f_03, layer="divides")
    var_list = [
        "U2D",
        "V2D",
        "LWDOWN",
        "RAINRATE",
        "T2D",
        "Q2D",
        "PSFC",
        "SWDOWN",
    ]

    # file_list = list_of_files[0:30]
    # gpkg_subset = gpkg_divides[0:2000]
    file_list = list_of_files[0:3]
    gpkg_subset = gpkg_divides[0:200]
    feature_list = gpkg_subset.geometry.to_list()

    # This way is extremely slow for anything more than a
    # few files, so we comment it out of the test

    start_time = time.time()
    print(f"Working on the old (slow) way")
    fd1 = get_forcing_dict(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
    )
    print(time.time() - start_time)

    start_time = time.time()
    print(f"Working on the new way")
    fd2 = get_forcing_dict_newway(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
    )
    print(time.time() - start_time)

    start_time = time.time()
    print(f"Working on the new way with threading parallel.")
    fd3t = get_forcing_dict_newway_parallel(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
        para="thread",
        para_n=16,
    )
    print(time.time() - start_time)

    start_time = time.time()
    print(f"Working on the new way with process parallel.")
    fd3p = get_forcing_dict_newway_parallel(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
        para="process",
        para_n=16,
    )
    print(time.time() - start_time)

    start_time = time.time()
    print(f"Working on the new way with loops reversed.")
    fd4 = get_forcing_dict_newway_inverted(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
    )
    print(time.time() - start_time)

    start_time = time.time()
    print(f"Working on the new way with loops reversed with threading parallel.")
    fd5t = get_forcing_dict_newway_inverted_parallel(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
        para="thread",
        para_n=16,
    )
    print(time.time() - start_time)

    start_time = time.time()
    print(f"Working on the new way with loops reversed with process parallel.")
    fd5p = get_forcing_dict_newway_inverted_parallel(
        gpkg_subset.index,
        feature_list,
        folder_prefix,
        file_list,
        var_list,
        para="process",
        para_n=16,
    )
    print(time.time() - start_time)


if __name__ == "__main__":
    main()
