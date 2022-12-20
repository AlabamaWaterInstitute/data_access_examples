# import rioxarray as rxr
import xarray as xr
import geopandas as gpd
from rasterstats import zonal_stats
# import rasterio
import pandas as pd

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

folder_prefix = Path('data')
list_of_files = [
    "nwm.t12z.medium_range.forcing.f001.conus.nc",
    "nwm.t12z.medium_range.forcing.f002.conus.nc",
    "nwm.t12z.medium_range.forcing.f003.conus.nc",
]

# Read basin boundary file
f_03 = "03w/nextgen_03W.gpkg"
gpkg_divides = gpd.read_file(f_03, layer="divides")
list_of_vars = [
    "U2D"      ,
    "V2D"     ,
    "LWDOWN" ,
    "RAINRATE",
    "T2D"   ,
    "Q2D"   ,
    "PSFC" ,
    "SWDOWN",
]

df_dict = {}
for _v in list_of_vars:
    df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

reng = "rasterio"
sum_stat = "mean"

for _nc_file in list_of_files:
    # _nc_file = ("nwm.t00z.medium_range.forcing.f001.conus.nc")
    _full_nc_file = (folder_prefix.joinpath(_nc_file))

    with xr.open_dataset(_full_nc_file, engine=reng) as _xds:
        for _v in list_of_vars:
            _src = _xds[_v]
            _aff2 = _src.rio.transform()
            _arr2 = _src.values[0]

            breakpoint()
            _df_zonal_stats = pd.DataFrame(zonal_stats(gpkg_divides, _arr2, affine=_aff2))
            # if adding statistics back to original GeoDataFrame
            # gdf3 = pd.concat([gpkg_divides, _df_zonal_stats], axis=1)
            df_dict[_v][_xds.time.values[0]]=_df_zonal_stats[sum_stat]

# TODO: Convert the output to CSV with something like
# `gdf3.to_csv`

