# import rioxarray as rxr
import xarray as xr
import geopandas as gpd
from rasterstats import zonal_stats
# import rasterio
import pandas as pd

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

#for file in list_of_files:
fc_nc = ("nwm.t00z.medium_range.forcing.f001.conus.nc")
fc_xds = xr.open_dataset(fc_nc)

reng = "rasterio"
fc_xds = xr.open_dataset(fc_nc, engine=reng)

# Read basin boundary file
f_03 = "03w/nextgen_03W.gpkg"

gpkg_03w_divides = gpd.read_file(f_03, layer="divides")

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
    with xr.open_dataset(fc_nc, engine=reng) as _xds:
        _src = _xds[_v]
        _aff2 = _src.rio.transform()
        _arr2 = _src.values[0]
        _df_zonal_stats = pd.DataFrame(zonal_stats(gpkg_03w_divides, _arr2, affine=_aff2))
    
    df_dict[_v] = pd.DataFrame(index=_df_zonal_stats.index)
    # adding statistics back to original GeoDataFrame
    # gdf3 = pd.concat([gpkg_03w_divides, _df_zonal_stats], axis=1)
    df_dict[_v][fc_xds.time.values[0]]=_df_zonal_stats["mean"]

# TODO: Convert the output to CSV with something like
# `gdf3.to_csv`

