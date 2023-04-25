
# https://github.com/jameshalgren/data-access-examples/blob/DONOTMERGE_VPU16/ngen_forcing/VERYROUGH_RTI_Forcing_example.ipynb

# !pip install --upgrade google-api-python-client
# !pip install --upgrade google-cloud-storage

import pickle
import time
import pandas as pd
import argparse, os, json
import gc
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import xarray as xr
from google.cloud import storage
from rasterio.io import MemoryFile
from rasterio.features import rasterize

from nwm_filenames.listofnwmfilenames import create_file_list

TEMPLATE_BLOB_NAME = (
    "nwm.20221001/forcing_medium_range/nwm.t00z.medium_range.forcing.f001.conus.nc"
)
NWM_BUCKET = "national-water-model"

# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'

HI_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-157.42],PARAMETER["standard_parallel_1",10.0],\
PARAMETER["standard_parallel_2",30.0],PARAMETER["latitude_of_origin",20.6],UNIT["Meter",1.0]]'

PR_NWM_WKT = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-65.91],PARAMETER["standard_parallel_1",18.1],\
PARAMETER["standard_parallel_2",18.1],PARAMETER["latitude_of_origin",18.1],UNIT["Meter",1.0]]'

# paths
CACHE_DIR = Path(Path.home(), "code", "data_access_examples", "raw_forcing_data")
NWM_CACHE_DIR = os.path.join(CACHE_DIR, "nwm")
USGS_CACHE_DIR = os.path.join(CACHE_DIR, "usgs")
GEO_CACHE_DIR = os.path.join(CACHE_DIR, "geo")

NWM_CACHE_H5 = os.path.join(NWM_CACHE_DIR, "gcp_client.h5")

PARQUET_CACHE_DIR = os.path.join(CACHE_DIR, "parquet")
MEDIUM_RANGE_FORCING_PARQUET = os.path.join(PARQUET_CACHE_DIR, "forcing_medium_range")
FORCING_ANALYSIS_ASSIM_PARQUET = os.path.join(
    PARQUET_CACHE_DIR, "forcing_analysis_assim"
)
MEDIUM_RANGE_PARQUET = os.path.join(PARQUET_CACHE_DIR, "medium_range")
USGS_PARQUET = os.path.join(PARQUET_CACHE_DIR, "usgs")

HUC10_SHP_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_conus.shp")
HUC10_PARQUET_FILEPATH = os.path.join(GEO_CACHE_DIR, "wbdhu10_conus.parquet")
HUC10_MEDIUM_RANGE_WEIGHTS_FILEPATH = os.path.join(
    GEO_CACHE_DIR, "wbdhu10_medium_range_weights.pkl"
)

ROUTE_LINK_FILE = os.path.join(NWM_CACHE_DIR, "RouteLink_CONUS.nc")
ROUTE_LINK_PARQUET = os.path.join(NWM_CACHE_DIR, "route_link_conus.parquet")


def parquet_to_gdf(parquet_filepath: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(parquet_filepath)
    return gdf

def get_cache_dir(create: bool = True):
    if not os.path.exists(NWM_CACHE_DIR) and create:
        os.mkdir(NWM_CACHE_DIR)
    if not os.path.exists(NWM_CACHE_DIR):
        raise NotADirectoryError
    return NWM_CACHE_DIR

def make_parent_dir(filepath):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

def get_dataset(blob_name: str, use_cache: bool = True) -> xr.Dataset:
    """Retrieve a blob from the data service as xarray.Dataset.
    Based largely on OWP HydroTools.
    Parameters
    ----------
    blob_name: str, required
        Name of blob to retrieve.
    use_cache: bool, default True
        If cache should be used.
        If True, checks to see if file is in cache, and
        If fetched from remote, will save to cache.
    Returns
    -------
    ds : xarray.Dataset
        The data stored in the blob.
    """
    # TODO: Check to see if this does any better than kerchunk
    # the caching should help, but probably needs to be managed to function asynchronously.
    # Perhaps if the files is not cached, we can create the dataset from
    # kerchunk with a remote path and then asynchronously do a download to cache it
    # for next time. The hypothesis would be that the download speed will not be any slower than
    # just accessing the file remotely.
    nc_filepath = os.path.join(get_cache_dir(), blob_name)
    make_parent_dir(nc_filepath)

    # If the file exists and use_cache = True
    if os.path.exists(nc_filepath) and use_cache:
        # Get dataset from cache
        ds = xr.load_dataset(
            nc_filepath,
            engine="h5netcdf",
        )
        return ds
    else:
        # Get raw bytes
        raw_bytes = get_blob(blob_name)
        # Create Dataset
        ds = xr.load_dataset(
            MemoryFile(raw_bytes),
            engine="h5netcdf",
        )
        if use_cache:
            # Subset and cache
            ds["RAINRATE"].to_netcdf(
                nc_filepath,
                engine="h5netcdf",
            )
        return ds
    
def generate_weights_file(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
    crosswalk_dict_key: str,
):
    """Generate a weights file."""

    gdf_proj = gdf.to_crs(CONUS_NWM_WKT)

    crosswalk_dict = {}

    # This is a probably a really poor performing way to do this
    # TODO: Consider vectorizing -- would require digging into the
    # other end of these where we unpack the weights...  
    i = 0  
    for index, row in gdf_proj.iterrows():
        geom_rasterize = rasterize(
            [(row["geometry"], 1)],
            out_shape=src.rio.shape,
            transform=src.rio.transform(),
            all_touched=True,
            fill=0,  # IS FILL 0
            dtype="uint8",
        )
        if crosswalk_dict_key:
            crosswalk_dict[row[crosswalk_dict_key]] = np.where(geom_rasterize == 1)
        else:
            crosswalk_dict[index] = np.where(geom_rasterize == 1)
        
        if i % 100 == 0:
            perc = i/len(gdf_proj)*100
            print(f"{i}, {perc:.2f}%".ljust(40), end="\r")
            if perc > 0.01: break
        i += 1

    with open(weights_filepath, "wb") as f:
        # TODO: This is a dict of ndarrays, which could be easily stored as a set of parquet files for safekeeping.
        pickle.dump(crosswalk_dict, f)

def add_zonalstats_to_gdf_weights(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
) -> gpd.GeoDataFrame:
    """Calculates zonal stats and adds to GeoDataFrame"""

    df = calc_zonal_stats_weights(src, weights_filepath)
    gdf_map = gdf.merge(df, left_on="huc10", right_on="catchment_id")

    return gdf_map


def get_blob(blob_name: str, bucket: str = NWM_BUCKET) -> bytes:
    """Retrieve a blob from the data service as bytes.
    Based largely on OWP HydroTools.
    Parameters
    ----------
    blob_name : str, required
        Name of blob to retrieve.
    Returns
    -------
    data : bytes
        The data stored in the blob.
    """
    # Setup anonymous client and retrieve blob data
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket)
    return bucket.blob(blob_name).download_as_bytes(timeout=120)


def calc_zonal_stats_weights(
    src: xr.DataArray,
    weights_filepath: str,
) -> pd.DataFrame:
    """Calculates zonal stats"""

    # Open weights dict from pickle
    # This could probably be done once and passed as a reference.
    with open(weights_filepath, "rb") as f:
        crosswalk_dict = pickle.load(f)

    r_array = src.values[0]
    r_array[r_array == src.rio.nodata] = np.nan

    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(r_array[value])

    df = pd.DataFrame.from_dict(mean_dict, orient="index", columns=["value"])

    df.reset_index(inplace=True, names="catchment_id")

    # This should not be needed, but without memory usage grows
    del crosswalk_dict
    del f
    gc.collect()

    return df


def get_forcing_dict_RTIway(
    pickle_file,  # This would be a Feature list for parallel calling --
    # if there is a stored weights file, we use it
    # (checking for an optional flag to force re-creation of the weights...)
    folder_prefix,
    file_list,
):

    var = "RAINRATE"
    reng = "rasterio"
    filehandles = [
        xr.open_dataset(folder_prefix / f, engine=reng)[var] for f in file_list
    ]
    # filehandles = [get_dataset("data/" + f, use_cache=True) for f in file_list]
    stats = []

    for _i, f in enumerate(filehandles):
        print(f"{_i}, {round(_i/len(file_list), 2)*100}".ljust(40), end="\r")
        stats.append(calc_zonal_stats_weights(f, pickle_file))

    [f.close() for f in filehandles]
    return stats


def get_forcing_dict_RTIway2(
    pickle_file,  # This would be a Feature list for parallel calling --
    # if there is a stored weights file, we use it
    # (checking for an optional flag to force re-creation of the weights...)
    gpkg_divides,
    folder_prefix,
    filelist,
    var_list,
):
    reng = "rasterio"
    pick_val = "value"

    df_dict = {}
    dl_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)
        dl_dict[_v] = []

    # ds_list = []
    for _i, _nc_file in enumerate(filelist):
        # _nc_file = ("nwm.t00z.medium_range.forcing.f001.conus.nc")
        _full_nc_file = folder_prefix.joinpath(_nc_file)

        try:
            # with xr.open_dataset(_full_nc_file, engine=reng) as _xds:
            with xr.open_dataset(_full_nc_file) as _xds:
                # _xds = ds_list[_i]
                # _xds.rio.write_crs(rasterio.crs.CRS.from_wkt(CONUS_NWM_WKT), inplace=True)
                print(f"{_i}, {round(_i/len(filelist), 5)*100}".ljust(40), end="\r")
                for _v in var_list:
                    _src = _xds[_v]
                    _df_zonal_stats = calc_zonal_stats_weights(_src, pickle_file)
                    # if adding statistics back to original GeoDataFrame
                    # gdf3 = pd.concat([gpkg_divides, _df_zonal_stats], axis=1)
                    _df = pd.DataFrame(index=gpkg_divides.index)
                    _df[_xds.time.values[0]] = _df_zonal_stats[pick_val]
                    # TODO: This same line could add the new values directly
                    # to the same dictionary. But after adding about 100 of them,
                    # pandas starts to complain about degraded performance due to
                    # fragmentation of the dataframe. We tried it this was as a
                    # workaround, with the loop below to accomplish the concatenation.
                    dl_dict[_v].append(_df)
        except:
            print(f"No such file: {_full_nc_file}")

    for _v in var_list:
        df_dict[_v] = pd.concat(dl_dict[_v], axis=1)

    # [_xds.close() for _xds in ds_list]

    return df_dict


def main():
    """
    Primary function to retrieve hydrofabrics data and convert it into files that can be ingested into ngen. 
    Also, the forcing data is retrieved.

    Inputs: <arg1> JSON config file specifying start_date, end_date, and vpu

    Outputs: ngen catchment/nexus configs and forcing files

    Will store files in the same folder as the JSON config to run this script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(dest="infile", type=str, help="A json containing user inputs to run ngen")
    args = parser.parse_args()

    # Take in user config
    conf = json.load(open(args.infile))
    start_date = conf['forcing']['start_date']
    end_date   = conf['forcing']['end_date']
    runinput = conf['forcing']['runinput']
    varinput = conf['forcing']['varinput']
    geoinput = conf['forcing']['geoinput']
    meminput = conf['forcing']['meminput']
    urlbaseinput = conf['forcing']['urlbaseinput']

    vpu        = conf['hydrofab']['vpu']
    # Subsetting ???

    top_dir    = os.path.dirname(args.infile)
    data_dir   = os.path.join(top_dir,'raw_forcing_data')
    output_dir = os.path.join(top_dir,'catchment_forcing_data')

    if not os.path.exists(data_dir):
        os.system(f'mkdir {data_dir}')   

    if not os.path.exists(output_dir):
        os.system(f'mkdir {output_dir}')  

    # Generate list of file names to retrieve for forcing data
    n = 6
    fcst_cycle = [n*x for x in range(24//n)]
    lead_time  = [x+1 for x in range(n)]

    # TODO: These need to be in the configuration file


    print(f'Creating list of file names to pull...')
    nwm_forcing_files = create_file_list(
            runinput,
            varinput,
            geoinput,
            meminput,
            start_date,
            end_date,
            fcst_cycle,
            urlbaseinput,
            lead_time,
        )
    
    print(f'Pulling files...')
    local_files = []
    for jfile in nwm_forcing_files:
        file_parts = jfile.split('/')
        local_file = os.path.join(data_dir,file_parts[-1])
        local_files.append(local_file)
        if os.path.exists(local_file): 
            continue
        else:
            command = f'wget -P {data_dir} -c https://storage.googleapis.com/national-water-model/{jfile}'
            os.system(command)      

    # TODO wget this if needed
    gpkg = '/home/jlaser/code/data/nextgen_03W.gpkg'
    ds   = get_dataset(TEMPLATE_BLOB_NAME, use_cache=True)
    src  = ds["RAINRATE"]

    # Why are we converting to paquet and then back into geopandas dataframe?
    polygonfile = gpd.read_file(gpkg, layer="divides")
    parq_file   = os.path.join(data_dir,"ng_03.parquet")
    polygonfile.to_parquet(parq_file)
    pkl_file = os.path.join(data_dir,"weights.pkl")
    generate_weights_file(polygonfile, src, pkl_file, crosswalk_dict_key="id")
    calc_zonal_stats_weights(src, pkl_file)

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

    just_files = []
    for jfile in local_files:
        splt = jfile.split('/') # Need a way to do this that doesn't break on windows
        just_files.append(splt[-1])

    fd2 = get_forcing_dict_RTIway2(
        pkl_file,
        polygonfile,
        Path(data_dir),
        just_files,
        var_list,
    )

    # pcp_var and pcp_var2 are indentical?
    pcp_var = fd2["RAINRATE"]
    lw_var = fd2["LWDOWN"]
    sw_var = fd2["SWDOWN"]
    sp_var = fd2["PSFC"]
    tmp_var = fd2["T2D"]
    u2d_var = fd2["U2D"]
    v2d_var = fd2["V2D"]
    pcp_var2 = fd2["RAINRATE"]

    ncatchments = len(polygonfile["id"])
    for _i in range(0, ncatchments):

        pcp_var_0 = pcp_var.transpose()[_i].rename("APCP_surface")
        lw_var_0 = lw_var.transpose()[_i].rename("DLWRF_surface")
        sw_var_0 = sw_var.transpose()[_i].rename("DSWRF_surface")
        sp_var_0 = sp_var.transpose()[_i].rename("SPFH_2maboveground")
        tmp_var_0 = tmp_var.transpose()[_i].rename("TMP_2maboveground")
        u2d_var_0 = u2d_var.transpose()[_i].rename("UGRD_10maboveground")
        v2d_var_0 = v2d_var.transpose()[_i].rename("VGRD_10maboveground")
        pcp_var2_0 = pcp_var2.transpose()[_i].rename("precip_rate")  ##BROKEN!!

        d = pd.concat(
            [
                pcp_var_0,
                lw_var_0,
                sw_var_0,
                sp_var_0,
                tmp_var_0,
                u2d_var_0,
                v2d_var_0,
                pcp_var2_0,
            ],
            axis=1,
        )
        d.index.name = "time"

        id = polygonfile["id"][_i]
        splt = id.split('-')
        csvname = f"{output_dir}/cat{vpu}_{splt[1]}.csv"
        d.to_csv(csvname)

    print(f'\n\nDone! Catchment forcing files have been generated for VPU {vpu} in {output_dir}\n\n')

if __name__ == "__main__":
    main()
 