# https://github.com/jameshalgren/data-access-examples/blob/DONOTMERGE_VPU16/ngen_forcing/VERYROUGH_RTI_Forcing_example.ipynb

# !pip install --upgrade google-api-python-client
# !pip install --upgrade google-cloud-storage

import pickle
import pandas as pd
import argparse, os, json
from sys import getsizeof
import gc
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import xarray as xr
from google.cloud import storage
from rasterio.io import MemoryFile
from rasterio.features import rasterize
import time

from nwm_filenames.listofnwmfilenames import create_file_list
from ngen_forcing.process_nwm_forcing_to_ngen import *

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
CACHE_DIR = Path(Path.home(), "code", "data_access_examples", "data", "raw_forcing_data")
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

# TODO: Implemenent these function to appropriately calculate precip_rate
def rho(temp): 
    """
        Calculate water density at temperature
    """
    return 999.99399 + 0.04216485*temp - 0.007097451*(temp**2) + 0.00003509571*(temp**3) - 9.9037785E-8*(temp**4) 

def aorc_as_rate(dataFrame):
    """
        Convert kg/m^2 -> m/s
    """
    if isinstance(dataFrame.index, pd.MultiIndex):
        interval = pd.Series(dataFrame.index.get_level_values(0))
    else:
        interval = pd.Series(dataFrame.index)
    interval = ( interval.shift(-1) - interval ) / np.timedelta64(1, 's')
    interval.index = dataFrame.index
    precip_rate = ( dataFrame['APCP_surface'].shift(-1) / dataFrame['TMP_2maboveground'].apply(rho) ) / interval
    precip_rate.name = 'precip_rate'
    return precip_rate

######

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

def calc_zonal_stats_weights_new(
    src: np.ndarray,
    weights_filepath: str,   
) -> pd.DataFrame:
    """Calculates zonal stats"""

    # Open weights dict from pickle
    # This could probably be done once and passed as a reference.
    with open(weights_filepath, "rb") as f:
        crosswalk_dict = pickle.load(f)

    nvar = src.shape[0]
    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.zeros((nvar,),dtype=np.float64)  
    
    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(src[:,value[0],value[1]],axis=1)

    # This should not be needed, but without memory usage grows
    del crosswalk_dict
    del f
    gc.collect()

    return mean_dict


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

def get_forcing_dict_JL(
    pickle_file,
    folder_prefix,
    filelist,
    var_list,
    var_list_out
):
    t1 = time.perf_counter()
    df_by_t = []    
    for _i, _nc_file in enumerate(filelist):
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        print(f"Data indexing progress -> {_i}, {round(_i/len(filelist), 5)*100}".ljust(40), end="\r")
        with xr.open_dataset(_full_nc_file) as _xds:
            shp   = _xds['U2D'].shape
            data_allvars = np.zeros(
                shape=(len(var_list),shp[1],shp[2]),
                dtype=_xds['U2D'].dtype)
            for var_dx, jvar in enumerate(var_list):
                data_allvars[var_dx,:,:] = np.squeeze(_xds[jvar].values)
            _df_zonal_stats = calc_zonal_stats_weights_new(data_allvars, pickle_file)
            df_by_t.append(_df_zonal_stats)

    print(f'Reformating and converting data into dataframe', end="\r")
    dfs = {}
    for jcat in list(df_by_t[0].keys()):
        data_catch = []
        for jt in range(len(df_by_t)):     
            data_catch.append(df_by_t[jt][jcat])
        dfs[jcat] = pd.DataFrame(data_catch,columns = var_list_out)

    print(f"Indexing data and generating the dataframes (JL) {time.perf_counter() - t1:.2f} s")

    return dfs

def get_forcing_dict_RTIway2(
    pickle_file,  # This would be a Feature list for parallel calling --
    # if there is a stored weights file, we use it
    # (checking for an optional flag to force re-creation of the weights...)
    gpkg_divides,
    folder_prefix,
    filelist,
    var_list,
):
    t1=time.perf_counter()
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
    print(f"Indexing data and generating the dataframes (RTI) {time.perf_counter() - t1:.2f} s")

    return df_dict

def wget(cmd,name):
    resp = os.system(cmd)  
    if resp > 0:
        raise Exception (f'\nwget failed! Tried: {name}\n')
    else:
        print(f'Successful download of {name}')    

def main():
    """
    Primary function to retrieve hydrofabrics data and convert it into files that can be ingested into ngen. 
    Also, the forcing data is retrieved.

    Inputs: <arg1> JSON config file specifying start_date, end_date, and vpu

    Outputs: ngen catchment/nexus configs and forcing files

    Will store files in the same folder as the JSON config to run this script
    """

    t00 = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument(dest="infile", type=str, help="A json containing user inputs to run ngen")
    args   = parser.parse_args()

    # Take in user config
    conf = json.load(open(args.infile))
    start_date   = conf['forcing']['start_date']
    end_date     = conf['forcing']['end_date']
    runinput     = conf['forcing']['runinput']
    varinput     = conf['forcing']['varinput']
    geoinput     = conf['forcing']['geoinput']
    meminput     = conf['forcing']['meminput']
    urlbaseinput = conf['forcing']['urlbaseinput']
    vpu          = conf['hydrofab']['vpu']
    ii_verbose   = conf['verbose']
    output_dir   = conf['output_dir']
    ii_cache     = conf['output_dir']

    # TODO: Subsetting!
    #

    # Set paths and make directories if needed
    top_dir    = os.path.dirname(args.infile)
    if not os.path.exists(CACHE_DIR):
        os.system(f'mkdir {CACHE_DIR}') 

    # TODO: Be able to write to anywhere we want (especially AWS bucket)
    if output_dir == "local":
        output_dir = Path(top_dir,'data/catchment_forcing_data')
        if not os.path.exists(output_dir):
            os.system(f'mkdir {output_dir}')  
    else:
        raise NotImplementedError(f"{output_dir} is not an option for output_dir")

    # Generate list of file names to retrieve for forcing data
    print(f'Creating list of file names to pull...')
    n = 6
    fcst_cycle = [n*x for x in range(24//n)]
    lead_time  = [x+1 for x in range(n)]
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
    
    # Check to see if we have files cached, if not wget them
    if ii_cache:
        local_files = []
        for jfile in nwm_forcing_files:
            if ii_verbose: print(f'Looking for {jfile}')
            file_parts = Path(jfile).parts

            local_file = os.path.join(CACHE_DIR,file_parts[-1])
            local_files.append(local_file)
            if os.path.exists(local_file):             
                if ii_verbose: print(f'Found and using raw forcing file {local_file}')
                continue
            else:
                if ii_verbose: print(f'Forcing file not found! Downloading {jfile}')
                command = f'wget -P {CACHE_DIR} -c {jfile}'
                wget(command,jfile)  

        cache_files = []
        for jfile in local_files:
            splt = Path(jfile).parts
            cache_files.append(splt[-1])

        forcing_files = cache_files   # interacting with files locally
    else:
        forcing_files = nwm_forcing_files # interacting with files remotely
    
    # Do we need a parquet file?
    # parq_file   = os.path.join(CACHE_DIR,"ng_03.parquet")
    # polygonfile.to_parquet(parq_file)

    # Generate weight file only if one doesn't exist already
    # Very time consuming so we don't want to do this if we can avoid it
    pkl_file = os.path.join(CACHE_DIR,"weights.pkl")
    if not os.path.exists(pkl_file):
        # Search for geopackage that matches the requested VPU, if it exists
        gpkg = None
        for jfile in os.listdir(os.path.join(top_dir,'data')):
            if jfile.find(vpu) >= 0:
                gpkg = Path(top_dir,"data",jfile)
                print(f'Found and using geopackge file {gpkg}')
        if gpkg == None:
            url = f'https://nextgen-hydrofabric.s3.amazonaws.com/05_nextgen/nextgen_{vpu}.gpkg'
            command = f'wget -P {CACHE_DIR} -c {url}'
            wget(command,url)

        print(f'Opening {gpkg}...')
        polygonfile = gpd.read_file(gpkg, layer="divides")

        ds   = get_dataset(TEMPLATE_BLOB_NAME, use_cache=True)
        src  = ds["RAINRATE"]

        print("Generating weights")
        t1 = time.perf_counter()
        generate_weights_file(polygonfile, src, pkl_file, crosswalk_dict_key="id")
        print(f"Generating the weights took {time.perf_counter() - t1:.2f} s")
    else:
        print(f"Not creating weight file! Delete this if you want to create a new one: {pkl_file}")

    var_list = [
        "U2D",
        "V2D",
        "LWDOWN",
        "RAINRATE",
        "RAINRATE",
        "T2D",
        "PSFC",
        "SWDOWN",
    ]

    var_list_out = [
        "UGRD_10maboveground",
        "VGRD_10maboveground",
        "DLWRF_surface",
        "APCP_surface",
        "precip_rate",    # BROKEN (Identical to APCP!) 
        "TMP_2maboveground",
        "SPFH_2maboveground",
        "DSWRF_surface",
    ]
    
    fd2 = get_forcing_dict_JL(
        pkl_file,
        CACHE_DIR,
        forcing_files,
        var_list,
        var_list_out,
    )

    t0 = time.perf_counter()
    for jcatch in fd2.keys(): 
        arr = fd2[jcatch]  
        splt = jcatch.split('-')
        csvname = f"{output_dir}/cat{vpu}_{splt[1]}.csv"
        arr.to_csv(csvname)

    print(f'JL write took {time.perf_counter() - t0:.2f} s')
    print(f'\n\nDone! Catchment forcing files have been generated for VPU {vpu} in {output_dir}\n\n')
    print(f'Total run time: {time.perf_counter() - t00:.2f} s')

if __name__ == "__main__":
    main()
 