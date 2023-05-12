# TODO NOTE a lot of this code is borrowed from https://github.com/RTIInternational/hydro-evaluation
# In the future, import this package
# https://github.com/jameshalgren/data-access-examples/blob/DONOTMERGE_VPU16/ngen_forcing/VERYROUGH_RTI_Forcing_example.ipynb

# !pip install --upgrade google-api-python-client
# !pip install --upgrade google-cloud-storage

import pandas as pd
import argparse, os, json, sys
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
import boto3
from io import BytesIO
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import concurrent.futures as cf

pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "nwm_filenames")
sys.path.append(str(pkg_dir))
from listofnwmfilenames import create_file_list

retro_file = Path(pkg_dir,'listofnwmfilenamesretro.py')
ii_retro = False
if retro_file.exists():
    ii_retro = True
    from listofnwmfilenamesretro import create_file_list_retro

pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "subsetting")
sys.path.append(str(pkg_dir))
from subset import subset_upstream, subset_upstream_prerelease

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

# TODO Make CACHE_DIR configurable
CACHE_DIR = Path(
    pkg_dir.parent, "data", "cache"
)  # Maybe this should have a date attached to the name

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
    # Perhaps if theget_dataset files is not cached, we can create the dataset from
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


# TODO: Import this instead!
# Adapted from https://github.com/RTIInternational/hydro-evaluation/blob/dev-denno-4-1/src/evaluation/loading/generate_weights.py
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
            perc = i / len(gdf_proj) * 100
            print(f"{i}, {perc:.2f}%".ljust(40), end="\r")
        i += 1

    weights_json = json.dumps(
        {k: [x.tolist() for x in v] for k, v in crosswalk_dict.items()}
    )
    with open(weights_filepath, "w") as f:
        f.write(weights_json)


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
    with open(weights_filepath, "r") as f:
        crosswalk_dict = json.load(f)

    nvar = src.shape[0]
    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.zeros((nvar,), dtype=np.float64)

    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(src[:, value[0], value[1]], axis=1)

    # This should not be needed, but without memory usage grows
    del crosswalk_dict
    del f
    gc.collect()

    return mean_dict


def get_forcing_timelist(wgt_file: str, filelist: list, var_list: list):
    """
    General function to read either remote or local nwm forcing files.

    Inputs:
    wgt_file: a path to the weights json,
    filelist: list of filenames (urls for remote, local paths otherwise),
    var_list: list (list of variable names to read),
    jt: the index to place the file. This is used to ensure elements increase in time, regardless of thread number,

    Outputs:
    df_by_t : (returned for local files) a list (in time) of forcing data. Note that this list may not be consistent in time
    t : model_output_valid_time for each

    """

    t1 = time.perf_counter()
    df_by_t = []
    t = []
    for _i, _nc_file in enumerate(filelist):
        if _nc_file[:5] == "https":
            eng = "rasterio"  # switch engine for remote processing
        else:
            eng = "h5netcdf"
        with xr.open_dataset(_nc_file, engine=eng) as _xds:
            shp = _xds["U2D"].shape
            dtp = _xds["U2D"].dtype
            data_allvars = np.zeros(shape=(len(var_list), shp[1], shp[2]), dtype=dtp)
            for var_dx, jvar in enumerate(var_list):
                data_allvars[var_dx, :, :] = np.squeeze(_xds[jvar].values)
            _df_zonal_stats = calc_zonal_stats_weights_new(data_allvars, wgt_file)
            df_by_t.append(_df_zonal_stats)
            time_splt = _xds.attrs["model_output_valid_time"].split("_")
            t.append(time_splt[0] + " " + time_splt[1])

    return df_by_t, t


def time2catchment(data_list, time_list, var_list_out):
    """
    Convert a list of catchment dictionaries into a single dictionary of dataframes for each catchment

    Inputs:
    time_list : a list returned by get_forcing_timelist. It is assumed this list is consistent in time.
    var_list_out : a list of clomun headers for the dataframes

    Outputs:
    dfs : a dictionary of catchment based dataframes

    """

    dfs = {}
    for jcat in list(data_list[0].keys()):
        data_catch = []
        for jt in range(len(data_list)):
            data_catch.append(data_list[jt][jcat])
        dfs[jcat] = pd.DataFrame(data_catch, columns=var_list_out)
        dfs[jcat]["time"] = time_list
        dfs[jcat] = dfs[jcat][["time"] + var_list_out]

    return dfs


def cmd(cmd):
    """
    Execute system commands

    Inputs
    cmd : the command to execute

    """
    resp = os.system(cmd)
    if resp > 0:
        raise Exception(f"\Threaded command failed! Tried: {cmd}\n")


def locate_dl_files_threaded(
    ii_cache: bool, ii_verbose: bool, forcing_file_names: list, nthreads: int
):
    """
    Look for forcing files locally, if found, will apend to local file list for local processing
    If not found and if we do not wish to cache, append to remote files for remote processing
    If not found and if we do wish to cache, append to local file list for local processing and perform a threaded download

    Inputs:
    ii_cache : user-defined caching bool
    ii_verbose : user-defined verbosity bool
    forcing_file_names : a list of forcing files names
    nthreads : user-defined maximum number of threads

    Outputs:
    local_files : list of paths to the local files. Note that even if ii_cache if false, if a file is found locally, it will be used.
    remote_files : list of urls to the remote files.
    """

    local_files = []
    remote_files = []
    dl_files = []
    cmds = []
    for jfile in forcing_file_names:
        file_parts = Path(jfile).parts
        local_file = os.path.join(CACHE_DIR, file_parts[-1])
        ii_dl = False

        # decide whether to use local file, download it, or index it remotely
        if os.path.exists(local_file):
            # Check to make sure file is not broken
            try:
                with xr.open_dataset(local_file, engine="h5netcdf") as _xds:
                    pass
                if ii_cache:
                    if ii_verbose:
                        print(f"Found and using local raw forcing file {local_file}")
                else:
                    if ii_verbose:
                        print(
                            f"CACHE OPTION OVERRIDE : Found and using local raw forcing file {local_file}"
                        )
                local_files.append(local_file)
            except:
                if ii_cache:
                    if ii_verbose:
                        print(f"{local_file} is broken! Will Download")
                    ii_dl = True
                else:
                    if ii_verbose:
                        print(f"{local_file} is broken! Will index remotely")
                    remote_files.append(jfile)

        elif not os.path.exists(local_file) and not ii_cache:
            # If file is not found locally, and we don't want to cache it, append to remote file list
            remote_files.append(jfile)
        elif not os.path.exists(local_file) and ii_cache:
            ii_dl = True

        if ii_dl:
            # Download file
            if ii_verbose:
                print(f"Forcing file not found! Downloading {jfile}")
            command = f"wget -P {CACHE_DIR} -c {jfile}"
            cmds.append(command)
            dl_files.append(jfile)
            local_files.append(local_file)

    # Get files with pool
    if len(cmds) > 0:
        pool = cf.ThreadPoolExecutor(max_workers=nthreads)
        pool.map(cmd, cmds)
        pool.shutdown()

    return local_files, remote_files

def threaded_data_extract(files,nthreads,ii_verbose,wgt_file,var_list):
    """
    Sets up the thread pool for get_forcing_timelist and returns the data and time axis ordered in time
    
    """
    pool = cf.ThreadPoolExecutor(max_workers=nthreads)
    arg0 = []
    arg1 = []
    arg2 = []
    for i in range(len(files)):
        arg0.append(wgt_file)
        arg1.append([files[i]])
        arg2.append(var_list)

    results = pool.map(get_forcing_timelist, arg0, arg1, arg2)

    data_list = []
    for jres in results:
        data_list.append(jres)

    # Build time axis
    t_ax_local = []
    for i in range(len(files)):
        t_ax_local.append(data_list[i][1])   

    return  data_list, t_ax_local


def main():
    """
    Primary function to retrieve forcing and hydrofabric data and convert it into files that can be ingested into ngen.

    Inputs: <arg1> JSON config file specifying start_date, end_date, and vpu

    Outputs: ngen catchment/nexus configs and forcing files

    Will store files in the same folder as the JSON config to run this script
    """

    t00 = time.perf_counter()

    # Take in user config
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="infile", type=str, help="A json containing user inputs to run ngen"
    )
    args = parser.parse_args()

    # Extract configurations
    conf = json.load(open(args.infile))
    forcing_type = conf["forcing"]["forcing_type"]
    if not ii_retro and forcing_type == "retrospective":
        raise NotImplementedError("Need listofnwmfilenamesretro for this!")
    ii_cache = conf["forcing"]["cache"]

    start_date = conf["forcing"].get("start_date",None)
    end_date = conf["forcing"].get("end_date",None)
    runinput = conf["forcing"].get("runinput",None)
    varinput = conf["forcing"].get("varinput",None)
    geoinput = conf["forcing"].get("geoinput",None)
    meminput = conf["forcing"].get("meminput",None)
    urlbaseinput = conf["forcing"].get("urlbaseinput",None)
    nwm_file = conf["forcing"].get("nwm_file",None)
    fcst_cycle = conf["forcing"].get("fcst_cycle",None)
    lead_time = conf["forcing"].get("lead_time",None)
    data_type = conf["forcing"].get("data_type",None)
    object_type = conf["forcing"].get("object_type",None)

    version = conf["hydrofab"].get('version','v1.2')
    vpu = conf["hydrofab"].get("vpu")
    catchment_subset = conf['hydrofab'].get("catch_subset")
    geopkg_file = conf["hydrofab"].get("geopkg_file")
    ii_weights_only = conf['hydrofab'].get('weights_only',False)

    bucket_type = conf["storage"]["bucket_type"]
    bucket_name = conf["storage"]["bucket_name"]
    file_prefix = conf["storage"]["file_prefix"]
    file_type = conf["storage"]["file_type"]

    ii_verbose = conf["run"]["verbose"]    
    dl_threads = conf["run"]["dl_threads"]
    proc_threads = conf["run"]["proc_threads"]

    print(f"\nWelcome to Preparing Data for NextGen-Based Simulations!\n")

    dl_time = 0
    proc_time = 0

    # configuration validation
    accepted = ['operational_archive','retrospective','from_file']
    msg = f'{forcing_type} is not a valid input for \"forcing_type\"\nAccepted inputs: {accepted}'
    assert forcing_type in accepted, msg
    file_types = ["csv", "parquet"]
    assert (
        file_type in file_types
    ), f"{file_type} for file_type is not accepted! Accepted: {file_types}"
    bucket_types = ["local", "S3"]
    assert (
        bucket_type in bucket_types
    ), f"{bucket_type} for bucket_type is not accepted! Accepted: {bucket_types}"
    assert vpu is not None or geopkg_file is not None, "Need to input either vpu or geopkg_file"

    # Set paths and make directories if needed
    top_dir = Path(os.path.dirname(args.infile)).parent
    if not os.path.exists(CACHE_DIR):
        os.system(f"mkdir {CACHE_DIR}")
        if not os.path.exists(CACHE_DIR):
            raise Exception(f"Creating {CACHE_DIR} failed!")

    # Prep output directory
    if bucket_type == "local":
        bucket_path = Path(top_dir, file_prefix, bucket_name)
        forcing_path = Path(bucket_path, 'forcing')
        if not os.path.exists(bucket_path):
            os.system(f"mkdir {bucket_path}")            
            os.system(f"mkdir {forcing_path}")
            if not os.path.exists(bucket_path):
                raise Exception(f"Creating {bucket_path} failed!")
    elif bucket_type == "S3":
        s3 = boto3.client("s3")

    # Generate weight file only if one doesn't exist already
    if catchment_subset is not None:
        wgt_file = os.path.join(CACHE_DIR, f"{catchment_subset}_upstream_weights.json")
    else:
        wgt_file = os.path.join(CACHE_DIR, f"{vpu}_weights.json")
    if not os.path.exists(wgt_file):

        # Use geopkg_file if given
        if geopkg_file is not None:
            gpkg = Path(Path(os.path.dirname(__file__)).parent,geopkg_file)
            if not gpkg.exists:
                raise Exception(f"{gpkg} doesn't exist!!")      

        elif catchment_subset is not None:
            gpkg = Path(Path(os.path.dirname(__file__)).parent,catchment_subset + '_upstream_subset.gpkg')

        # Default to geopackage that matches the requested VPU
        else:            
            gpkg = None
            for jfile in os.listdir(CACHE_DIR):
                if jfile.find(f"nextgen_{vpu}.gpkg") >= 0:
                    gpkg = Path(CACHE_DIR, jfile)
                    if ii_verbose:
                        print(f"Found and using geopackge file {gpkg}")
            if gpkg == None:
                url = f"https://nextgen-hydrofabric.s3.amazonaws.com/{version}/nextgen_{vpu}.gpkg"
                command = f"wget -P {CACHE_DIR} -c {url}"
                t0 = time.perf_counter()
                cmd(command)
                dl_time += time.perf_counter() - t0
                gpkg = Path(CACHE_DIR, f"nextgen_{vpu}.gpkg")

        if not os.path.exists(gpkg):

            # Generate geopackage through subsetting routine. This will generate ngen geojsons files 
            if catchment_subset is not None:
                if ii_verbose: print(f'Subsetting catchment with id {catchment_subset} from {gpkg}')
                if catchment_subset.find("release"):
                    try:
                        subset_upstream_prerelease(gpkg,catchment_subset)
                    except:
                        raise NotImplementedError(f"Need Tony's version of subset.py!")
                else:
                    subset_upstream(gpkg,catchment_subset)

                # geojsons will be placed in working directory. Copy them to bucket            
                if bucket_type == 'local':
                    out_path = Path(bucket_path,'configs')
                    if not os.path.exists(out_path): os.system(f'mkdir {out_path}')
                    os.system(f"mv ./catchments.geojson ./nexus.geojson ./crosswalk.json ./flowpaths.geojson ./flowpath_edge_list.json {out_path}")
                else:
                    print(f'UNTESTED!!')
                    files = ["./catchments.geojson" "./nexus.geojson" "./crosswalk.json" "./flowpaths.geojson" "./flowpath_edge_list.json"]
                    buf = BytesIO()
                    for jfile in files:
                        s3.put_object(
                        Body=json.dumps(jfile),
                        Bucket={bucket_name}
                        )

                # TODO: Create Realization file
                # TODO: Validate configs                     
        else:
            if ii_verbose:
                print(f"Opening {gpkg}...")
            t0 = time.perf_counter()
            polygonfile = gpd.read_file(gpkg, layer="divides")

            ds = get_dataset(TEMPLATE_BLOB_NAME, use_cache=True)
            src = ds["RAINRATE"]

            if ii_verbose:
                print("Generating weights")
            t1 = time.perf_counter()
            generate_weights_file(polygonfile, src, wgt_file, crosswalk_dict_key="id")
            if ii_verbose:
                print(f"\nGenerating the weights took {time.perf_counter() - t1:.2f} s")
            proc_time += time.perf_counter() - t0
    else:
        if ii_verbose:
            print(
                f"Not creating weight file! Delete this if you want to create a new one: {wgt_file}"
            )

    # Exit early if we only want to calculate the weights
    if ii_weights_only:
        exit

    # Get nwm forcing file names
    t0 = time.perf_counter()
    if not forcing_type == 'from_file':

        if forcing_type == "operational_archive":
            nwm_forcing_files = create_file_list(
                runinput,
                varinput,
                geoinput,
                meminput,
                start_date,
                end_date,
                fcst_cycle,
                urlbaseinput,
                lead_time
            )
        elif forcing_type == "retrospective":
            nwm_forcing_files = create_file_list_retro(
                    runinput,
                    varinput,
                    geoinput,
                    meminput,
                    start_date + "0000", # Hack
                    end_date + "0000", # Hack
                    fcst_cycle,
                    urlbaseinput,
                    lead_time,
                    data_type,
                    object_type
                )
            nwm_forcing_files = nwm_forcing_files[0]

    else:
        nwm_forcing_files = []
        with open(nwm_file, "r") as f:
            for line in f:
                nwm_forcing_files.append(line)
    if ii_verbose:
        print(f"Raw file names:")
        for jfile in nwm_forcing_files:
            print(f"{jfile}")

    proc_time += time.perf_counter() - t0

    # This will look for local raw forcing files and download them if needed
    t0 = time.perf_counter()
    local_nwm_files, remote_nwm_files = locate_dl_files_threaded(
        ii_cache, ii_verbose, nwm_forcing_files, dl_threads
    )
    dl_time += time.perf_counter() - t0

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
        "precip_rate",  # BROKEN (Identical to APCP!)
        "TMP_2maboveground",
        "SPFH_2maboveground",
        "DSWRF_surface",
    ]

    t0 = time.perf_counter()
    # Index remote files with threads
    if len(remote_nwm_files) > 0:
        if ii_verbose:
            print(
                f"Performing threaded remote data extraction with {proc_threads} workers..."
            )
        remote_data_list, t_ax_remote = threaded_data_extract(remote_nwm_files,proc_threads,ii_verbose,wgt_file,var_list)

    # Index local files with threads
    if len(local_nwm_files) > 0:
        if ii_verbose:
            print(
                f"Performing threaded local data extraction with {proc_threads} workers..."
            )
        local_data_list, t_ax_local = threaded_data_extract(local_nwm_files,proc_threads,ii_verbose,wgt_file,var_list)      

    # Sync in time between remote and local files
    complete_data_timelist = []
    timelist = []
    for ifile in nwm_forcing_files:
        filename = Path(ifile).parts[-1]
        for j, jfile in enumerate(local_nwm_files):
            if jfile.find(filename) >= 0:
                complete_data_timelist.append(local_data_list[j][0][0])
                timelist.append(t_ax_local[j])
        for j, jfile in enumerate(remote_nwm_files):
            if jfile.find(filename) >= 0:
                complete_data_timelist.append(remote_data_list[j][0][0])
                timelist.append(t_ax_remote[j])

    # Convert time-synced list of catchment dictionaries
    # to catchment based dataframes
    if ii_verbose:
        print(f"Reformatting data into dataframes...")
    dfs = time2catchment(complete_data_timelist, timelist, var_list_out)
    proc_time += time.perf_counter() - t0

    # Write to file
    t0 = time.perf_counter()
    nfiles = len(dfs)
    write_int = 1000
    for j, jcatch in enumerate(dfs.keys()):
        df = dfs[jcatch]
        splt = jcatch.split("-")

        if bucket_type == "local":
            if file_type == "csv":
                csvname = Path(forcing_path, f"cat{vpu}_{splt[1]}.csv")
                df.to_csv(csvname, index=False)
            if file_type == "parquet":
                parq_file = Path(forcing_path, f"cat{vpu}_{splt[1]}.parquet")
                df.to_parquet(parq_file)
        elif bucket_type == "S3":
            buf = BytesIO()
            if file_type == "parquet":
                parq_file = f"cat{vpu}_{splt[1]}.parquet"
                df.to_parquet(buf)
            elif file_type == "csv":
                csvname = f"cat{vpu}_{splt[1]}.csv"
                df.to_csv(buf, index=False)
            buf.seek(0)
            key_name = f"{file_prefix}/forcing/{csvname}"
            s3.put_object(Bucket=bucket_name, Key=key_name, Body=buf.getvalue())

        if (j + 1) % write_int == 0:
            print(
                f"{j+1} files written out of {len(dfs)}, {(j+1)/len(dfs)*100:.2f}%",
                end="\r",
            )
        if j == nfiles - 1:
            print(
                f"{j+1} files written out of {len(dfs)}, {(j+1)/len(dfs)*100:.2f}%",
                end="\r",
            )
    write_time = time.perf_counter() - t0
    total_time = time.perf_counter() - t00

    print(f"\n\n--------SUMMARY-------")
    if bucket_type == "local":
        msg = f"\nData has been written locally to {bucket_path}"
    else:
        msg = f"\nData has been written to S3 bucket {bucket_name} at {file_prefix}"
    msg += f"\nCheck and DL data : {dl_time:.2f}s"
    msg += f"\nProcess data      : {proc_time:.2f}s"
    msg += f"\nWrite data        : {write_time:.2f}s"
    msg += f"\nTotal time        : {total_time:.2f}s\n"
    print(msg)


if __name__ == "__main__":
    main()
