# !pip install --upgrade google-api-python-client
# !pip install --upgrade google-cloud-storage

import pandas as pd
import argparse, os, json, sys
import s3fs
from pathlib import Path
import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.io import MemoryFile
from rasterio.features import rasterize
import time
import boto3
from botocore.exceptions import ClientError
from io import BytesIO, TextIOWrapper
import concurrent.futures as cf
import gzip
from datetime import datetime
import hashlib

pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "nwm_filenames")
sys.path.append(str(pkg_dir))
from listofnwmfilenames import create_file_list

retro_file = Path(pkg_dir,'listofnwmfilenamesretro.py')
from listofnwmfilenamesretro import create_file_list_retro

pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "subsetting")
sys.path.append(str(pkg_dir))
from subset import subset_upstream

TEMPLATE_BLOB_NAME = (
    "nwm.t00z.medium_range.forcing.f001.conus.nc"
)
NWM_BUCKET = "national-water-model"

# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'

def get_dataset(
        blob_name: str,
        use_cache: bool = True
) -> xr.Dataset:
    """Retrieve a blob from the data service as xarray.Dataset.
    Based largely on OWP HydroTools.
    Parameters
    ----------
    blob_name: str, required
        Name of blob to retrieve.
    use_cacahe: bool, default True
        If cache should be used.  
        If True, checks to see if file is in cache, and 
        if fetched from remote will save to cache.
    Returns
    -------
    ds : xarray.Dataset
        The data stored in the blob.
    """
    # nc_filepath = os.path.join(get_cache_dir(), blob_name)
    # make_parent_dir(nc_filepath)

    # # If the file exists and use_cache = True
    # if os.path.exists(nc_filepath) and use_cache:
        # Get dataset from cache
    ds = xr.open_dataset(
        blob_name,
        engine='h5netcdf',
    )
    return ds
    # else:
    #     # Get raw bytes
    #     raw_bytes = get_blob(blob_name)
    #     # Create Dataset
    #     ds = xr.load_dataset(
    #         MemoryFile(raw_bytes),
    #         engine='h5netcdf',
    #     )
    #     if use_cache:
    #         # Subset and cache
    #         ds["RAINRATE"].to_netcdf(
    #             nc_filepath,
    #             engine='h5netcdf',
    #         )
    #     return ds

def get_cache_dir(nwm_cache_dir: str,create: bool = True):
    if not os.path.exists(nwm_cache_dir) and create:
        os.mkdir(nwm_cache_dir)
    if not os.path.exists(nwm_cache_dir):
        raise NotADirectoryError
    return nwm_cache_dir

def make_parent_dir(filepath):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

def generate_weights_file(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
    crosswalk_dict_key: str,
):
    """Generate a weights file."""

    gdf_proj = gdf.to_crs(CONUS_NWM_WKT)

    crosswalk_dict = {}
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

def get_weights_dict(weights_file):
    # Open weights dict from pickle
    # The if statement is here to decide how to read the weight file based on local or bucket
    if type(weights_file) is dict:
        crosswalk_dict = json.loads(weights_file["Body"].read().decode())
    else:
        with open(weights_file, "r") as f:
            crosswalk_dict = json.load(f)

    return crosswalk_dict

def calc_zonal_stats_weights_new(
    src: np.ndarray,
    crosswalk_dict: dict,
) -> pd.DataFrame:
    """Calculates zonal stats"""

    nvar = src.shape[0]
    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.zeros((nvar,), dtype=np.float64)

    mean_dict = {}
    for key, value in crosswalk_dict.items():
        mean_dict[key] = np.nanmean(src[:, value[0], value[1]], axis=1)

    return mean_dict


def get_forcing_timelist(crosswalk_dict: dict, filelist: list, var_list: list, cache_dir=None, s3=None):
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

    df_by_t = []
    t = []
    for _i, _nc_file in enumerate(filelist):
        if _nc_file[:5] == "https":
            eng = "rasterio"  # switch engine for remote processing
        else:
            pass
        eng = "h5netcdf"

        # Create an S3 filesystem object
        # Open the NetCDF file using xarray's open_dataset function
        _nc_file = 's3://' + cache_dir + '/' + _nc_file.split('/')[-1]
        s3_file_obj = s3.open(_nc_file, mode='rb')

        # with xr.open_dataset(_nc_file, engine=eng) as _xds:
        with xr.open_dataset(s3_file_obj, engine=eng) as _xds:
            shp = _xds["U2D"].shape
            dtp = _xds["U2D"].dtype
            data_allvars = np.zeros(shape=(len(var_list), shp[1], shp[2]), dtype=dtp)
            for var_dx, jvar in enumerate(var_list):
                data_allvars[var_dx, :, :] = np.squeeze(_xds[jvar].values)
            _df_zonal_stats = calc_zonal_stats_weights_new(data_allvars, crosswalk_dict)
            df_by_t.append(_df_zonal_stats)
            time_splt = _xds.attrs["model_output_valid_time"].split("_")
            t.append(time_splt[0] + " " + time_splt[1])

    return df_by_t, t


def time2catchment(data_list, time_list, var_list_out, ii_collect_stats):
    """
    Convert a list of catchment dictionaries into a single dictionary of dataframes for each catchment

    Inputs:
    time_list : a list returned by get_forcing_timelist. It is assumed this list is consistent in time.
    var_list_out : a list of clomun headers for the dataframes

    Outputs:
    dfs : a dictionary of catchment based dataframes

    """
    ii_collect_stats = True

    dfs = {}
    stats_avg = []
    stats_median = []
    for jcat in list(data_list[0].keys()):
        data_catch = []
        for jt in range(len(data_list)):
            data_catch.append(data_list[jt][jcat])
        dfs[jcat] = pd.DataFrame(data_catch, columns=var_list_out)
        dfs[jcat]["time"] = time_list
        dfs[jcat] = dfs[jcat][["time"] + var_list_out]

        if ii_collect_stats:
            stacked = np.stack(data_catch)
            data_avg = np.average(stacked,axis=0)
            data_median = np.median(stacked,axis=0)
            stats_avg.append([jcat] + list(data_avg))
            stats_median.append([jcat] + list(data_median))

    if ii_collect_stats:
        avg_df = pd.DataFrame(stats_avg,columns=['catchment'] + var_list_out)
        median_df = pd.DataFrame(stats_median,columns=['catchment'] + var_list_out)

    return dfs, avg_df, median_df


def cmd(cmd):
    """
    Execute system commands

    Inputs
    cmd : the command to execute

    """
    resp = os.system(cmd)
    if resp > 0:
        raise Exception(f"\nThreaded command failed! Tried: {cmd}\n")


def locate_dl_files_threaded(
    cache_dir: str, ii_cache: bool, ii_verbose: bool, forcing_file_names: list, nthreads: int, s3=None
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
        local_file = os.path.join(cache_dir, file_parts[-1])
        ii_dl = False

        if s3 is not None:
            try:
                # if this succeeds, file has been found in bucket
                s3_obj = s3.get_object(Bucket=cache_dir,Key=local_file.split('/')[1])    
                local_files.append(jfile)            
            except:
                raise Exception(f'{local_file} not found in {cache_dir}!!')
        else:

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
                command = f"wget -P {cache_dir} -c {jfile}"
                cmds.append(command)
                dl_files.append(jfile)
                local_files.append(local_file)

    # Get files with pool
    if len(cmds) > 0:
        pool = cf.ThreadPoolExecutor(max_workers=nthreads)
        pool.map(cmd, cmds)
        pool.shutdown()

    return local_files, remote_files


def get_secret(
        secret_name : str,
        region_name : str,

):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.

    return json.loads(secret)

def prep_ngen_data(conf):
    """
    Primary function to retrieve forcing and hydrofabric data and convert it into files that can be ingested into ngen.

    Inputs: <arg1> JSON config file specifying start_date, end_date, and vpu

    Outputs: ngen catchment/nexus configs and forcing files

    Will store files in the same folder as the JSON config to run this script
    """

    datentime = datetime.utcnow().strftime("%m%d%y_%H%M%S")

    t00 = time.perf_counter()

    forcing_type = conf["forcing"]["forcing_type"]
    ii_cache = conf["forcing"]["cache"]

    start_date = conf["forcing"].get("start_date",None)
    end_date = conf["forcing"].get("end_date",None)
    runinput = conf["forcing"].get("runinput",None)
    varinput = conf["forcing"].get("varinput",None)
    geoinput = conf["forcing"].get("geoinput",None)
    meminput = conf["forcing"].get("meminput",None)
    urlbaseinput = conf["forcing"].get("urlbaseinput",None)
    nwm_file = conf["forcing"].get("nwm_file",None)
    path_override = conf["forcing"].get("path_override",None)
    fcst_cycle = conf["forcing"].get("fcst_cycle",None)
    lead_time = conf["forcing"].get("lead_time",None)
    data_type = conf["forcing"].get("data_type",None)
    object_type = conf["forcing"].get("object_type",None)

    version = conf["hydrofab"].get('version','v1.2')
    vpu = conf["hydrofab"].get("vpu")
    if vpu =='ENV': vpu = os.getenv('VPU') # allows lambdas to be unique

    catchment_subset = conf['hydrofab'].get("catch_subset")
    geopkg_file = conf["hydrofab"].get("geopkg_file")
    ii_weights_only = conf['hydrofab'].get('weights_only',False)

    storage_type = conf["storage"]["storage_type"]
    output_bucket = conf["storage"]["output_bucket"]
    output_bucket_path = conf["storage"]["output_bucket_path"]    
    cache_bucket = conf["storage"]["cache_bucket"]
    cache_bucket_path = conf["storage"]["cache_bucket_path"]
    output_file_type = conf["storage"]["output_file_type"]

    ii_verbose = conf["run"]["verbose"]    
    dl_threads = conf["run"]["dl_threads"]
    ii_collect_stats = conf["run"].get("collect_stats",True)
    secret_name = conf["run"]["secret_name"]
    region_name = conf["run"]["region_name"]

    print(f"\nWelcome to Preparing Data for NextGen-Based Simulations!\n")

    dl_time = 0
    proc_time = 0

    # configuration validation
    accepted = ['operational_archive','retrospective','from_file']
    msg = f'{forcing_type} is not a valid input for \"forcing_type\"\nAccepted inputs: {accepted}'
    assert forcing_type in accepted, msg
    file_types = ["csv", "parquet"]
    assert (
        output_file_type in file_types
    ), f"{output_file_type} for output_file_type is not accepted! Accepted: {file_types}"
    bucket_types = ["local", "s3"]
    assert (
        storage_type.lower() in bucket_types
    ), f"{storage_type} for storage_type is not accepted! Accepted: {bucket_types}"
    assert vpu is not None or geopkg_file is not None, "Need to input either vpu or geopkg_file"    
        
    if len(catchment_subset) > 0:
        vpu_or_subset = catchment_subset + "_upstream"
    else:
        vpu_or_subset = vpu

    if output_bucket_path == "":
        output_bucket_path = datentime

    if storage_type == "local":

        # Prep output directory
        top_dir = Path(os.path.dirname(__file__)).parent
        bucket_path = Path(top_dir, output_bucket_path, output_bucket)
        forcing_path = Path(bucket_path, 'forcing')  
        meta_path = Path(forcing_path, 'forcing_metadata')        
        if not os.path.exists(bucket_path):
            os.system(f"mkdir {bucket_path}")
            os.system(f"mkdir {bucket_path}")            
            os.system(f"mkdir {forcing_path}")
            os.system(f"mkdir {meta_path}")
            if not os.path.exists(bucket_path):
                raise Exception(f"Creating {bucket_path} failed!")
             
        # Prep cache directory
        cache_dir = Path(Path(os.path.dirname(__file__)).parent,cache_bucket_path)
        nwm_cache_dir = os.path.join(cache_dir, "nwm")
        if not os.path.exists(cache_dir):
            os.system(f"mkdir {cache_dir}")
            if not os.path.exists(cache_dir):
                raise Exception(f"Creating {cache_dir} failed!")   

        if len(geopkg_file) == 0: geopkg_file = os.path.join(cache_dir,"nextgen_" + vpu + ".gpkg")
        wgt_file = os.path.join(cache_dir, f"{vpu_or_subset}_weights.json")
        ii_wgt_file = os.path.exists(wgt_file)

    elif storage_type == "S3":
        cache_dir = cache_bucket

        secret = get_secret(secret_name,region_name)

        key_id = secret['aws_access_key_id']
        key = secret['aws_secret_access_key']

        fs_s3 = s3fs.S3FileSystem(anon=False, 
                          key=key_id, 
                          secret=key)

        s3 = boto3.client("s3",
                        aws_access_key_id=key_id,
                        aws_secret_access_key=key
                        )        
        
        try:
            wgt_file = s3.get_object(Bucket=cache_bucket, Key=f"{vpu_or_subset}_weights.json")
            ii_wgt_file = True
        except :
            ii_wgt_file = False
            raise NotImplementedError(f'Need to implement weight file creation in bucket')
    
        # Save config to metadata
        s3.put_object(
                Body=json.dumps(conf),
                Bucket=output_bucket,
                Key=f"{output_bucket_path}/forcing_metadata/conf.json"
            )

    # Generate weight file only if one doesn't exist already
    # TODO: This will break hard if looking for the weight file in S3, 
    # this code block assumes the weight files are local
    if not ii_wgt_file:

        # Use geopkg_file if given
        if geopkg_file is not None:
            gpkg = Path(Path(os.path.dirname(__file__)).parent,geopkg_file)
            if not gpkg.exists():
                raise Exception(f"{gpkg} doesn't exist!!")      

        elif catchment_subset is not None:
            gpkg = Path(Path(os.path.dirname(__file__)).parent,catchment_subset + '_upstream_subset.gpkg')

        # Default to geopackage that matches the requested VPU
        else:            
            gpkg = None
            for jfile in os.listdir(cache_dir):
                if jfile.find(f"nextgen_{vpu}.gpkg") >= 0:
                    gpkg = Path(cache_dir, jfile)
                    if ii_verbose:
                        print(f"Found and using geopackge file {gpkg}")
            if gpkg == None:
                url = f"https://nextgen-hydrofabric.s3.amazonaws.com/{version}/nextgen_{vpu}.gpkg"
                command = f"wget -P {cache_dir} -c {url}"
                t0 = time.perf_counter()
                cmd(command)
                dl_time += time.perf_counter() - t0
                gpkg = Path(cache_dir, f"nextgen_{vpu}.gpkg")

        if not os.path.exists(gpkg):

            # Generate geopackage through subsetting routine. This will generate ngen geojsons files 
            if catchment_subset is not None:
                if ii_verbose: print(f'Subsetting catchment with id {catchment_subset} from {gpkg}')
                if catchment_subset.find("release") >= 0:
                    try:
                        subset_upstream_prerelease(gpkg,catchment_subset)
                    except:
                        raise NotImplementedError(f"Need Tony's version of subset.py!")
                else:
                    subset_upstream(gpkg,catchment_subset)

                # geojsons will be placed in working directory. Copy them to bucket            
                if storage_type == 'local':
                    out_path = Path(bucket_path,'configs')
                    if not os.path.exists(out_path): os.system(f'mkdir {out_path}')
                    os.system(f"mv ./catchments.geojson ./nexus.geojson ./crosswalk.json ./flowpaths.geojson ./flowpath_edge_list.json {out_path}")
                else:
                    # Just don't worry about all the output files from subsetting for now
                    os.system(f"rm ./catchments.geojson ./nexus.geojson ./crosswalk.json ./flowpaths.geojson ./flowpath_edge_list.json")
                    # print(f'UNTESTED!!')
                    # files = ["./catchments.geojson",
                    #         "./nexus.geojson",
                    #         "./crosswalk.json",
                    #         "./flowpaths.geojson",
                    #         "./flowpath_edge_list.json"]
                    # buf = BytesIO()
                    # for jfile in files:
                    #     s3.put_object(
                    #     Body=json.dumps(jfile),
                    #     Bucket={output_bucket}
                    #     )

                # TODO: Create Realization file
                # TODO: Validate configs                     
        else:
            if ii_verbose:
                print(f"Opening {gpkg}...")
            t0 = time.perf_counter()
            polygonfile = gpd.read_file(gpkg, layer="divides")

            ds = get_dataset(os.path.join(nwm_cache_dir,TEMPLATE_BLOB_NAME), use_cache=True)
            # ds = get_dataset(nwm_cache_dir,TEMPLATE_BLOB_NAME, use_cache=True)
            src = ds["RAINRATE"]

            if ii_verbose:
                print("Generating weights")
            t1 = time.perf_counter()
            generate_weights_file(polygonfile, src, wgt_file, crosswalk_dict_key="id")
            if ii_verbose:
                print(f"\nGenerating the weights took {time.perf_counter() - t1:.2f} s")
            proc_time += time.perf_counter() - t0
    else:
        crosswalk_dict = get_weights_dict(wgt_file)
        if ii_verbose:
            print(
                f"Not creating weight file! Delete this if you want to create a new one: {wgt_file}"
            )

    # Exit early if we only want to calculate the weights
    if ii_weights_only:
        return

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

        if path_override is not None:
            print(f'Over riding path with {path_override}')
            files = []
            for jfile in nwm_forcing_files:
                files.append(path_override + jfile.split('/')[-1])
            nwm_forcing_files = files

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
    if ii_verbose: print(f'Locating nwm input forcing files...')
    local_nwm_files, remote_nwm_files = locate_dl_files_threaded(
        cache_dir, ii_cache, ii_verbose, nwm_forcing_files, dl_threads, s3
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

    # AWS does not support thread pools so this will have to be unthreaded...
    if ii_verbose: print(f'Extracting data...')
    if len(remote_nwm_files) > 0: remote_data_list, t_ax_remote = get_forcing_timelist(crosswalk_dict,remote_nwm_files,var_list,cache_dir,fs_s3)
    if len(local_nwm_files) > 0: local_data_list, t_ax_local = get_forcing_timelist(crosswalk_dict,local_nwm_files,var_list,cache_dir,fs_s3)  

    # Sync in time between remote and local files
    complete_data_timelist = []
    timelist = []
    for ifile in nwm_forcing_files:
        filename = Path(ifile).parts[-1]
        for j, jfile in enumerate(local_nwm_files):
            if jfile.find(filename) >= 0:
                complete_data_timelist.append(local_data_list[j])
                timelist.append(t_ax_local[j])
        for j, jfile in enumerate(remote_nwm_files):
            if jfile.find(filename) >= 0:
                complete_data_timelist.append(remote_data_list[j])
                timelist.append(t_ax_remote[j])

    # Convert time-synced list of catchment dictionaries
    # to catchment based dataframes
    if ii_verbose:
        print(f"Reformatting data into dataframes...")
    dfs, stats_avg, stats_median = time2catchment(complete_data_timelist, timelist, var_list_out, ii_collect_stats)
    proc_time += time.perf_counter() - t0

    # Write forcings to file
    t0 = time.perf_counter()
    nfiles = len(dfs)
    write_int = 1000
    catch_lim = 10
    forcing_cat_ids = []
    forcing_hashes = []
    for j, jcatch in enumerate(dfs.keys()):
        if j > catch_lim: break # TODO: remove this break for actual deployment. Just don't want to get charged for uploads.
        df = dfs[jcatch]
        cat_id = jcatch.split("-")[1]

        forcing_cat_ids.append(cat_id)

        sha256_hash = hashlib.sha256(df.to_json().encode()).hexdigest()
        forcing_hashes.append(sha256_hash)

        if storage_type == "local":
            if output_file_type == "csv":
                csvname = Path(forcing_path, f"cat{vpu}_{cat_id}.csv")
                df.to_csv(csvname, index=False)
            if output_file_type == "parquet":
                parq_file = Path(forcing_path, f"cat{vpu}_{cat_id}.parquet")
                df.to_parquet(parq_file)
        elif storage_type == "S3":
            buf = BytesIO()
            if output_file_type == "parquet":
                parq_file = f"cat{vpu}_{cat_id}.parquet"
                df.to_parquet(buf)
            elif output_file_type == "csv":
                csvname = f"cat{vpu}_{cat_id}.csv"
                df.to_csv(buf, index=False)
            buf.seek(0)
            key_name = f"{output_bucket_path}/forcing/{csvname}"
            s3.put_object(Bucket=output_bucket, Key=key_name, Body=buf.getvalue())     

        if (j + 1) % write_int == 0:
            print(
                f"{j+1} files written out of {len(dfs)}, {(j+1)/len(dfs)*100:.2f}%",
                end="\r",
            )
        if j == nfiles - 1:
            print(
                f"{j+1} files written out of {len(dfs)}, {(j+1)/len(dfs)*100:.2f}%\n",
                end="\r",
            )
    if storage_type == "S3": buf.close()
    write_time = time.perf_counter() - t0    

    total_time = time.perf_counter() - t00

    # Metadata        
    if ii_collect_stats:

        # Forcing hashes
        full_str = ''
        for x in forcing_hashes: full_str = full_str + x
        root_hash = hashlib.sha256(full_str.encode()).hexdigest()

        hash_dict = {"root_hash":root_hash,
                     "cat_ids":forcing_cat_ids,
                     "hash":forcing_hashes,                    
                     }
        
        hash_df = pd.DataFrame(dict([(key, pd.Series(value)) for key, value in hash_dict.items()]))

        t000 = time.perf_counter()
        print(f'Saving metadata...')
        # Write out a csv with script runtime stats
        nwm_file_sizes = []
        for jfile in nwm_forcing_files:
            response = s3.head_object(
                Bucket=cache_bucket,
                Key=jfile.split('/')[-1]
            )
            nwm_file_sizes.append(response['ContentLength'])
        nwm_file_size_avg = np.average(nwm_file_sizes)
        nwm_file_size_med = np.median(nwm_file_sizes)
        nwm_file_size_std = np.std(nwm_file_sizes)

        catchment_sizes = []
        zipped_sizes = []
        for j, jcatch in enumerate(dfs.keys()):     
            if j > catch_lim: break # TODO: remove this break for actual deployment. Just don't want to get charged for uploads.   
            
            # Check forcing size
            splt = jcatch.split("-")
            csvname = f"cat{vpu}_{splt[1]}.csv"
            key_name = f"{output_bucket_path}/forcing/{csvname}"
            response = s3.head_object(
                Bucket=output_bucket,
                Key=key_name
            )
            catchment_sizes.append(response['ContentLength'])

            # zip 
            zipname = csvname[:-4] + '.zip'
            buf = BytesIO()
            buf.seek(0)
            df = dfs[jcatch]
            with gzip.GzipFile(mode='w', fileobj=buf) as zipped_file:
                df.to_csv(TextIOWrapper(zipped_file, 'utf8'), index=False)
            key_name = f"{output_bucket_path}/forcing_metadata/zipped_forcing/{zipname}"
            s3.put_object(Bucket=output_bucket, Key=key_name, Body=buf.getvalue())    
            buf.close()

            # Check zipped size            
            response = s3.head_object(
                Bucket=output_bucket,
                Key=key_name
            )
            zipped_sizes.append(response['ContentLength'])

        catch_file_size_avg = np.average(catchment_sizes)
        catch_file_size_med = np.median(catchment_sizes)
        catch_file_size_std = np.std(catchment_sizes)    

        catch_file_zip_size_avg = np.average(zipped_sizes)
        catch_file_zip_size_med = np.median(zipped_sizes)
        catch_file_zip_size_std = np.std(zipped_sizes)  

        mil = 1000000

        metadata = {        
            "runtime_s"               : [round(total_time,2)],
            "nvars_intput"            : [len(var_list)],               
            "nwmfiles_input"          : [len(nwm_forcing_files)],           
            "nwm_file_size_avg_MB"    : [nwm_file_size_avg/mil],
            "nwm_file_size_med_MB"    : [nwm_file_size_med/mil],
            "nwm_file_size_std_MB"    : [nwm_file_size_std/mil],
            "catch_files_output"      : [nfiles],
            "nvars_output"            : [len(var_list_out)],
            "catch_file_size_avg_MB"  : [catch_file_size_avg/mil],
            "catch_file_size_med_MB"  : [catch_file_size_med/mil],
            "catch_file_size_std_MB"  : [catch_file_size_std/mil],
            "catch_file_zip_size_avg_MB" : [catch_file_zip_size_avg/mil],
            "catch_file_zip_size_med_MB" : [catch_file_zip_size_med/mil],
            "catch_file_zip_size_std_MB" : [catch_file_zip_size_std/mil],                                                 
        }

        # Save input config file and script commit 
        metadata_df = pd.DataFrame.from_dict(metadata)
        if storage_type == 'S3':
            # Write files to s3 bucket
            meta_path = f"{output_bucket_path}/forcing_metadata/"
            buf = BytesIO()
            csvname = f"hashes_{vpu_or_subset}.csv"
            hash_df.to_csv(buf, index=False)
            buf.seek(0)
            key_name = meta_path + csvname
            s3.put_object(Bucket=output_bucket, Key=key_name, Body=buf.getvalue())            
            csvname = f"metadata_{vpu_or_subset}.csv"
            metadata_df.to_csv(buf, index=False)
            buf.seek(0)
            key_name = meta_path + csvname
            s3.put_object(Bucket=output_bucket, Key=key_name, Body=buf.getvalue())
            csvname = f"catchments_avg.csv"
            stats_avg.to_csv(buf, index=False)
            buf.seek(0)
            key_name = meta_path + csvname
            s3.put_object(Bucket=output_bucket, Key=key_name, Body=buf.getvalue())        
            csvname = f"catchments_median.csv"
            stats_median.to_csv(buf, index=False)
            buf.seek(0)
            key_name = meta_path + csvname
            s3.put_object(Bucket=output_bucket, Key=key_name, Body=buf.getvalue())
            buf.close()
        else:
            # Write files locally
            csvname = Path(forcing_path, f"hashes_{vpu_or_subset}.csv")
            hash_df.to_csv(csvname, index=False)
            csvname = Path(forcing_path, f"metadata_{vpu_or_subset}.csv")
            metadata_df.to_csv(csvname, index=False)
            csvname = Path(forcing_path, f"catchments_avg.csv")
            stats_avg.to_csv(csvname, index=False)
            csvname = Path(forcing_path, f"catchments_median.csv")
            stats_median.to_csv(csvname, index=False)
        
        meta_time = time.perf_counter() - t000
        
    print(f"\n\n--------SUMMARY-------")
    if storage_type == "local":
        msg = f"\nData has been written locally to {bucket_path}"
    else:
        msg = f"\nData has been written to S3 bucket {output_bucket} at /{output_bucket_path}/forcing"
    msg += f"\nCheck and DL data : {dl_time:.2f}s"
    msg += f"\nProcess data      : {proc_time:.2f}s"
    msg += f"\nWrite data        : {write_time:.2f}s"
    msg += f"\nTotal time        : {total_time:.2f}s\n"
    if ii_collect_stats: msg += f"\nCollect stats     : {meta_time:.2f}s"
    
    print(msg)


if __name__ == "__main__":
    # Take in user config
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="infile", type=str, help="A json containing user inputs to run ngen"
    )
    args = parser.parse_args()

    # Extract configurations
    conf = json.load(open(args.infile))
    prep_ngen_data(conf)
