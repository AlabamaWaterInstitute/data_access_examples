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

import threading

pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "nwm_filenames")
sys.path.append(str(pkg_dir))
from listofnwmfilenames import create_file_list

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
    pkg_dir.parent, "data", "raw_data"
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


def get_forcing_dict_JL(wgt_file, filelist, var_list, var_list_out):
    t1 = time.perf_counter()
    df_by_t = []
    for _i, _nc_file in enumerate(filelist):
        with xr.open_dataset(_nc_file) as _xds:
            shp = _xds["U2D"].shape
            data_allvars = np.zeros(
                shape=(len(var_list), shp[1], shp[2]), dtype=_xds["U2D"].dtype
            )
            for var_dx, jvar in enumerate(var_list):
                data_allvars[var_dx, :, :] = np.squeeze(_xds[jvar].values)
            _df_zonal_stats = calc_zonal_stats_weights_new(data_allvars, wgt_file)
            df_by_t.append(_df_zonal_stats)
        print(
            f"Indexing catchment data progress -> {_i+1} files proccessed out of {len(filelist)}, {(_i+1)/len(filelist)*100:.2f}%",
            end="\r",
        )

    print(f"Reformating and converting data into dataframe")
    dfs = {}
    for jcat in list(df_by_t[0].keys()):
        data_catch = []
        for jt in range(len(df_by_t)):
            data_catch.append(df_by_t[jt][jcat])
        dfs[jcat] = pd.DataFrame(data_catch, columns=var_list_out)

    print(
        f"Indexing data and generating the dataframes (JL) {time.perf_counter() - t1:.2f}s"
    )

    return dfs


def wget(cmd, name, semaphore=None):
    if not semaphore == None:
        semaphore.acquire()
    resp = os.system(cmd)
    if resp > 0:
        raise Exception(f"\nwget failed! Tried: {name}\n")
    else:
        print(f"Successful download of {name}")

    if not semaphore == None:
        semaphore.release()


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
    parser.add_argument(
        dest="infile", type=str, help="A json containing user inputs to run ngen"
    )
    args = parser.parse_args()

    # Take in user config
    conf = json.load(open(args.infile))
    start_date = conf["forcing"]["start_date"]
    end_date = conf["forcing"]["end_date"]
    if "nwm_files" in conf["forcing"]:
        nwm_files = conf["forcing"]["nwm_files"]
    else:
        nwm_files = ""
    runinput = conf["forcing"]["runinput"]
    varinput = conf["forcing"]["varinput"]
    geoinput = conf["forcing"]["geoinput"]
    meminput = conf["forcing"]["meminput"]
    urlbaseinput = conf["forcing"]["urlbaseinput"]
    version   = conf["hydrofab"]["version"]
    vpu = conf["hydrofab"]["vpu"]
    ii_verbose = conf["verbose"]
    bucket_type = conf["bucket_type"]
    bucket_name = conf["bucket_name"]
    file_prefix = conf["file_prefix"]
    file_type = conf["file_type"]
    ii_cache = conf["cache"]
    if ii_cache:
        dl_threads = conf["dl_threads"]

    file_types = ["csv", "parquet"]
    assert (
        file_type in file_types
    ), f"{file_type} for file_type is not accepted! Accepted: {file_types}"

    bucket_types = ["local", "S3"]
    assert (
        bucket_type in bucket_types
    ), f"{bucket_type} for bucket_type is not accepted! Accepted: {bucket_types}"

    # TODO: Subsetting!
    #

    # Set paths and make directories if needed
    top_dir = Path(os.path.dirname(args.infile)).parent
    if not os.path.exists(CACHE_DIR):
        os.system(f"mkdir {CACHE_DIR}")
        if not os.path.exists(CACHE_DIR):
            raise Exception(f"Creating {CACHE_DIR} failed!")

    # Prep output directory
    if bucket_type == "local":
        bucket_path = Path(top_dir, file_prefix, bucket_name)
        if not os.path.exists(bucket_path):
            os.system(f"mkdir {bucket_path}")
            if not os.path.exists(bucket_path):
                raise Exception(f"Creating {bucket_path} failed!")
    elif bucket_type == "S3":
        s3 = boto3.client("s3")

    # Get nwm forcing file names
    if len(nwm_files) == 0:
        print(f"Creating list of file names to pull...")
        # n = 6
        # fcst_cycle = [n*x for x in range(24//n)]
        # lead_time  = [x+1 for x in range(n)]
        fcst_cycle = [0]

        nwm_forcing_files = create_file_list(
            runinput,
            varinput,
            geoinput,
            meminput,
            start_date,
            end_date,
            fcst_cycle,
            urlbaseinput,
        )
    else:
        print(f"Reading list of file names from {nwm_files}...")
        nwm_forcing_files = []
        with open(nwm_files, "r") as f:
            for line in f:
                nwm_forcing_files.append(line)

    # Download whole files and store locally if cache is true,
    # otherwise index remotely and save catchment based forcings
    t0 = time.perf_counter()
    if ii_cache:
        # Check to see if we have files cached, if not wget them
        local_files = []
        cmds = []
        fls = []
        for jfile in nwm_forcing_files:
            if ii_verbose:
                print(f"Looking for {jfile}")
            file_parts = Path(jfile).parts

            local_file = os.path.join(CACHE_DIR, file_parts[-1])
            local_files.append(local_file)
            if os.path.exists(local_file):
                if ii_verbose:
                    print(f"Found and using raw forcing file {local_file}")
                continue
            else:
                if ii_verbose:
                    print(f"Forcing file not found! Downloading {jfile}")
                command = f"wget -P {CACHE_DIR} -c {jfile}"
                cmds.append(command)
                fls.append(jfile)

        threads = []
        semaphore = threading.Semaphore(dl_threads)
        for i, jcmd in enumerate(cmds):
            t = threading.Thread(target=wget, args=[jcmd, fls[i], semaphore])
            t.start()
            threads.append(t)

        for jt in threads:
            jt.join()

        forcing_files = local_files  # interacting with files locally
    else:
        forcing_files = nwm_forcing_files  # interacting with files remotely

    print(f"Time to download files {time.perf_counter() - t0}")

    # Generate weight file only if one doesn't exist already
    # Very time consuming so we don't want to do this if we can avoid it
    wgt_file = os.path.join(CACHE_DIR, "weights.json")
    if not os.path.exists(wgt_file):
        # Search for geopackage that matches the requested VPU, if it exists
        gpkg = None
        for jfile in os.listdir(CACHE_DIR):
            if jfile.find(f"nextgen_{vpu}.gpkg") >= 0:
                gpkg = Path(CACHE_DIR, jfile)
                print(f"Found and using geopackge file {gpkg}")
        if gpkg == None:
            url = f"https://nextgen-hydrofabric.s3.amazonaws.com/{version}/nextgen_{vpu}.gpkg"
            command = f"wget -P {CACHE_DIR} -c {url}"
            wget(command, url)
            gpkg = Path(CACHE_DIR, f"nextgen_{vpu}.gpkg")

        print(f"Opening {gpkg}...")
        polygonfile = gpd.read_file(gpkg, layer="divides")

        ds = get_dataset(TEMPLATE_BLOB_NAME, use_cache=True)
        src = ds["RAINRATE"]

        print("Generating weights")
        t1 = time.perf_counter()
        generate_weights_file(polygonfile, src, wgt_file, crosswalk_dict_key="id")
        print(f"Generating the weights took {time.perf_counter() - t1:.2f} s")
    else:
        print(
            f"Not creating weight file! Delete this if you want to create a new one: {wgt_file}"
        )

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
    
    fd2 = get_forcing_dict_JL(
        wgt_file,
        forcing_files,
        var_list,
        var_list_out,
    )

    print(f'Writting data!')
    # Write CSVs to file
    t0 = time.perf_counter()
    write_int = 100
    for j, jcatch in enumerate(fd2.keys()):
        df = fd2[jcatch]
        splt = jcatch.split("-")

        if bucket_type == "local":
            if file_type == "csv":
                csvname = Path(bucket_path, f"cat{vpu}_{splt[1]}.csv")
                df.to_csv(csvname)
            if file_type == "parquet":
                parq_file = Path(bucket_path, f"cat{vpu}_{splt[1]}.parquet")
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
            key_name = f"{file_prefix}{csvname}"
            s3.put_object(Bucket=bucket_name, Key=key_name, Body=buf.getvalue())

        if (j + 1) % write_int == 0:
            print(
                f"{j+1} files written out of {len(fd2)}, {(j+1)/len(fd2)*100:.2f}%",
                end="\r",
            )

    print(f"{file_type} write took {time.perf_counter() - t0:.2f} s\n")

    print(
        f"\n\nDone! Catchment forcing files have been generated for VPU {vpu} in {bucket_type}\n\n"
    )
    print(f"Total run time: {time.perf_counter() - t00:.2f} s")


if __name__ == "__main__":
    main()
