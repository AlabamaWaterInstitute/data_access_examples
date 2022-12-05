import pandas as pd
from pyarrow.parquet import ParquetFile
import dask.dataframe as dd
import ujson
import fsspec
import xarray as xr
from kerchunk.hdf import SingleHdf5ToZarr


def get_nwm_data(files, parquet=False, dataframe=True):

    fs = fsspec.filesystem("gcs", anon=True)

    sr_h5 = []
    for f in files:
        sr_h5.append(gen_json(f, fs))

    fds = []
    for xj in sr_h5:
        backend_args = {
            "consolidated": False,
            "storage_options": {
                "fo": xj,
                # Adding these options returns a properly dimensioned but otherwise null dataframe
                # "remote_protocol": "https",
                # "remote_options": {'anon':True}
            },
        }
        fds.append(
            xr.open_dataset(
                "reference://",
                engine="zarr",
                mask_and_scale=False,
                backend_kwargs=backend_args,
            )
        )
    ds = xr.concat(fds, dim="time")

    df = ds.to_dataframe()

    if parquet:
        file_name = "parquet_med_range_mem1_channel_rt_t18z_20220901.parquet"
        df.to_parquet(
            "gs://awi-ciroh-persistent/arpita0911patel/data/"+file_name,
            engine="pyarrow", compression="snappy"
        )

    if dataframe and parquet:
        file_name = "parquet_med_range_mem1_channel_rt_t18z_20220901.parquet"
        data_parquet = dd.read_parquet(
            "gs://awi-ciroh-persistent/arpita0911patel/data/"+file_name,
            engine='pyarrow'
        )
        return df, data_parquet
    elif parquet:
        file_name = "parquet_med_range_mem1_channel_rt_t18z_20220901.parquet"
        data_parquet = dd.read_parquet(
            "gs://awi-ciroh-persistent/arpita0911patel/data/" + file_name,
            engine='pyarrow'
        )
        return data_parquet

    return df


def gen_json(u, fs, outf=None):

    fs = fsspec.filesystem("gcs", anon=True)
    so = dict(mode="rb", anon=True, default_fill_cache=False, default_cache_type="first")

    with fs.open(u, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, u, inline_threshold=300)
        if outf:
            with open(outf, "wb") as f:
                f.write(ujson.dumps(h5chunks.translate()).encode())
        else:
            return h5chunks.translate()
