import numpy as np
import pandas as pd
import xarray as xr
import s3fs
import zarr
import fsspec


def get_nwm_data(feature_id, time_range):
    """
    Get NOAA NWM data from AWS
    It is filtered to retrieve data for a particular time range corresponding to a particular feature ID

    Arguments:
    ----------
    feature_id (int): Feature ID for which NWM data needs to be returned
    time_range (array): Time array containing start and end date as string in YYYY-MM-DD format

    Returns
    -------
    (pandas.dataframe): Pandas dataframe with NWM data for selected time range corresponding to feature ID

    """

    url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
    fs = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(url, s3=fs)
    ds_nwm_chrtout = xr.open_zarr(store, consolidated=True)




