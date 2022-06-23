# Script to access NOAA NWM data from AWS S3
# Author: Karnesh Jain

import pandas as pd
import xarray as xr
import s3fs
from datetime import datetime
import plotly.express as px


def get_nwm_data(feature_id, start_date, end_date):
    """
    Get NOAA NWM data from AWS
    It is filtered to retrieve data for a particular time range corresponding to a feature ID

    Arguments:
    ----------
    feature_id (int): Feature ID for which NWM data needs to be returned
    start_date (str): Start date in "YYYY-MM-DD" format
    end_date (str): End date in "YYYY-MM-DD" format

    Returns
    -------
    (pandas.dataframe): Pandas dataframe with NWM data for user queried time range and feature ID

    """

    # check start and end date format
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Start and end date should have YYYY-MM-DD format")

    url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
    fs = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(url, s3=fs)
    ds_nwm_chrtout = xr.open_zarr(store, consolidated=True)

    ds_nwm_filtered = ds_nwm_chrtout.sel(feature_id=feature_id, time=slice(start_date, end_date))

    df_nwm_chrtout = ds_nwm_filtered.to_dataframe()

    return df_nwm_chrtout


def plot_nwm_data(*dfs_nwm):
    """
    Get NOAA NWM data from AWS
    It is filtered to retrieve data for a particular time range corresponding to a feature ID

    Arguments:
    ----------
    dfs_nwm (pandas_dataframe): NWM data from get_nwm_data method

    Returns
    -------
    Geographic plot with time slider

    """
    df_nwm = pd.DataFrame()

    for df in dfs_nwm:
        df = df.resample('D').mean()
        if df_nwm.empty:
            df_nwm = df
        else:
            df_nwm = pd.concat([df_nwm, df], ignore_index=False)

    df_nwm = df_nwm.dropna().reset_index()
    df_nwm = df_nwm.sort_values(by='time')
    df_nwm['time'] = df_nwm.time.apply(lambda x: x.date()).apply(str)  # convert timestamp to a string

    fig = px.scatter_mapbox(df_nwm, lat="latitude", lon="longitude",
                             animation_frame='time', animation_group='feature_id',
                             color="streamflow", size="streamflow",
                             zoom=3, height=500, hover_name='feature_id',
                             hover_data=['streamflow'])

    fig.update_layout(mapbox_style="white-bg",
                       mapbox_layers=[
                           {
                               "below": 'traces',
                               "sourcetype": "raster",
                               "sourceattribution": "USGS",
                               "source": [
                                   "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"
                               ]
                           }
                       ])

    fig.show()
