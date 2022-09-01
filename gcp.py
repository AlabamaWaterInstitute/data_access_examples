"""

This module contains classes to retrieve National Water Model (NWM) data
from Google Cloud Platform using kerchunk.

NWM Data: https://console.cloud.google.com/marketplace/details/noaa-public/national-water-model

Author: Karnesh Jain

"""

import xarray as xr
import fsspec
from datetime import datetime, timedelta
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr


class NWMData:
    """
    The NWMData class provides methods for querying NWM data on Google Cloud Platform.
    """

    def __init__(self, bucket_name = 'national-water-model'):
        """
        Instantiate NWMData class

        Parameters
        ----------
        bucket_name : str, default: 'national-water-model' (Google Cloud Bucket)

        Returns
        -------
        A NWM data object.
        """

        # set bucket_name
        self.bucket_name = bucket_name

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)+1):
            yield start_date + timedelta(n)

    @property
    def configurations(self) -> list:
        # Valid configurations compatible with this client
        # TODO Find crosswalk for Alaska
        return [
            'analysis_assim',
            'analysis_assim_extend',
            'analysis_assim_hawaii',
            'analysis_assim_long',
            'analysis_assim_puertorico',
            'long_range_mem1',
            'long_range_mem2',
            'long_range_mem3',
            'long_range_mem4',
            'medium_range_mem1',
            'medium_range_mem2',
            'medium_range_mem3',
            'medium_range_mem4',
            'medium_range_mem5',
            'medium_range_mem6',
            'medium_range_mem7',
            'short_range',
            'short_range_hawaii',
            'short_range_puertorico',
            'analysis_assim_no_da',
            'analysis_assim_extend_no_da',
            'analysis_assim_hawaii_no_da',
            'analysis_assim_long_no_da',
            'analysis_assim_puertorico_no_da',
            'medium_range_no_da',
            'short_range_hawaii_no_da',
            'short_range_puertorico_no_da'
        ]

