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
        """
        Iterator for generating dates

        Parameters
        ----------
        start_date: str, YYYYMMDD format
            Start date for getting the NWM data
        end_date: str, YYYYMMDD format
            End date for getting the NWM data

        Returns
        -------
        date
        """
        for n in range(int((end_date - start_date).days)+1):
            yield start_date + timedelta(n)

    def get_dataset(self, start_date, end_date, configuration):
        """
        Method to get the NWM dataset

        Parameters
        ----------
        start_date: str, YYYYMMDD format
            Start date for getting the NWM data
        end_date: str, YYYYMMDD format
            End date for getting the NWM data
        configuration: str
            Particular model simulation or forecast configuration

        Returns
        -------
        ds: xarray.Dataset
            The dataset containing NWM data for queried configuration from start to end date.
        """

        # Validate configuration
        if configuration not in self.configurations:
            message = f'Invalid configuration. Must select from {str(self.configurations)}'
            raise ValueError(message)

        files = self.get_files(start_date, end_date, configuration)

    def get_files(self, start_date, end_date, configuration):
        """

        Parameters
        ----------
        start_date: str, YYYYMMDD format
            Start date for getting the NWM data
        end_date: str, YYYYMMDD format
            End date for getting the NWM data
        configuration: str
            Particular model simulation or forecast configuration

        Returns
        -------
        files: list (str)
            List of files corresponding to the particular configuration for the date range specified.

        """
        files = []
        return files

    @property
    def configurations(self):
        """
        Valid configurations

        Returns
        -------
        List containing valid configurations
        """
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

