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

    def __init__(self, bucket_name='national-water-model'):
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
        for n in range(int((end_date - start_date).days) + 1):
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

        open_files = fsspec.open_files(files)
        out_zarr = []
        for file in open_files:
            with file as f:
                out_zarr.append(SingleHdf5ToZarr(f, file.path).translate())

        mzz = MultiZarrToZarr(out_zarr,
                              remote_protocol='gcs',
                              concat_dims=['time', 'reference_time'],
                              )

        combined_dataset = mzz.translate()

        backend_args = {"consolidated": False,
                        "storage_options": {"fo": combined_dataset,
                                            "remote_protocol": "gcs",
                                            "remote_options": {'anon': True}}}

        ds = xr.open_dataset(
            "reference://", engine="zarr",
            backend_kwargs=backend_args
        )

        return ds

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
        fs = fsspec.filesystem('gcs', anon=True)
        files = []

        start = datetime.strptime(start_date, '%Y%m%d').date()
        end = datetime.strptime(end_date, '%Y%m%d').date()

        for date in self.daterange(start, end):
            date_str = date.strftime('%Y%m%d')
            for time in self.configurations[configuration]['t']:
                if 'analysis' in configuration:
                    for tm in self.configurations[configuration]['tm']:
                        files.append(f'gcs://{self.bucket_name}/nwm.{date_str}/{configuration}/nwm.t{time:02d}z.'
                                     f'{self.configurations[configuration]["fname_config"]}.channel_rt.tm{tm:02d}.'
                                     f'conus.nc')
                else:
                    for f in self.configurations[configuration]['f']:
                        files.append(f'gcs://{self.bucket_name}/nwm.{date_str}/{configuration}/nwm.t{time:02d}z.'
                                     f'{self.configurations[configuration]["fname_config"]}.channel_rt_1.f{f:03d}.'
                                     f'conus.nc')

        return files

    @property
    def configurations(self):
        """
        Valid configurations

        Returns
        -------
        Dictionary containing valid configurations with forcast/analysis time details
        """
        return {
            'analysis_assim': {'t': range(0, 24), 'tm': range(0, 2), 'var': 'channel_rt',
                               'fname_config': 'analysis_assim'},
            'analysis_assim_extend': {'t': range(0, 24), 'tm': range(0, 2), 'var': 'channel_rt',
                                      'fname_config': 'analysis_assim'},
            'analysis_assim_hawaii': {'t': range(0, 24), 'tm': range(0, 2), 'var': 'channel_rt',
                                      'fname_config': 'analysis_assim'},
            'analysis_assim_long': {'t': range(0, 24), 'tm': range(0, 2), 'var': 'channel_rt',
                                    'fname_config': 'analysis_assim'},
            'analysis_assim_puertorico': {'t': range(0, 24), 'tm': range(0, 2), 'var': 'channel_rt',
                                          'fname_config': 'analysis_assim'},
            'long_range_mem1': {'t': range(0, 24, 6), 'f': range(0, 720, 6), 'var': 'channel_rt_1',
                                'fname_config': 'long_range'},
            'long_range_mem2': {'t': range(0, 24, 6), 'f': range(0, 720, 6), 'var': 'channel_rt_1',
                                'fname_config': 'long_range'},
            'long_range_mem3': {'t': range(0, 24, 6), 'f': range(0, 720, 6), 'var': 'channel_rt_1',
                                'fname_config': 'long_range'},
            'long_range_mem4': {'t': range(0, 24, 6), 'f': range(0, 720, 6), 'var': 'channel_rt_1',
                                'fname_config': 'long_range'},
            'medium_range_mem1': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'medium_range_mem2': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'medium_range_mem3': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'medium_range_mem4': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'medium_range_mem5': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'medium_range_mem6': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'medium_range_mem7': {'t': range(0, 24, 6), 'f': range(0, 240, 6), 'var': 'channel_rt',
                                  'fname_config': 'medium_range'},
            'short_range': {'t': range(0, 24, 1), 'f': range(0, 2), 'var': 'channel_rt', 'fname_config': 'short_range'},
            'short_range_hawaii': {'t': range(0, 24, 1), 'f': range(0, 2), 'var': 'channel_rt',
                                   'fname_config': 'short_range'},
            'short_range_puertorico': {'t': range(0, 24, 1), 'f': range(0, 2), 'var': 'channel_rt',
                                       'fname_config': 'short_range'},
            'analysis_assim_no_da': {'t': range(0, 24, 1), 'tm': range(0, 2), 'var': 'channel_rt',
                                     'fname_config': 'analysis_assim'},
            'analysis_assim_extend_no_da': {'t': range(0, 24, 1), 'tm': range(0, 2), 'var': 'channel_rt',
                                            'fname_config': 'analysis_assim'},
            'analysis_assim_hawaii_no_da': {'t': range(0, 24, 1), 'tm': range(0, 2), 'var': 'channel_rt',
                                            'fname_config': 'analysis_assim'},
            'analysis_assim_long_no_da': {'t': range(0, 24, 1), 'tm': range(0, 2), 'var': 'channel_rt',
                                          'fname_config': 'analysis_assim'},
            'analysis_assim_puertorico_no_da': {'t': range(0, 24, 1), 'tm': range(0, 2), 'var': 'channel_rt',
                                                'fname_config': 'analysis_assim'},
            'medium_range_no_da': {'t': range(0, 24, 1), 'f': range(0, 2), 'var': 'channel_rt',
                                   'fname_config': 'medium_range'},
            'short_range_hawaii_no_da': {'t': range(0, 24, 1), 'f': range(0, 2), 'var': 'channel_rt',
                                         'fname_config': 'short_range'},
            'short_range_puertorico_no_da': {'t': range(0, 24, 1), 'f': range(0, 2), 'var': 'channel_rt',
                                             'fname_config': 'short_range'}
        }
