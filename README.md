## NOAA NWM Dataset from AWS

Script to access [National Water Model Dataset](https://registry.opendata.aws/nwm-archive/) from AWS S3 storage .
The data is filtered based on user queried date range and feature ID.

### Dependencies

#### Required packages

- s3fs
- xarray
- matplotlib
- pandas
- zarr
- numpy
- hydroeval

