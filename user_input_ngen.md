# Manual for ngen user options

## Example
filename = 'user_input_ngen.json'

contents:

    {
        "forcing"  : {
            "start_date"   : "20220822",
            "end_date"     : "20220822",
            "runinput"     : 2,
            "varinput"     : 5,
            "geoinput"     : 1,
            "meminput"     : 0,
            "urlbaseinput" : 3
        },

        "hydrofab" : {
            "vpu"          : "03W"
        },

        "verbose"     : true,
        "output_dir"  : "local",
        "cache"       : true
    }
    
### forcing
| Field Name | Data Type | Description |
| --- | --- | --- |
| start_date | `string` | YYYYMMDD |
| end_date | `string` | YYYYMMDD |
| runinput | `int` | <ol><li>short_range</li><li>medium_range</li><li>medium_range_no_da</li><li>long_range</li><li>analysis_assim</li><li>analysis_assim_extend</li><li>analysis_assim_extend_no_da</li><li>analysis_assim_long</li><li>analysis_assim_long_no_da</li><li>analysis_assim_no_da</li><li>short_range_no_da</li></ol> |
| varinput | `int` | <ol><li>channel_rt: for real-time channel data</li><li>land: for land data</li><li>reservoir: for reservoir data</li><li>terrain_rt: for real-time terrain data</li><li>forcing: for forcing data</li></ol> |
| geoinput | `int` | <ol><li>conus: for continental US</li><li>hawaii: for Hawaii</li><li>puertorico: for Puerto Rico</li></ol> |
| meminput | `int` | <ol><li>mem_1</li><li>mem_2</li><li>mem_3</li><li>mem_4</li><li>mem_5</li><li>mem_6</li><li>mem_7</li></ol> |
| urlbaseinput | `int` | <ol><li>Empty string: use local files</li><li>https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/: for real-time operational data from NOAA</li><li>https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/post-processed/WMS/: for post-processed data from NOAA's Web Map Service</li><li>https://storage.googleapis.com/national-water-model/: for input/output data stored on Google Cloud Storage</li><li>https://storage.cloud.google.com/national-water-model/: for input/output data stored on Google Cloud Storage</li><li>gs://national-water-model/: for input/output data stored on Google Cloud Storage</li><li>https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/: for retrospective data from AWS S3</li><li>s3://noaa-nwm-retrospective-2-1-pds/model_output/: for retrospective data from AWS S3</li></ol> |


### hydrofab
| Field Name | Data Type | Description |
| --- | --- | --- |
| vpu | `string` | Check here for map of VPUs https://noaa-owp.github.io/hydrofabric/articles/data_access.html |

### other options
| Field Name | Data Type | Description |
| --- | --- | --- |
| verbose | `bool` | Print raw forcing files |
| output_dir | `string` |  <ol><li>"local" : output to ./data/catchment_forcing_data/</li></ol> |
| cache | `bool` | <il><li>true: store forcing files locally</li><li> false: interact with forcing files remotely</li></il> |
