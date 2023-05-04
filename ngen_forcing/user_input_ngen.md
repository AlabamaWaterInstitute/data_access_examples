# Manual for ngen user options

## Example
filename = 'user_input_ngen.json'

contents:

    {
        "forcing"  : {
            "start_date"   : "20220822",
            "end_date"     : "20220822",
            "nwm_files"    : "",
            "runinput"     : 1,
            "varinput"     : 5,
            "geoinput"     : 1,
            "meminput"     : 0,
            "urlbaseinput" : 3
        },

        "hydrofab" : {
            "version"      : "v1.2",
            "vpu"          : "03W"
        },

        "verbose"     : true,
        "bucket_type" : "S3",
        "bucket_name" : "ciroh-devconf",
        "file_prefix" : "data/",    
        "file_type"   : "csv",
        "cache"       : true,
        "dl_threads"  : 10
        
    }

    
### forcing
| Field Name | Data Type | Description |
| --- | --- | --- |
| start_date | `string` | YYYYMMDD |
| end_date | `string` | YYYYMMDD |
| nwm_files | `string` | Path to a text file containing nwm file names. One filename per line. To have nwm forcing file names generated automatically, leave this option out of the config or set it to ""  |
| runinput | `int` | <ol><li>short_range</li><li>medium_range</li><li>medium_range_no_da</li><li>long_range</li><li>analysis_assim</li><li>analysis_assim_extend</li><li>analysis_assim_extend_no_da</li><li>analysis_assim_long</li><li>analysis_assim_long_no_da</li><li>analysis_assim_no_da</li><li>short_range_no_da</li></ol> |
| varinput | `int` | <ol><li>channel_rt: for real-time channel data</li><li>land: for land data</li><li>reservoir: for reservoir data</li><li>terrain_rt: for real-time terrain data</li><li>forcing: for forcing data</li></ol> |
| geoinput | `int` | <ol><li>conus: for continental US</li><li>hawaii: for Hawaii</li><li>puertorico: for Puerto Rico</li></ol> |
| meminput | `int` | <ol><li>mem_1</li><li>mem_2</li><li>mem_3</li><li>mem_4</li><li>mem_5</li><li>mem_6</li><li>mem_7</li></ol> |
| urlbaseinput | `int` | <ol><li>Empty string: use local files</li><li>https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/: for real-time operational data from NOAA</li><li>https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/post-processed/WMS/: for post-processed data from NOAA's Web Map Service</li><li>https://storage.googleapis.com/national-water-model/: for input/output data stored on Google Cloud Storage</li><li>https://storage.cloud.google.com/national-water-model/: for input/output data stored on Google Cloud Storage</li><li>gs://national-water-model/: for input/output data stored on Google Cloud Storage</li><li>https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/: for retrospective data from AWS S3</li><li>s3://noaa-nwm-retrospective-2-1-pds/model_output/: for retrospective data from AWS S3</li></ol> |

### hydrofab
| Field Name | Data Type | Description |
| --- | --- | --- |
| version | `string` | Current hydrofabric version |
| vpu | `string` | Check here for map of VPUs https://noaa-owp.github.io/hydrofabric/articles/data_access.html |

### other options
| Field Name | Data Type | Description |
| --- | --- | --- |
| verbose | `bool` | Print raw forcing files |
| bucket_type | `string` |  <ol><li>"local" : write to local directory</li><li>"S3" : output to AWS S3 bucket</li></ol> |
| bucket_name | `string` | If local, this is the name of the folder the data will be placed in. If S3, this is the name of S3 bucket, which must exist already. |
| file_prefix | `string` | If local, this is the relative path to the bucket_name folder. If S3, this is the relative path within the S3 bucket_name bucket to store files |
| file_type | `string` | <ol><li>"csv" : write data as csv files/</li><li>"parquet" : write data as parquet files</li></ol> |
| cache | `bool` | <il><li>true: Store forcing files locally. Must specify dl_threads</li><li> false: Interact with forcing files remotely</li></il>  |
| dl_threads | `int` | Number of threads to use while downloading. |
