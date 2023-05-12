# Manual for ngen user options

## Example
filename = 'user_input_ngen.json'

contents:

    {
        "forcing"  : {
            "forcing_type" : "operational_archive",
            "start_date"   : "20220822",
            "end_date"     : "20220822",
            "cache"        : true,        
            "nwm_file"     : "",
            "runinput"     : 1,
            "varinput"     : 5,
            "geoinput"     : 1,
            "meminput"     : 0,
            "urlbaseinput" : 3,
            "fcst_cycle"   : [0],
            "lead_time"    : null
        },

        "hydrofab" : {
            "version"      : "v1.2",
            "vpu"          : "03W",
            "catch_subset" : "cat-112977",
            "weights_only" : false
        },

        "storage":{
            "bucket_type" : "local",
            "bucket_name" : "ngen_inputs",
            "file_prefix" : "data/",    
            "file_type"   : "csv"
        },

        "run" : {
            "verbose"  : true,
            "nthreads" : 2
        }

    }

### forcing
| Field Name | Data Type | Description |
| --- | --- | --- |
| forcing_type | `string` | <il><li>operational_archive</li><li>retrospective</li><li>from_file</li></il>|
| start_date | `string` | YYYYMMDD |
| end_date | `string` | YYYYMMDD |
| cache | `bool` | <il><li>true: Store forcing files locally. Must specify dl_threads</li><li> false: Interact with forcing files remotely</li></il>  |
| nwm_file | `string` | Path to a text file containing nwm file names. One filename per line. Set this only if forcing_type is set to 'from_file' |
| runinput | `int` | <ol><li>short_range</li><li>medium_range</li><li>medium_range_no_da</li><li>long_range</li><li>analysis_assim</li><li>analysis_assim_extend</li><li>analysis_assim_extend_no_da</li><li>analysis_assim_long</li><li>analysis_assim_long_no_da</li><li>analysis_assim_no_da</li><li>short_range_no_da</li></ol> |
| varinput | `int` | <ol><li>channel_rt</li><li>land</li><li>reservoir</li><li>terrain_rt terrain</li><li>forcing</li></ol> |
| geoinput | `int` | <ol><li>conus</li><li>hawaii</li><li>puertorico</li></ol> |
| meminput | `int` | <ol><li>mem_1</li><li>mem_2</li><li>mem_3</li><li>mem_4</li><li>mem_5</li><li>mem_6</li><li>mem_7</li></ol> |
| urlbaseinput | `int` | <ol><li>https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/: for real-time operational data from NOAA</li><li>https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/post-processed/WMS/: for post-processed data from NOAA's Web Map Service</li><li>https://storage.googleapis.com/national-water-model/: for input/output data stored on Google Cloud Storage</li><li>https://storage.cloud.google.com/national-water-model/: for input/output data stored on Google Cloud Storage</li><li>gs://national-water-model/: for input/output data stored on Google Cloud Storage</li><li>https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/: for retrospective data from AWS S3</li><li>s3://noaa-nwm-retrospective-2-1-pds/model_output/: for retrospective data from AWS S3</li></ol> |
| fcst_cycle | `list` | List of forecast cycles in UTC. If empty, will use all available cycles |
| lead_time | `list` | List of lead times in hours. If empty, will use all available lead times |
| data_type | `list` | Only required for retroactive <ol><li>CHRTOUT_DOMAIN1</li><li>GWOUT_DOMAIN1</li><li>LAKEOUT_DOMAIN1</li><li>LDASOUT_DOMAIN1</li><li>RTOUT_DOMAIN1</li><li>LDASIN_DOMAIN1</li></ol> |
| object_type | `list` or `int` | Only required for retroactive <ol><li>forcing</li><li>model_output</li></ol> |


### hydrofab
| Field Name | Data Type | Description |
| --- | --- | --- |
| version | `string` | Desired hydrofabric data version |
| vpu | `string` | Check here for map of VPUs https://noaa-owp.github.io/hydrofabric/articles/data_access.html |
| geopkg_file | `string` | Path to file containing catchment polygons. Must exist locally |
| catch_subset | `string` | catchment id of the form "cat-#". If provided, a subsetted geopackage will be created from vpu geopackage. NGen config files will be generated as well |
| weights_only | `bool` | <il><li>true: Generate weight file and exit. </li><li> false: Proceed with full script, generate forcing files</li></il> |


### storage
| Field Name | Data Type | Description |
| --- | --- | --- |
| bucket_type | `string` |  <ol><li>"local" : write to local directory</li><li>"S3" : output to AWS S3 bucket</li></ol> |
| bucket_name | `string` | If local, this is the name of the folder the data will be placed in. If S3, this is the name of S3 bucket, which must exist already |
| file_prefix | `string` | If local, this is the relative path to the bucket_name folder. If S3, this is the relative path within the S3 bucket_name bucket to store files |
| file_type | `string` | <ol><li>"csv" : write data as csv files/</li><li>"parquet" : write data as parquet files</li></ol> |


### run
| Field Name | Data Type | Description |
| --- | --- | --- |
| verbose | `bool` | Print raw forcing files |
| dl_threads | `int` | Number of threads to use while downloading. |
| proc_threads | `int` | Number of threads to use while processing data (either remotely or locally). |
