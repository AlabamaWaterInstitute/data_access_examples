import os, sys
from pathlib import Path
pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "ngen_forcing")
sys.path.append(str(pkg_dir))
from prep_hydrofab_forcings_ngen import prep_ngen_data      

def test_data_prep():
    """
    This tests the entire script. 
    #TODO: Break this into separate test functions
    """

    conf = {
        "forcing":{
        "forcing_type" : "retrospective",
        "start_date"   : "19790201",
        "end_date"     : "19790202",
        "cache"        : True,        
        "nwm_file"     : "",
        "runinput"     : 2,
        "varinput"     : 1,
        "geoinput"     : 1,
        "meminput"     : 1,
        "urlbaseinput" : 6,
        "fcst_cycle"   : [12,18],
        "lead_time"    : [1, 2, 240],
        "data_type"    : [6],
        "object_type"  : 1
    },
        "hydrofab":{
        "version"      : "v1.2",
        "vpu"          : "03W",
        "catch_subset" : "cat-112977",
        "weights_only" : False
    },
        "storage": {
        "bucket_type" : "local",
        "bucket_name" : "ngen_inputs",
        "file_prefix" : "tests/data/",    
        "file_type"   : "csv"
    },
        "run":{
        "verbose"      : True,
        "dl_threads"   : 10,
        "proc_threads" : 2 
    }
    }

    prep_ngen_data(conf)
