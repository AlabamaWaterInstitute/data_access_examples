import pytest
import os, sys, json
from pathlib import Path
pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "ngen_forcing")
sys.path.append(str(pkg_dir))
from prep_hydrofab_forcings_ngen import prep_ngen_data

pkg_dir = Path(Path(os.path.dirname(__file__)).parent, "nwm_filenames")
sys.path.append(str(pkg_dir))
from listofnwmfilenames import create_file_list
from listofnwmfilenamesretro import create_file_list_retro

def test_filenames_operational_archive():
    runinput = 2
    varinput = 1
    geoinput = 1
    meminput = 1
    start_date = "20220822"
    end_date = "20220824"
    fcst_cycle = [12, 18]    
    urlbaseinput = None
    lead_time = [1, 2, 240]

    nwm_forcing_files = create_file_list(
    runinput,
    varinput,
    geoinput,
    meminput,
    start_date,
    end_date,
    fcst_cycle,
    urlbaseinput,
    lead_time
    )

    nwm_forcing_files_truth = [
        'nwm.20220822/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f001.conus.nc',
        'nwm.20220822/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f002.conus.nc',
        'nwm.20220822/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f240.conus.nc',
        'nwm.20220822/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f001.conus.nc', 
        'nwm.20220822/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f002.conus.nc', 
        'nwm.20220822/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f240.conus.nc', 
        'nwm.20220823/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f001.conus.nc', 
        'nwm.20220823/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f002.conus.nc', 
        'nwm.20220823/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f240.conus.nc', 
        'nwm.20220823/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f001.conus.nc', 
        'nwm.20220823/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f002.conus.nc', 
        'nwm.20220823/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f240.conus.nc', 
        'nwm.20220824/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f001.conus.nc', 
        'nwm.20220824/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f002.conus.nc', 
        'nwm.20220824/medium_range_mem1/nwm.t12z.medium_range.channel_rt_1.f240.conus.nc', 
        'nwm.20220824/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f001.conus.nc', 
        'nwm.20220824/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f002.conus.nc', 
        'nwm.20220824/medium_range_mem1/nwm.t18z.medium_range.channel_rt_1.f240.conus.nc'
        ]
    
    for j,jf in enumerate(nwm_forcing_files_truth):
        assert nwm_forcing_files[j] == jf


def test_filenames_retrospective():
    runinput = 2
    varinput = 1
    geoinput = 1
    meminput = 1
    start_date = "19790201"
    end_date = "19790202"
    fcst_cycle = [12, 18]    
    urlbaseinput = 6
    lead_time = [1, 2, 240]
    data_type = [6]
    object_type = 1

    nwm_forcing_files = create_file_list_retro(
            runinput,
            varinput,
            geoinput,
            meminput,
            start_date + "0000", # Hack
            end_date + "0000", # Hack
            fcst_cycle,
            urlbaseinput,
            lead_time,
            data_type,
            object_type
        )
    
    nwm_forcing_files = nwm_forcing_files[0]

    nwm_forcing_files_truth = [
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010000.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010100.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010200.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010300.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010400.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010500.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010600.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010700.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010800.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902010900.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011000.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011100.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011200.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011300.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011400.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011500.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011600.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011700.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011800.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902011900.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902012000.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902012100.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902012200.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902012300.LDASIN_DOMAIN1', 
        'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/1979/197902020000.LDASIN_DOMAIN1'
        ]

    for j,jf in enumerate(nwm_forcing_files_truth):
        assert nwm_forcing_files[j] == jf        

def test_data_prep():
    """
    This tests the entire script. 
    #TODO: Break this into seperate test functions
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
