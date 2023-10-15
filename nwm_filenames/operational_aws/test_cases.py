import pytest
from datetime import datetime
from listofnwmfilenames import (
    selectvar,
    selectgeo,
    selectrun,
    makename,
    run_type,
    fhprefix,
    varsuffix,
    run_typesuffix,
    select_forecast_cycle,
    select_lead_time,
    selecturlbase,
    create_file_list,
)


def test_selectvar():
    assert selectvar({1: "channel_rt"}, 1) == "channel_rt"


def test_selectgeo():
    assert selectgeo({1: "conus"}, 1) == "conus"


def test_selectrun():
    assert selectrun({1: "short_range"}, 1) == "short_range"


def test_makename():
    assert makename(
        datetime(2022, 1, 1, 0, 0, 0, 0),
        "short_range",
        "channel_rt",
        0,
        1,
        "conus",
        "forcing",
        fhprefix="f",
        runsuffix="_test",
        varsuffix="_test",
        run_typesuffix="_test",
        urlbase_prefix="https://example.com/",
    ) == "https://example.com/nwm.20220101/forcing_test/nwm.t00z.short_range_test.channel_rt_test.f001.conus.nc"

@pytest.mark.parametrize("runinput, varinput, geoinput, expected_output", [
    (5, 5, 2, "forcing_analysis_assim_hawaii"),
    (5, 5, 3, "forcing_analysis_assim_puertorico"),
    (2, 5, 7, "forcing_medium_range"),
    (1, 5, 7, "forcing_short_range"),
    (1, 3, 3, "short_range_puertorico"),
    (1, 5, 2, "forcing_short_range_hawaii"),
    (1, 5, 3, "forcing_short_range_puertorico"),
    (5, 5, 7, "forcing_analysis_assim"),
    (6, 5, 7, "forcing_analysis_assim_extend"),
    (5, 3, 3, "analysis_assim_puertorico"),
    (10, 3, 3, "analysis_assim_puertorico_no_da"),
    (1, 3, 3, "short_range_puertorico"),
    (11, 3, 3, "short_range_puertorico_no_da"),
    (2, 2, 2, "default_value")  # Add a test case for default value
])
def test_run_type(runinput, varinput, geoinput, expected_output):
    assert run_type(runinput, varinput, geoinput, "default_value") == expected_output


def test_fhprefix():
    assert fhprefix(5) == "tm"
    assert fhprefix(1) == "f"
    assert fhprefix(10) == "tm"


def test_varsuffix():
    assert varsuffix(1) == "_1"
    assert varsuffix(7) == "_7"
    assert varsuffix(8) == ""


def test_run_typesuffix():
    assert run_typesuffix(1) == "_mem1"
    assert run_typesuffix(7) == "_mem7"
    assert run_typesuffix(8) == ""


def test_select_forecast_cycle():
    assert select_forecast_cycle(12, 0) == 12
    assert select_forecast_cycle(None, 0) == 0


def test_select_lead_time():
    assert select_lead_time(240, 0) == 240
    assert select_lead_time(None, 0) == 0


def test_selecturlbase():
    assert selecturlbase({1: "https://example.com/"}, 1) == "https://example.com/"
    assert selecturlbase({1: "https://example.com/"}, 2, "default") == "default"

fcst_cycle_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
lead_time_values = [1, 2, 240]
valid_base_urls = [
    "",
    "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/",
    "https://storage.googleapis.com/national-water-model/",
    "https://storage.cloud.google.com/national-water-model/",
    "gs://national-water-model/",
    "gcs://national-water-model/",
    "https://noaa-nwm-pds.s3.amazonaws.com/",
    "https://ciroh-nwm-zarr-copy.s3.amazonaws.com/national-water-model/",
]

valid_folder_names = [
    "analysis_assim",
    "analysis_assim_alaska",
    "analysis_assim_alaska_no_da",
    "analysis_assim_coastal_atlgulf",
    "analysis_assim_coastal_hawaii",
    "analysis_assim_coastal_pacific",
    "analysis_assim_coastal_puertorico",
    "analysis_assim_extend",
    "analysis_assim_extend_alaska",
    "analysis_assim_extend_alaska_no_da",
    "analysis_assim_extend_coastal_atlgulf",
    "analysis_assim_extend_coastal_pacific",
    "analysis_assim_extend_no_da",
    "analysis_assim_hawaii",
    "analysis_assim_hawaii_no_da",
    "analysis_assim_long",
    "analysis_assim_long_no_da",
    "analysis_assim_no_da",
    "analysis_assim_puertorico",
    "analysis_assim_puertorico_no_da",
    "forcing_analysis_assim",
    "forcing_analysis_assim_alaska",
    "forcing_analysis_assim_extend",
    "forcing_analysis_assim_extend_alaska",
    "forcing_analysis_assim_hawaii",
    "forcing_analysis_assim_puertorico",
    "forcing_medium_range",
    "forcing_medium_range_alaska",
    "forcing_medium_range_blend",
    "forcing_medium_range_blend_alaska",
    "forcing_short_range",
    "forcing_short_range_alaska",
    "forcing_short_range_hawaii",
    "forcing_short_range_puertorico",
    "long_range_mem1",
    "long_range_mem2",
    "long_range_mem3",
    "long_range_mem4",
    "medium_range_alaska_mem1",
    "medium_range_alaska_mem2",
    "medium_range_alaska_mem3",
    "medium_range_alaska_mem4",
    "medium_range_alaska_mem5",
    "medium_range_alaska_mem6",
    "medium_range_alaska_no_da",
    "medium_range_blend",
    "medium_range_blend_alaska",
    "medium_range_blend_coastal_atlgulf",
    "medium_range_blend_coastal_pacific",
    "medium_range_coastal_atlgulf_mem1",
    "short_range"
]
import requests

def is_valid_url(url):
    try:
        response = requests.head(url)
        return response.status_code < 400
    except requests.ConnectionError:
        return False

@pytest.mark.parametrize("runinput, varinput, geoinput, meminput, start_date, end_date, fcst_cycle, urlbaseinput, lead_time, expected_output", [
    (1, 1, 1, 0, "201809170000", "201809172300", fcst_cycle_values, 3, None, ["expected_file_name_1"]),
    (5, 5, 2, 1, "201809170000", "201809171200", fcst_cycle_values, 1, lead_time_values, ["expected_file_name_2"]),
    (2, 5, 3, 3, "201809170600", "201809171800", fcst_cycle_values, 2, lead_time_values, ["expected_file_name_3"]),

])
def test_create_file_list(runinput, varinput, geoinput, meminput, start_date, end_date, fcst_cycle, urlbaseinput, lead_time, expected_output):
    file_list = create_file_list(runinput, varinput, geoinput, meminput, start_date, end_date, fcst_cycle, urlbaseinput, lead_time)
    assert isinstance(file_list, list)
    assert all(isinstance(file_name, str) for file_name in file_list)
    for url in file_list:
        # assert is_valid_url(url), f"Invalid URL: {url}"
        assert any(substring in url for substring in valid_folder_names), f"No valid folder name found in URL: {url}"


    # Check if all base URLs exist in the predefined list
    for url in file_list:
        assert any(url.startswith(base_url) for base_url in valid_base_urls), f"Invalid base URL in generated URL: {url}"



if __name__ == "__main__":
    pytest.main()