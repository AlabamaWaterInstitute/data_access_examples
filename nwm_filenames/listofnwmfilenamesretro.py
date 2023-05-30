import requests
from dateutil import rrule
from datetime import datetime
from itertools import product
import multiprocessing
from multiprocessing.pool import Pool

retrospective_var_types = [
    ".CHRTOUT_DOMAIN1.comp",
    ".GWOUT_DOMAIN1.comp",
    ".LAKEOUT_DOMAIN1.comp",
    ".LDASOUT_DOMAIN1.comp",
    ".RTOUT_DOMAIN1.comp",
    ".LDASIN_DOMAIN1.comp",
]
objecttype = ["forcing/", "model_output/"]

def generate_url(date, file_type, urlbase_prefix, data_type):
    year_txt = f"{date.strftime('%Y')}"
    date_txt = f"{date.strftime('%Y%m%d%H')}"
    urlbase_prefix = urlbase_prefix + objecttype[file_type - 1]

    if data_type == 6:
        url = f"{urlbase_prefix}{year_txt}/{date_txt}00.LDASIN_DOMAIN1"
    else:
        url = f"{urlbase_prefix}{year_txt}/{date_txt}00{retrospective_var_types[data_type - 1]}"
    return url


def validate_date_range(start_date, end_date):
    _dtstart = datetime.strptime(start_date, "%Y%m%d%H%M")
    _until = datetime.strptime(end_date, "%Y%m%d%H%M")
    return _dtstart, _until

def retrospective_archive_file_name_creator(start_date, end_date, objecttype, file_types, urlbase_prefix):
    _dtstart, _until = validate_date_range(start_date, end_date)
    dates = rrule.rrule(
        rrule.HOURLY,
        dtstart=_dtstart,
        until=_until,
    )

    r = []
    datetimes = product(dates, range(1))
    for _dt, th in datetimes:
        for tp in file_types:
            for obj_type in objecttype:
                file_name = generate_url(_dt, obj_type, urlbase_prefix, tp)
                if file_name is not None:
                    r.append(file_name)

    return r, len(r)

urlbasedict = {
    6: "https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/",
    7: "s3://noaa-nwm-retrospective-2-1-pds/model_output/",
}

def create_file_list_retro(start_date=None, end_date=None, fcst_cycle=None, urlbaseinput=None, file_types=[1], objecttype=None):
    urlbase_prefix = urlbasedict[urlbaseinput]

    if urlbaseinput == 6:
        return retrospective_archive_file_name_creator(start_date, end_date, objecttype, file_types, urlbase_prefix)

def check_url(file):
    try:
        response = requests.head(file, timeout=1)
        if response.status_code == 200:
            return file
    except requests.exceptions.RequestException:
        pass

def check_valid_urls(file_list):
    with Pool(multiprocessing.cpu_count()) as p:
        valid_file_list = p.map(check_url, file_list)
    return [file for file in valid_file_list if file is not None]

def main():
    start_date = "19790201"
    end_date = "19790202"
    fcst_cycle = [12, 18]
    urlbaseinput = 6
    file_types = [5, 6]
    objecttype = [1]
    start_time = "0000"
    end_time = "0800"
    file_list, length = create_file_list_retro(start_date + start_time, end_date + end_time, fcst_cycle, urlbaseinput, file_types, objecttype)
    if length == 0:
        print(f"No files found")
    else:
        print(f"Files: {file_list}\nTotal files: {len(file_list)}")
        valid_file_list = check_valid_urls(file_list)
        print(f"Valid Files: {valid_file_list}\nValid files: {len(valid_file_list)}")

if __name__ == "__main__":
    main()