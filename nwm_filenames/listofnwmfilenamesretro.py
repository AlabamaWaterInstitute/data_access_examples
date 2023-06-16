from datetime import datetime, timedelta

from filename_helpers import check_valid_urls

retrospective_var_types = {
    1: ".CHRTOUT_DOMAIN1.comp",
    2: ".GWOUT_DOMAIN1.comp",
    3: ".LAKEOUT_DOMAIN1.comp",
    4: ".LDASOUT_DOMAIN1.comp",
    5: ".RTOUT_DOMAIN1.comp",
    6: ".LDASIN_DOMAIN1.comp",
}

objecttypes = {1: "forcing/", 2: "model_output/"}

urlbasedict = {
    6: "https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/",
    7: "s3://noaa-nwm-retrospective-2-1-pds/model_output/",
}


def generate_url(date, file_type, urlbase_prefix, retrospective_var_types=None):
    year_txt = date.strftime("%Y")
    date_txt = date.strftime("%Y%m%d%H")

    if "forcing" in file_type and date.year < 2007:
        url = f"{urlbase_prefix}{file_type}{year_txt}/{date_txt}00.LDASIN_DOMAIN1"
    elif "forcing" in file_type and date.year >= 2007:
        url = f"{urlbase_prefix}{file_type}{year_txt}/{date_txt}.LDASIN_DOMAIN1"
    elif "model_output" in file_type:
        url = [
            f"{urlbase_prefix}{file_type}{year_txt}/{date_txt}00{type}"
            for type in retrospective_var_types
        ]

    return url


def create_file_list_retro(
    start_date=None,
    end_date=None,
    urlbaseinput=None,
    objecttype=objecttypes,
    selected_var_types=None,
):
    urlbase_prefix = urlbasedict[urlbaseinput]
    objecttype = [objecttypes[i] for i in objecttype]
    retrospective_var_types_selected = [
        retrospective_var_types[i] for i in selected_var_types
    ]

    start_dt = datetime.strptime(start_date, "%Y%m%d%H%M")
    end_dt = datetime.strptime(end_date, "%Y%m%d%H%M")

    delta = end_dt - start_dt
    date_range = [
        start_dt + timedelta(hours=i)
        for i in range(delta.days * 24 + delta.seconds // 3600 + 1)
    ]

    file_list = []
    for date in date_range:
        for obj_type in objecttype:
            file_names = generate_url(
                date, obj_type, urlbase_prefix, retrospective_var_types_selected
            )
            if file_names is not None:
                if isinstance(file_names, list):
                    file_list.extend(file_names)
                else:
                    file_list.append(file_names)

    return file_list


def main():
    start_date = "20070101"
    end_date = "20070102"
    urlbaseinput = 6
    selected_var_types = [1, 2]
    selected_object_types = [1]  # To test both forcing and model_output
    start_time = "0000"
    end_time = "0800"

    file_list = create_file_list_retro(
        start_date + start_time,
        end_date + end_time,
        urlbaseinput,
        selected_object_types,
        selected_var_types,
    )

    if len(file_list) == 0:
        print(f"No files found")
    else:
        print(f"Files: {file_list}\nTotal Files: {len(file_list)}")

        valid_file_list = check_valid_urls(file_list)

        print(f"Valid Files: {valid_file_list}\nValid files: {len(valid_file_list)}")


if __name__ == "__main__":
    main()
