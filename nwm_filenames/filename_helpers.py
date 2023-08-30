from concurrent.futures import ThreadPoolExecutor
import requests
from functools import partial


def check_valid_urls(file_list, session=None):
    if not session:
        session = requests.Session()
    check_url_part = partial(check_url, session)
    with ThreadPoolExecutor(max_workers=10) as executor:
        valid_file_list = list(executor.map(check_url_part, file_list))

    return [file for file in valid_file_list if file is not None]


def check_url(session, file):
    try:
        with requests.get(file, stream=True, timeout=1) as response:
            response.raise_for_status()
            return file
        response = session.head(file, timeout=1)
    except requests.exceptions.RequestException:
        pass
