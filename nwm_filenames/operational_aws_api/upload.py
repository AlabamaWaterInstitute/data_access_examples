from concurrent.futures import ProcessPoolExecutor
import requests
import boto3 
from kerchunk.hdf import SingleHdf5ToZarr
import fsspec
import json
import os

AWS_ACCESS_KEY_ID = 'AKIA4P7DSRJWW4TWOXVA'
AWS_SECRET_ACCESS_KEY = 'Gr9dS0Rrq8KmB8937honqzZDT06MXCy/j0H+VS4t'
BUCKET = "ciroh-nwm-zarr-copy"

def download_and_convert_and_upload(filename):

    fileurl = filename.replace("\n","")

    text_split = fileurl.split("/")
    filename = text_split[-1]
    justname = filename[:-3]
    sub_folder = text_split[-2]
    date_folder = text_split[-3]
    bucket_name = text_split[-4]

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    res = requests.get(fileurl, allow_redirects=True)
    with open(f"./tmp{justname}.nc", "w+b") as file:

        file.write(res.content)
        converted_data = SingleHdf5ToZarr(file, fileurl).translate()
        with open(f"./{justname}.json", "w") as file2:
            json.dump(converted_data,file2)
        with open(f"./{justname}.json", "r") as file2:
            s3.upload_file(f"./{justname}.json",BUCKET,f"{bucket_name}/{date_folder}/{sub_folder}/{filename}.json")
        

    os.remove(f"./{justname}.json")
    os.remove(f"./tmp{justname}.nc")

def main():

    with open("./filenamelist.txt") as file:

        lines = file.readlines()

    with ProcessPoolExecutor() as executor:

        executor.map(download_and_convert_and_upload, lines)


if __name__ == "__main__":
    main()
