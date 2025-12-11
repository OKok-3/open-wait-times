import os
import tarfile
from concurrent.futures import ThreadPoolExecutor

import dotenv
import pendulum
import s3fs

dotenv.load_dotenv()

KEY_ID = os.getenv("KEY_ID")
SECRET = os.getenv("SECRET")
ARCHIVAL_YEAR = 2025
ARCHIVAL_MONTH = 11

hospital_ids = {
    "LHSC Children's Hospital": "lhsc_ch_pediatric_er",
    "LHSC University Hospital": "lhsc_uh_adult_er",
    "LHSC Victoria Hospital": "lhsc_vh_adult_er",
}

s3 = s3fs.S3FileSystem(
    key=KEY_ID,
    secret=SECRET,
    endpoint_url="https://s3.tguan.xyz",
    client_kwargs={"region_name": "canada-east-1"},
)

hospitals = [
    hospital
    for hospital in s3.ls("open-wait-times", detail=False)
    if "Hospital" in hospital
]
files = s3.find("open-wait-times")
files_to_archive = []


def download_file(file: str) -> None:
    timestamp = file[file.rfind("_") + 1 : file.rfind(".")]
    month = pendulum.parse(timestamp).month
    year = pendulum.parse(timestamp).year
    if month == ARCHIVAL_MONTH and year == ARCHIVAL_YEAR:
        s3.download(file, f"./{file}")
        files_to_archive.append(file)


with ThreadPoolExecutor(max_workers=os.cpu_count() or 6) as executor:
    executor.map(download_file, files)

print("Finished downloading files")

for hospital in hospitals:
    print(f"Compressing files for {hospital.split('/')[-1]}")
    output_filename = f"{hospital_ids[hospital.split('/')[-1]]}_{ARCHIVAL_YEAR}-{ARCHIVAL_MONTH:02d}.tar.xz"
    with tarfile.open(output_filename, "w:xz", preset=9) as tar:
        for file in os.listdir(f"{hospital}/ER"):
            tar.add(f"{hospital}/ER/{file}", arcname=file)

    s3.mkdir(f"open-wait-times/archives/{ARCHIVAL_YEAR}", create_parents=True)
    s3.upload(
        output_filename, f"open-wait-times/archives/{ARCHIVAL_YEAR}/{output_filename}"
    )

for file in files_to_archive:
    s3.rm_file(file)
