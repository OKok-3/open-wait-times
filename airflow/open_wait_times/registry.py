import os

import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Registry:
    hospitals: list[dict[str, any]]

    def __init__(self) -> None:
        self.hospitals = {}

    def register(self) -> None:
        """Register the hospitals from the manifest.yaml file and load data into database."""
        # Load hospitals from manifest.yaml file
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/manifest.yaml", "r") as manifest_file:
            manifest = yaml.safe_load(manifest_file)
            self.hospitals = manifest["hospitals"]

        # Load data into database
        pg = PostgresHook(postgres_conn_id="owt-pg")
        for hospital in self.hospitals:
            pg.insert_rows(
                table="owt.hospital_metadata",
                rows=[
                    (
                        hospital["id"],
                        hospital["name"],
                        hospital["dept"],
                        hospital["address"],
                        hospital["county"],
                        hospital["city"],
                        hospital["province"],
                        hospital["timezone"],
                        hospital["url"],
                        hospital["scraper_module"],
                        hospital["scraper_class"],
                        hospital["version"],
                    )
                ],
                target_fields=[
                    "id",
                    "name",
                    "dept",
                    "address",
                    "county",
                    "city",
                    "province",
                    "timezone",
                    "url",
                    "scraper_module",
                    "scraper_class",
                    "version",
                ],
                replace=True,
            )
