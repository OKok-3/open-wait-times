import os
import sys
from datetime import timedelta
from importlib import import_module

from airflow.sdk import dag, task
from pendulum import datetime

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from registry import Registry

registry = Registry()
registry.register()

for hospital in registry.hospitals:
    dag_id = f"extract_data_{hospital['id']}"

    def create_dag(hospital_config: dict[str, any]):
        name = hospital_config["name"]
        dept = hospital_config["dept"]
        scraper_module = import_module(hospital_config["scraper_module"])
        scraper_class = getattr(scraper_module, hospital_config["scraper_class"])

        @dag(
            dag_id=f"extract_data_{hospital_config['id']}",
            dag_display_name=f"{name} {dept} Wait Times",
            tags=["Open Wait Times"],
            start_date=datetime(2025, 1, 1),
            schedule=timedelta(minutes=10),
            catchup=False,
            params={**hospital_config},
            default_args={
                "retries": 3,
                "retry_delay": timedelta(seconds=10),
            },
        )
        def extract_data() -> None:
            scraper = scraper_class(hospital_config)

            @task
            def scrape(ts: str) -> dict[str, str]:
                data = scraper.scrape(ts)
                return data

            @task.short_circuit
            def webpage_changed(skip_downstream_flag: bool) -> bool:
                return not skip_downstream_flag

            @task
            def parse(data: dict[str, str]) -> dict[str, any]:
                data = scraper.parse(data)
                return data

            @task.short_circuit
            def new_data_available(skip_downstream_flag: bool) -> bool:
                return not skip_downstream_flag

            @task
            def load_data(data: dict[str, any]) -> None:
                scraper.load_data(data)

            raw_data = scrape()
            wc = webpage_changed(raw_data["skip_downstream"])
            parsed_data = parse(raw_data)
            nda = new_data_available(parsed_data["skip_downstream"])
            load = load_data(parsed_data)

            wc >> parsed_data
            nda >> load

        return extract_data()

    globals()[dag_id] = create_dag(hospital)
