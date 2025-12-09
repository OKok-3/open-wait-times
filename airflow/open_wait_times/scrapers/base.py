from abc import ABC, abstractmethod


class BaseScraper(ABC):
    _id: str
    name: str
    dept: str
    latitude: float
    longitude: float
    address: str
    county: str
    city: str
    province: str
    timezone: str
    url: str
    scraper_module: str
    scraper_class: str
    version: int

    def __init__(self, metadata: dict[str, any]):
        self._id = metadata["id"]
        self.name = metadata["name"]
        self.dept = metadata["dept"]
        self.latitude = metadata["latitude"]
        self.longitude = metadata["longitude"]
        self.address = metadata["address"]
        self.county = metadata["county"]
        self.city = metadata["city"]
        self.province = metadata["province"]
        self.timezone = metadata["timezone"]
        self.url = metadata["url"]
        self.scraper_module = metadata["scraper_module"]
        self.scraper_class = metadata["scraper_class"]
        self.version = metadata["version"]

    @abstractmethod
    def scrape(self, ts: str) -> dict[str, str]:
        """Scrape the data from the URL, upload the raw data to S3 and insert the fetch log into the database.

        Args:
            ts (str): The timestamp of the scrape

        Returns:
            dict[str, str]: Data and metadata about the scrape
        """
        raise NotImplementedError

    @abstractmethod
    def parse(self, data: dict[str, str]) -> dict[str, any]:
        """Parse the data from the scrape task and return the parsed data.

        Args:
            data (dict[str, str]): The data from the scrape

        Returns:
            dict[str, any]: The parsed data
        """
        raise NotImplementedError

    @abstractmethod
    def load_data(self, data: dict[str, any]) -> None:
        """Load the data into the database.

        Args:
            data (dict[str, any]): The data from the parse task
        """
        raise NotImplementedError
