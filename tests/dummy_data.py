from __future__ import annotations

import csv
from pathlib import Path


def generate_dummy_data(base: str | Path, scrape_ym: str = "2021-01") -> Path:
    """Create minimal raw input data for tests.

    Parameters
    ----------
    base : str | Path
        Directory used as the base URI for apps.
    scrape_ym : str
        Year-month string used to build city paths.

    Returns
    -------
    Path
        Path object of the base directory used for data generation.
    """
    base_path = Path(base)
    raw = base_path / "raw"

    # global listings
    raw.mkdir(parents=True, exist_ok=True)
    gl_file = raw / "airbnb-listings.csv"
    with gl_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "city", "host_id", "host_name", "last_scraped"])
        writer.writerow([1, "Amsterdam", 10, "Host", "2021-01-05"])

    # city listings
    city_dir = raw / "cities" / "amsterdam" / scrape_ym
    city_dir.mkdir(parents=True, exist_ok=True)
    with (city_dir / "listings.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "city", "host_id", "host_name", "last_scraped"])
        writer.writerow([1, "Amsterdam", 10, "Host", "2021-01-05"])

    # city reviews
    with (city_dir / "reviews.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "id",
                "listing_id",
                "date",
                "reviewer_id",
                "reviewer_name",
                "comments",
            ]
        )
        writer.writerow([1, 1, "2021-01-10", 100, "Reviewer", "Great!"])

    # weather temperature
    temp_dir = raw / "weather" / "ECA_blend_tg"
    temp_dir.mkdir(parents=True, exist_ok=True)
    with (temp_dir / "temp.txt").open("w") as f:
        f.write("STAID,SOUID,DATE,TG,Q_TG\n593,XX,20210110,50,0\n")

    # weather rain
    rain_dir = raw / "weather" / "ECA_blend_rr"
    rain_dir.mkdir(parents=True, exist_ok=True)
    with (rain_dir / "rain.txt").open("w") as f:
        f.write("STAID,SOUID,DATE,RR,Q_TG\n593,XX,20210110,5,0\n")

    return base_path
