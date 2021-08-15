"""app.services.location.jhu.py"""
import csv
import logging
import os
from datetime import datetime
from pprint import pformat as pf

from asyncache import cached
from cachetools import TTLCache

from ...caches import check_cache, load_cache
from ...coordinates import Coordinates
from ...location import TimelinedLocation
from ...models import Timeline
from ...utils import countries
from ...utils import date as date_util
from ...utils import httputils
from . import LocationService
from ...location import factorylocation

LOGGER = logging.getLogger("services.location.jhu")
PID = os.getpid()

BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/2019-nCoV/master/csse_covid_19_data/csse_covid_19_time_series/"

class JhuLocationService():
    @cached(cache=TTLCache(maxsize=1, ttl=1800))
    async def get_locations():
        """
        Retrieves the locations from the categories. The locations are cached for 1 hour.
        :returns: The locations.
        :rtype: List[Location]
        """
        data_id = "jhu.locations"
        LOGGER.info(f"pid:{PID}: {data_id} Requesting data...")
        # Get all of the data categories locations.
        confirmed = await get_category("confirmed")
        deaths = await get_category("deaths")
        recovered = await get_category("recovered")

        locations_confirmed = confirmed["locations"]
        locations_deaths = deaths["locations"]
        locations_recovered = recovered["locations"]

        # Final locations to return.
        locations = []
        # ***************************************************************************
        # TODO: This iteration approach assumes the indexes remain the same
        #       and opens us to a CRITICAL ERROR. The removal of a column in the data source
        #       would break the API or SHIFT all the data confirmed, deaths, recovery producting
        #       incorrect data to consumers.
        # ***************************************************************************
        # Go through locations.
        for index, location in enumerate(locations_confirmed):
            # Get the timelines.

            # TEMP: Fix for merging recovery data. See TODO above for more details.
            key = (location["country"], location["province"])

            timelines = {
                "confirmed": location["history"],
                "deaths": parse_history(key, locations_deaths, index),
                "recovered": parse_history(key, locations_recovered, index),
            }

            # Grab coordinates.
            coordinates = location["coordinates"]

            # Create location (supporting timelines) and append.
            locations.append(
                TimelinedLocation(
                    # General info.
                    index,
                    location["country"],
                    location["province"],
                    # Coordinates.
                    Coordinates(
                        latitude=coordinates["lat"], longitude=coordinates["long"]),
                    # Last update.
                    datetime.utcnow().isoformat() + "Z",
                    # Timelines (parse dates as ISO).
                    {
                        "confirmed": Timeline(
                            timeline={
                                datetime.strptime(date, "%m/%d/%y").isoformat() + "Z": amount
                                for date, amount in timelines["confirmed"].items()
                            }
                        ),
                        "deaths": Timeline(
                            timeline={
                                datetime.strptime(date, "%m/%d/%y").isoformat() + "Z": amount
                                for date, amount in timelines["deaths"].items()
                            }
                        ),
                        "recovered": Timeline(
                            timeline={
                                datetime.strptime(date, "%m/%d/%y").isoformat() + "Z": amount
                                for date, amount in timelines["recovered"].items()
                            }
                        ),
                    },
                )
            )
        LOGGER.info(f"{data_id} Data normalized")

        return locations