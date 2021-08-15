from services.location import LocationService


class Alocation (LocationService):
    """
    Adapter that retrieves locations from data source (jhu, csbs, nyt)
    """
    async def get_all(self):
        # Get the locations.
        locations = await LocationService.get_locations()
        return locations

    async def get(self, loc_id):  # pylint: disable=arguments-differ
        # Get location at the index equal to the provided id.
        locations = await LocationService.get_locations()
        return locations[loc_id]