import asyncio

from functools import cache
import random
from site import venv
import time
import requests
import asyncio
import aiohttp
import pandas as pd
from geopy import distance


TEMPLATE = {
    "isPrivateResidence": "",
    "address": "",
    "businessType": "",
    "additionalTypes": [],
}


async def async_get(session, url):
    async with session.get(url) as response:
        return await response.json()


def _build_url(lat, lng, radius, list_entity_types):
    return f'https://dev.virtualearth.net/REST/v1/LocationRecog/{lat},{lng}?radius={radius}&top=5&includeEntityTypes={list_entity_types}&key={os.environ.get("BING_KEY")}'


def find_nearest_business(response, lat, lon):
    distance_ = []
    for loc in response["resourceSets"][0]["resources"][0]["businessesAtLocation"]:
        distance_.append(
            [
                distance.distance(
                    (
                        loc["businessAddress"]["latitude"],
                        loc["businessAddress"]["longitude"],
                    ),
                    (lat, lon),
                ).kilometers,
                loc,
            ]
        )
        if "apartments" in loc["businessInfo"].get("type", "").lower() or any(
            "apartments" in x.lower() for x in loc["businessInfo"].get("otherTypes", [])
        ):
            distance_[-1][0] = 0
            break

    distance_ = sorted(distance_, key=lambda x: x[0])
    return distance_[0][1]


async def pull_data(
    lat,
    lng,
    session,
    radius=0.05,
):

    # check first if Bing cateorizes it as a private address or not
    list_of_entity_types = "address"
    try:
        response = await async_get(
            session, _build_url(lat, lng, radius, list_of_entity_types)
        )
        try:
            if (
                response["resourceSets"][0]["resources"][0]["isPrivateResidence"]
                in "true"
            ):
                return {
                    "isPrivateResidence": True,
                    "address": response["resourceSets"][0]["resources"][0][
                        "addressOfLocation"
                    ][0]["formattedAddress"],
                    "businessType": "",
                    "additionalTypes": [],
                }
        except IndexError as e:
            # this means that we've hit a rate limit error
            print(response)
            raise e

    except requests.exceptions.HTTPError as e:
        print(e)
        return TEMPLATE

    try:
        # this means that the address is not a private residence, now lookup what the address is
        response = await async_get(
            session, _build_url(lat, lng, radius, "businessAndPOI")
        )
        try:
            business = find_nearest_business(response, lat, lng)
            return {
                "isPrivateResidence": False,
                "address": business["businessAddress"].get("formattedAddress", ""),
                "businessType": business["businessInfo"].get("type", ""),
                "additionalTypes": business["businessInfo"].get("otherTypes", []),
            }
        except IndexError:
            v = TEMPLATE.copy()
            v["isPrivateResidence"] = True
            return v

    except requests.exceptions.HTTPError as e:
        print(e)
        return TEMPLATE


# this no longer is rate limited
async def rate_limited_puller(session, row, results_dictionary, radius=0.1):
    results_dictionary[tuple(row)] = await pull_data(*row, session, radius)
    return None
    # print(row)


async def pull_data(df: pd.Dataframe) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        results_dictionary = {}
        tasks = [
            asyncio.create_task(rate_limited_puller(session, row, results_dictionary))
            for row in df.itertuples(index=False)
        ]

        await asyncio.gather(*tasks)
    return pd.DataFrame(results_dictionary).T.reset_index()
