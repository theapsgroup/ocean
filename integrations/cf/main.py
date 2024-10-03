from typing import Any
from datetime import datetime

from port_ocean.context.ocean import ocean
from cloudflare import AsyncCloudflare

def init_cloudflare_client() -> AsyncCloudflare:
    if ocean.integration_config["cf_api_token"] is None:
        return AsyncCloudflare(
            api_email=ocean.integration_config["cf_email"],
            api_key=ocean.integration_config["cf_api_key"],
        )
    else:
        return AsyncCloudflare(
            api_token=ocean.integration_config["cf_api_token"],
        )
    
cloudflare_client = init_cloudflare_client()
all_zones = []

async def get_all_zones(refresh: bool = False) -> list[dict[Any, Any]]:
    if not refresh and all_zones:
        return all_zones
    
    async for zone in cloudflare_client.zones.list(
        account={"id": ocean.integration_config["cf_account_id"]}
    ):
        zone = zone.to_dict()
        zone['created_on'] = zone['created_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        zone['activated_on'] = zone['activated_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        zone['modified_on'] = zone['modified_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        all_zones.append(zone)

    return all_zones

# Required
# Listen to the resync event of all the kinds specified in the mapping inside port.
# Called each time with a different kind that should be returned from the source system.
@ocean.on_resync()
async def on_resync(kind: str) -> list[dict[Any, Any]]:
    if kind == "zone":
        print("Getting all zones")
        return await get_all_zones(refresh=True)

    if kind == "dns_record":
        print("Getting all records")
        all_dns_records = []
        for zone in all_zones:
            async for record in cloudflare_client.dns.records.list(
                zone_id = zone['id']
            ):
                record = record.to_dict()
                record['zone_id'] = zone['id']
                record['created_on'] = record['created_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                record['modified_on'] = record['modified_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                # record['comment_modified_on'] = record['comment_modified_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                # record['tags_modified_on'] = record['tags_modified_on'].strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                all_dns_records.append(record)

        print(all_dns_records[0])
        return all_dns_records

    # if kind == "cf-example-kind":
    #     return [
    #         {
    #             "my_custom_id": f"id_{x}",
    #             "my_custom_text": f"very long text with {x} in it",
    #             "my_special_score": x * 32 % 3,
    #             "my_component": f"component-{x}",
    #             "my_service": f"service-{x %2}",
    #             "my_enum": "VALID" if x % 2 == 0 else "FAILED",
    #         }
    #         for x in range(25)
    #     ]

    # return []


# The same sync logic can be registered for one of the kinds that are available in the mapping in port.
# @ocean.on_resync('project')
# async def resync_project(kind: str) -> list[dict[Any, Any]]:
#     # 1. Get all projects from the source system
#     # 2. Return a list of dictionaries with the raw data of the state
#     return [{"some_project_key": "someProjectValue", ...}]
#
# @ocean.on_resync('issues')
# async def resync_issues(kind: str) -> list[dict[Any, Any]]:
#     # 1. Get all issues from the source system
#     # 2. Return a list of dictionaries with the raw data of the state
#     return [{"some_issue_key": "someIssueValue", ...}]

# Optional
# Listen to the start event of the integration. Called once when the integration starts.
@ocean.on_start()
async def on_start() -> None:
    if (not ocean.integration_config["cf_api_token"]
        and not (ocean.integration_config["cf_email"]
                 and ocean.integration_config["cf_api_key"])
    ):
        raise ValueError("No Cloudflare API Token or Email and API Key provided")
    # Something to do when the integration starts
    # For example create a client to query 3rd party services - GitHub, Jira, etc...
    print("Starting cf integration")
