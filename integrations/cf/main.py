import cloudflare
from typing import Any, Union
from datetime import datetime
from loguru import logger

from port_ocean.context.ocean import ocean
from port_ocean.core.ocean_types import AsyncIterator
from cloudflare import AsyncCloudflare
from integration import ObjectKind


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
all_zones = list[dict[str, Any]]()
all_tunnels = list[dict[str, Any]]()
all_access_applications = list[dict[str, Any]]()


def convert_datetime_to_string(
    data: Union[dict[Any, Any], list[Any]]
) -> Union[dict[Any, Any], list[Any]]:
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            elif isinstance(value, (dict, list)):
                data[key] = convert_datetime_to_string(value)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            if isinstance(item, datetime):
                data[index] = item.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            elif isinstance(item, (dict, list)):
                data[index] = convert_datetime_to_string(item)
    return data


async def get_all_zones(refresh: bool = False) -> list[dict[Any, Any]]:
    global all_zones
    if not refresh and len(all_zones) > 0:
        return all_zones
    elif refresh:
        all_zones = []

    logger.info("Getting all zones")

    async for zone in cloudflare_client.zones.list(
        account={"id": ocean.integration_config["cf_account_id"]}
    ):
        zone_dict = zone.to_dict()
        all_zones.append(zone_dict)

    return convert_datetime_to_string(all_zones)


async def get_all_tunnels(refresh: bool = False) -> list[dict[Any, Any]]:
    global all_tunnels
    if not refresh and len(all_tunnels) > 0:
        return all_tunnels
    elif refresh:
        all_tunnels = []

    logger.info("Getting all tunnels")

    async for tunnel in cloudflare_client.zero_trust.tunnels.list(
        account_id=ocean.integration_config["cf_account_id"]
    ):
        all_tunnels.append(tunnel.to_dict())

    return convert_datetime_to_string(all_tunnels)


async def get_all_access_applications(refresh: bool = False) -> list[dict[Any, Any]]:
    global all_access_applications
    if not refresh and len(all_access_applications) > 0:
        return all_access_applications
    elif refresh:
        all_access_applications = []

    logger.info("Getting all Zero Trust Access Applications")

    async for access_application in cloudflare_client.zero_trust.access.applications.list(
        account_id=ocean.integration_config["cf_account_id"]
    ):
        all_access_applications.append(access_application.to_dict())

    return convert_datetime_to_string(all_access_applications)


@ocean.on_resync(ObjectKind.ACCOUNT)
async def resync_account(kind: str) -> list[dict[Any, Any]]:
    logger.info("Getting account")
    account = await cloudflare_client.accounts.get(
        account_id=ocean.integration_config["cf_account_id"]
    )
    return convert_datetime_to_string([account])


@ocean.on_resync(ObjectKind.ZONES)
async def resync_zone(kind: str) -> list[dict[Any, Any]]:
    return await get_all_zones(refresh=True)


@ocean.on_resync(ObjectKind.DNS_RECORDS)
async def resync_dns_record(kind: str) -> AsyncIterator[dict[Any, Any]]:
    logger.info("Getting all records")
    zones = await get_all_zones()
    for zone in zones:
        all_dns_records = []
        logger.info(f"Getting all records for zone: {zone['name']}")
        async for record in cloudflare_client.dns.records.list(zone_id=zone["id"]):
            record_dict = record.to_dict()
            record_dict["zone_id"] = zone["id"]
            all_dns_records.append(record_dict)
        yield convert_datetime_to_string(all_dns_records)


@ocean.on_resync(ObjectKind.ZEROTRUST_ACCESS_APPLICATIONS)
async def resync_zerotrust_access_application(kind: str) -> list[dict[Any, Any]]:
    return await get_all_access_applications(refresh=True)


@ocean.on_resync(ObjectKind.ZEROTRUST_TUNNELS)
async def resync_zerotrust_tunnel(kind: str) -> list[dict[Any, Any]]:
    return await get_all_tunnels(refresh=True)


@ocean.on_resync(ObjectKind.ZEROTRUST_TUNNEL_CONFIGURATIONS)
async def resync_zerotrust_tunnel_routes(kind: str) -> AsyncIterator[dict[Any, Any]]:
    logger.info("Getting all Zero Trust Tunnel Configurations")
    tunnels = await get_all_tunnels()
    for tunnel in tunnels:
        try:
            logger.info(f"Getting configuration for tunnel: {tunnel['name']}")
            config = await cloudflare_client.zero_trust.tunnels.configurations.get(
                account_id=ocean.integration_config["cf_account_id"],
                tunnel_id=tunnel["id"]
            )
            yield [config]
        except cloudflare.NotFoundError as e:
            logger.info("Configuration for tunnel was not found")
            yield []


@ocean.router.post("/webhook")
async def handle_cloudflare_webhook(data: dict[str, Any]) -> None:
    logger.info(
        f"Processing Cloudflare webhook for event type: {data['data']['alert_name']}"
    )

    kind = data["data"]["alert_name"]

    if kind == "tunnel_health_event":
        logger.info(
            f"Processing Cloudflare tunnel health event for tunnel: {data['data']['tunnel_name']}"
        )
        tunnel = await cloudflare_client.zero_trust.tunnels.get(
            tunnel_id=data["data"]["tunnel_id"], account_id=data["account_id"]
        )
        await ocean.register_raw(ObjectKind.ZEROTRUST_TUNNELS, convert_datetime_to_string([tunnel.to_dict()]))


# Optional
# Listen to the start event of the integration. Called once when the integration starts.
@ocean.on_start()
async def on_start() -> None:
    if not ocean.integration_config["cf_api_token"] and not (
        ocean.integration_config["cf_email"] and ocean.integration_config["cf_api_key"]
    ):
        raise ValueError("No Cloudflare API Token or Email and API Key provided")
    # Something to do when the integration starts
    # For example create a client to query 3rd party services - GitHub, Jira, etc...
    logger.info("Starting cf integration")
