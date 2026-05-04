"""Routed network distances via OSRM for access and egress legs.

Computes routed (network) distances from origin to first boarding and from
last alighting to destination using an OSRM HTTP server. Distances are
mode-aware: the OSRM profile (driving / foot / bicycle) is selected based
on the access/egress mode reported by the respondent.

The OSRM server URL is read from ``PipelineConfig.osrm_server_url``.
When the URL is ``None`` the step is skipped and null columns are returned.
When the server is unreachable an error is raised.
"""

import asyncio
import logging

import aiohttp
import polars as pl
import requests
from osrm.http_client import OSRM_HTTP
from tqdm import tqdm

from transit_passenger_tools.config import get_config
from transit_passenger_tools.models import FieldDependencies

logger = logging.getLogger(__name__)

FIELD_DEPENDENCIES = FieldDependencies(
    inputs=[
        "orig_lat",
        "orig_lon",
        "first_board_lat",
        "first_board_lon",
        "last_alight_lat",
        "last_alight_lon",
        "dest_lat",
        "dest_lon",
    ],
    outputs=[
        "distance_orig_first_board_routed",
        "distance_last_alight_dest_routed",
    ],
    optional_inputs=[
        "access_mode",
        "egress_mode",
    ],
)

# Meters → miles conversion factor
_METERS_TO_MILES = 1.0 / 1609.344

# Map AccessEgressMode values → OSRM profile names.
# The OSRM server is behind an nginx reverse proxy that exposes
# multiple profiles on a shared port.
_MODE_TO_PROFILE: dict[str | None, str] = {
    "Walk": "foot",
    "Bike": "bicycle",
    "PNR": "driving",
    "KNR": "driving",
    "TNC": "driving",
    "Transit": "driving",
    "Other": "driving",
    None: "driving",  # fallback when mode is unknown
}

_DEFAULT_PROFILE = "driving"
_MAX_CONCURRENCY = 32


def _create_osrm_client(server_url: str, profile: str) -> OSRM_HTTP:
    """Create an OSRM_HTTP client, bypassing the root-path health check.

    The default ``OSRM_HTTP.__init__`` pings ``{base_url}/`` to verify
    connectivity.  Nginx reverse-proxy setups (like MTC's) return 404 on
    the root path even though route endpoints work fine.  This helper
    constructs the client without that check.
    """
    client = object.__new__(OSRM_HTTP)
    client._requests = requests  # noqa: SLF001
    client.base_url = server_url.rstrip("/")
    client.profile = profile
    client.timeout = 30.0
    client.session = requests.Session()
    return client


def _route_one(
    client: OSRM_HTTP,
    origin_lon: float,
    origin_lat: float,
    dest_lon: float,
    dest_lat: float,
) -> float | None:
    """Route a single OD pair and return distance in meters, or None."""
    try:
        resp = client.Route(
            coordinates=[[origin_lon, origin_lat], [dest_lon, dest_lat]],
        )
    except (requests.RequestException, OSError, KeyError, RuntimeError):
        return None
    routes = resp.get("routes", [])
    if routes:
        return routes[0].get("distance")
    return None


async def _route_async(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    profile: str,
    origin_lon: float,
    origin_lat: float,
    dest_lon: float,
    dest_lat: float,
) -> float | None:
    """Route a single OD pair asynchronously, return distance in meters."""
    req_url = (
        f"{url}/route/v1/{profile}/"
        f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}?overview=false"
    )
    async with sem:
        try:
            async with session.get(req_url) as resp:
                if resp.ok:
                    data = await resp.json()
                    routes = data.get("routes", [])
                    if routes:
                        return routes[0].get("distance")
        except (aiohttp.ClientError, TimeoutError, OSError):
            pass
    return None


async def _route_profile_group_async(
    server_url: str,
    profile: str,
    lons_from: list[float],
    lats_from: list[float],
    lons_to: list[float],
    lats_to: list[float],
    *,
    desc: str = "Routing",
) -> list[float | None]:
    """Route all OD pairs for one profile using async I/O."""
    n = len(lons_from)
    sem = asyncio.Semaphore(_MAX_CONCURRENCY)
    connector = aiohttp.TCPConnector(
        limit=_MAX_CONCURRENCY, limit_per_host=_MAX_CONCURRENCY,
    )
    timeout = aiohttp.ClientTimeout(total=60, sock_read=10)
    results: list[float | None] = [None] * n

    async def _indexed_route(
        session: aiohttp.ClientSession, idx: int,
        o_lon: float, o_lat: float, d_lon: float, d_lat: float,
    ) -> tuple[int, float | None]:
        val = await _route_async(
            session, sem, server_url, profile, o_lon, o_lat, d_lon, d_lat,
        )
        return idx, val

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            asyncio.ensure_future(
                _indexed_route(session, i, o_lon, o_lat, d_lon, d_lat),
            )
            for i, (o_lon, o_lat, d_lon, d_lat) in enumerate(
                zip(lons_from, lats_from, lons_to, lats_to, strict=True),
            )
        ]
        with tqdm(total=n, desc=desc, unit="route") as pbar:
            for fut in asyncio.as_completed(tasks):
                idx, val = await fut
                results[idx] = val
                pbar.update(1)
    return results


def _resolve_profiles(
    valid_df: pl.DataFrame, mode_col: str | None,
) -> dict[str, list[int]]:
    """Group valid-row indices by OSRM profile name."""
    if mode_col and mode_col in valid_df.columns:
        profiles = (
            valid_df[mode_col]
            .map_elements(
                lambda m: _MODE_TO_PROFILE.get(m, _DEFAULT_PROFILE),
                return_dtype=pl.Utf8,
                skip_nulls=False,
            )
            .fill_null(_DEFAULT_PROFILE)
            .to_list()
        )
    else:
        profiles = [_DEFAULT_PROFILE] * len(valid_df)

    groups: dict[str, list[int]] = {}
    for i, prof in enumerate(profiles):
        groups.setdefault(prof, []).append(i)
    return groups


def _route_leg(
    df: pl.DataFrame,
    from_lat: str,
    from_lon: str,
    to_lat: str,
    to_lon: str,
    mode_col: str | None,
    output_col: str,
    server_url: str,
) -> pl.Series:
    """Route one leg (access or egress) and return a distance Series (miles).

    Rows are grouped by OSRM profile to minimise client re-creation.
    Rows with missing coordinates get null.
    """
    n = len(df)
    result = pl.Series(output_col, [None] * n, dtype=pl.Float64)

    coord_cols = [from_lat, from_lon, to_lat, to_lon]
    missing = [c for c in coord_cols if c not in df.columns]
    if missing:
        logger.debug("Skipping %s — missing columns: %s", output_col, missing)
        return result

    has_coords = df.select(
        pl.all_horizontal([pl.col(c).is_not_null() for c in coord_cols]).alias("_valid")
    )["_valid"]
    valid_idx = [i for i, v in enumerate(has_coords) if v]
    if not valid_idx:
        return result

    valid_df = df[valid_idx]
    profile_groups = _resolve_profiles(valid_df, mode_col)
    distances: list[float | None] = [None] * len(valid_df)

    for profile, row_indices in profile_groups.items():
        lons_from = [valid_df[from_lon][i] for i in row_indices]
        lats_from = [valid_df[from_lat][i] for i in row_indices]
        lons_to = [valid_df[to_lon][i] for i in row_indices]
        lats_to = [valid_df[to_lat][i] for i in row_indices]

        group_dists = asyncio.run(
            _route_profile_group_async(
                server_url, profile, lons_from, lats_from, lons_to, lats_to,
                desc=f"{output_col} ({profile})",
            )
        )

        for idx, dist_m in zip(row_indices, group_dists, strict=True):
            if dist_m is not None:
                distances[idx] = dist_m * _METERS_TO_MILES

    result_list = result.to_list()
    for pos, orig_idx in enumerate(valid_idx):
        result_list[orig_idx] = distances[pos]

    return pl.Series(output_col, result_list, dtype=pl.Float64)


def compute_routed_distances(df: pl.DataFrame) -> pl.DataFrame:
    """Compute routed network distances for access and egress legs.

    Reads ``osrm_server_url`` from pipeline config.  When the URL is
    ``None`` the two output columns are added as all-null and the function
    returns immediately.

    Args:
        df: DataFrame with lat/lon and optional access_mode/egress_mode columns.

    Returns:
        DataFrame with ``distance_orig_first_board_routed`` and
        ``distance_last_alight_dest_routed`` columns added.

    Raises:
        ConnectionError: If the OSRM server is unreachable.
    """
    config = get_config()
    server_url = config.osrm_server_url

    null_col = pl.lit(None).cast(pl.Float64)

    if server_url is None:
        logger.info("OSRM server URL not configured — skipping routed distances")
        return df.with_columns(
            null_col.alias("distance_orig_first_board_routed"),
            null_col.alias("distance_last_alight_dest_routed"),
        )

    logger.info("Computing routed distances via OSRM at %s", server_url)

    # Access leg: origin → first boarding
    access_dist = _route_leg(
        df,
        from_lat="orig_lat",
        from_lon="orig_lon",
        to_lat="first_board_lat",
        to_lon="first_board_lon",
        mode_col="access_mode",
        output_col="distance_orig_first_board_routed",
        server_url=server_url,
    )

    # Egress leg: last alighting → destination
    egress_dist = _route_leg(
        df,
        from_lat="last_alight_lat",
        from_lon="last_alight_lon",
        to_lat="dest_lat",
        to_lon="dest_lon",
        mode_col="egress_mode",
        output_col="distance_last_alight_dest_routed",
        server_url=server_url,
    )

    return df.with_columns(access_dist, egress_dist)
