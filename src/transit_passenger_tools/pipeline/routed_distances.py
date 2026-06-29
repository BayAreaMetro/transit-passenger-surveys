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
from typing import NamedTuple

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
        "geometry_orig_first_board_routed",
        "geometry_last_alight_dest_routed",
        "access_leg_implausible",
        "egress_leg_implausible",
    ],
    optional_inputs=[
        "access_mode",
        "egress_mode",
    ],
)

# Meters → miles conversion factor
_METERS_TO_MILES = 1.0 / 1609.344

# Per-mode routed-distance ceilings (miles) beyond which an access/egress leg is
# implausible for its reported mode — almost always a mis-geocoded endpoint or a
# mis-coded mode (common in self-reported online surveys). Modes not listed here
# (drive-like) are never flagged. Walk > 2 mi and Bike > 10 mi are conservative,
# high-confidence cutoffs; tune here.
IMPLAUSIBLE_MILES: dict[str, float] = {"Walk": 2.0, "Bike": 10.0}

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

# OSRM query params: request the full route geometry as an encoded polyline
# (precision 5). Stored verbatim in the *_routed geometry columns.
_GEOMETRY_QUERY = "overview=full&geometries=polyline"


class RouteResult(NamedTuple):
    """A single routed OD pair: distance in meters and encoded-polyline geometry."""

    distance_m: float | None
    geometry: str | None


_EMPTY_RESULT = RouteResult(None, None)


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
) -> RouteResult:
    """Route a single OD pair; return its distance (meters) and geometry."""
    try:
        resp = client.Route(
            coordinates=[[origin_lon, origin_lat], [dest_lon, dest_lat]],
            overview="full",
            geometries="polyline",
        )
    except (requests.RequestException, OSError, KeyError, RuntimeError):
        return _EMPTY_RESULT
    routes = resp.get("routes", [])
    if routes:
        return RouteResult(routes[0].get("distance"), routes[0].get("geometry"))
    return _EMPTY_RESULT


async def _route_async(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
    profile: str,
    origin_lon: float,
    origin_lat: float,
    dest_lon: float,
    dest_lat: float,
) -> RouteResult:
    """Route a single OD pair asynchronously; return distance and geometry."""
    req_url = (
        f"{url}/route/v1/{profile}/"
        f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}?{_GEOMETRY_QUERY}"
    )
    async with sem:
        try:
            async with session.get(req_url) as resp:
                if resp.ok:
                    data = await resp.json()
                    routes = data.get("routes", [])
                    if routes:
                        return RouteResult(
                            routes[0].get("distance"),
                            routes[0].get("geometry"),
                        )
        except (aiohttp.ClientError, TimeoutError, OSError):
            pass
    return _EMPTY_RESULT


async def _route_profile_group_async(
    server_url: str,
    profile: str,
    lons_from: list[float],
    lats_from: list[float],
    lons_to: list[float],
    lats_to: list[float],
    *,
    desc: str = "Routing",
) -> list[RouteResult]:
    """Route all OD pairs for one profile using async I/O."""
    n = len(lons_from)
    sem = asyncio.Semaphore(_MAX_CONCURRENCY)
    connector = aiohttp.TCPConnector(
        limit=_MAX_CONCURRENCY, limit_per_host=_MAX_CONCURRENCY,
    )
    timeout = aiohttp.ClientTimeout(total=60, sock_read=10)
    results: list[RouteResult] = [_EMPTY_RESULT] * n

    async def _indexed_route(
        session: aiohttp.ClientSession, idx: int,
        o_lon: float, o_lat: float, d_lon: float, d_lat: float,
    ) -> tuple[int, RouteResult]:
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
    dist_col: str,
    geom_col: str,
    server_url: str,
) -> tuple[pl.Series, pl.Series]:
    """Route one leg (access or egress) and return (distance, geometry) Series.

    Distance is in miles; geometry is the OSRM-encoded polyline string.
    Rows are grouped by OSRM profile to minimise client re-creation.
    Rows with missing coordinates get null in both Series.
    """
    n = len(df)
    dist_result = pl.Series(dist_col, [None] * n, dtype=pl.Float64)
    geom_result = pl.Series(geom_col, [None] * n, dtype=pl.Utf8)

    coord_cols = [from_lat, from_lon, to_lat, to_lon]
    missing = [c for c in coord_cols if c not in df.columns]
    if missing:
        logger.debug("Skipping %s — missing columns: %s", dist_col, missing)
        return dist_result, geom_result

    has_coords = df.select(
        pl.all_horizontal([pl.col(c).is_not_null() for c in coord_cols]).alias("_valid")
    )["_valid"]
    valid_idx = [i for i, v in enumerate(has_coords) if v]
    if not valid_idx:
        return dist_result, geom_result

    valid_df = df[valid_idx]
    profile_groups = _resolve_profiles(valid_df, mode_col)
    distances: list[float | None] = [None] * len(valid_df)
    geometries: list[str | None] = [None] * len(valid_df)

    for profile, row_indices in profile_groups.items():
        lons_from = [valid_df[from_lon][i] for i in row_indices]
        lats_from = [valid_df[from_lat][i] for i in row_indices]
        lons_to = [valid_df[to_lon][i] for i in row_indices]
        lats_to = [valid_df[to_lat][i] for i in row_indices]

        group_results = asyncio.run(
            _route_profile_group_async(
                server_url, profile, lons_from, lats_from, lons_to, lats_to,
                desc=f"{dist_col} ({profile})",
            )
        )

        for idx, res in zip(row_indices, group_results, strict=True):
            if res.distance_m is not None:
                distances[idx] = res.distance_m * _METERS_TO_MILES
            geometries[idx] = res.geometry

    dist_list = dist_result.to_list()
    geom_list = geom_result.to_list()
    for pos, orig_idx in enumerate(valid_idx):
        dist_list[orig_idx] = distances[pos]
        geom_list[orig_idx] = geometries[pos]

    return (
        pl.Series(dist_col, dist_list, dtype=pl.Float64),
        pl.Series(geom_col, geom_list, dtype=pl.Utf8),
    )


def add_implausible_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Add boolean access/egress implausibility flags from mode + routed distance.

    A leg is flagged when its routed distance exceeds the per-mode ceiling in
    :data:`IMPLAUSIBLE_MILES` (Walk > 2 mi, Bike > 10 mi). The flag is null-safe
    (missing mode, distance, column, or an un-listed mode → ``False``) and pure:
    it derives only from columns already present, so it can be (re)computed on
    warehouse data without routing.
    """

    def flag(mode_col: str, dist_col: str) -> pl.Expr:
        if mode_col in df.columns and dist_col in df.columns:
            # Map each mode to its ceiling (un-listed modes -> null -> never flagged).
            ceiling = pl.col(mode_col).replace_strict(
                IMPLAUSIBLE_MILES, default=None, return_dtype=pl.Float64
            )
            return (pl.col(dist_col) > ceiling).fill_null(value=False)
        return pl.lit(value=False)

    return df.with_columns(
        flag("access_mode", "distance_orig_first_board_routed").alias(
            "access_leg_implausible"
        ),
        flag("egress_mode", "distance_last_alight_dest_routed").alias(
            "egress_leg_implausible"
        ),
    )


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
        return add_implausible_flags(
            df.with_columns(
                null_col.alias("distance_orig_first_board_routed"),
                null_col.alias("distance_last_alight_dest_routed"),
                pl.lit(None).cast(pl.Utf8).alias("geometry_orig_first_board_routed"),
                pl.lit(None).cast(pl.Utf8).alias("geometry_last_alight_dest_routed"),
            )
        )

    logger.info("Computing routed distances via OSRM at %s", server_url)

    # Access leg: origin → first boarding
    access_dist, access_geom = _route_leg(
        df,
        from_lat="orig_lat",
        from_lon="orig_lon",
        to_lat="first_board_lat",
        to_lon="first_board_lon",
        mode_col="access_mode",
        dist_col="distance_orig_first_board_routed",
        geom_col="geometry_orig_first_board_routed",
        server_url=server_url,
    )

    # Egress leg: last alighting → destination
    egress_dist, egress_geom = _route_leg(
        df,
        from_lat="last_alight_lat",
        from_lon="last_alight_lon",
        to_lat="dest_lat",
        to_lon="dest_lon",
        mode_col="egress_mode",
        dist_col="distance_last_alight_dest_routed",
        geom_col="geometry_last_alight_dest_routed",
        server_url=server_url,
    )

    return add_implausible_flags(
        df.with_columns(access_dist, access_geom, egress_dist, egress_geom)
    )
