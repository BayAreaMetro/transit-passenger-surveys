"""Tests for the routed-distance report script (polyline decoding + map data)."""

import importlib.util
from pathlib import Path

import polars as pl
import pytest

# The report lives in scripts/ which is not on pythonpath; load it by path.
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "report_routed_distances.py"
_spec = importlib.util.spec_from_file_location("report_routed_distances", _SCRIPT)
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


def _encode_value(num: int) -> str:
    """Encode one signed integer the way the OSRM/Google polyline format does."""
    num = ~(num << 1) if num < 0 else (num << 1)
    chunks = []
    while num >= 0x20:
        chunks.append((0x20 | (num & 0x1F)) + 63)
        num >>= 5
    chunks.append(num + 63)
    return "".join(chr(c) for c in chunks)


def encode_polyline(coords: list[tuple[float, float]], precision: int = 5) -> str:
    """Inverse of ``report.decode_polyline`` — used to build test fixtures."""
    factor = 10 ** precision
    out, plat, plon = [], 0, 0
    for lat, lon in coords:
        ilat, ilon = round(lat * factor), round(lon * factor)
        out.append(_encode_value(ilat - plat))
        out.append(_encode_value(ilon - plon))
        plat, plon = ilat, ilon
    return "".join(out)


class TestDecodePolyline:
    """Verify the precision-5 OSRM/Google polyline decoder."""

    def test_known_example(self):
        """Canonical example from the Google polyline spec (precision 5)."""
        decoded = report.decode_polyline("_p~iF~ps|U_ulLnnqC_mqNvxq`@")
        assert decoded == [
            (38.5, -120.2),
            (40.7, -120.95),
            (43.252, -126.453),
        ]

    def test_empty_string(self):
        """Empty input decodes to an empty list."""
        assert report.decode_polyline("") == []

    def test_none(self):
        """None input decodes to an empty list (no error)."""
        assert report.decode_polyline(None) == []

    def test_single_point(self):
        """A single coordinate round-trips through the decoder."""
        # Encoded form of (38.5, -120.2) alone.
        decoded = report.decode_polyline("_p~iF~ps|U")
        assert len(decoded) == 1
        assert decoded[0] == pytest.approx((38.5, -120.2))


class TestBuildTraceLinks:
    """Verify route aggregation into per-mode weighted links."""

    def test_buckets_by_mode(self):
        """Access/egress legs land in their mode's bucket; Drive is the fallback."""
        line = "_p~iF~ps|U_ulLnnqC_mqNvxq`@"  # 3 vertices -> 2 links
        traces = pl.DataFrame({
            "access_mode": ["Walk", None],
            "egress_mode": ["Bike", "PNR"],
            "geometry_orig_first_board_routed": [line, line],
            "geometry_last_alight_dest_routed": [line, line],
            "distance_orig_first_board_routed": [0.5, None],
            "distance_last_alight_dest_routed": [0.5, 5.0],
        })

        links, stats = report.build_trace_links(traces)

        assert set(links) == set(report._BUCKETS)
        # Each leg yields 2 unique links. Walk: one access leg; Bike: one egress
        # leg; Drive: null-access + PNR-egress are the SAME geometry, so its 2
        # links aggregate (count 2) rather than duplicating.
        assert len(links["Walk"]) == 2
        assert len(links["Bike"]) == 2
        assert len(links["Drive"]) == 2
        # Each link is [lat1, lon1, lat2, lon2, count].
        walk = sorted(links["Walk"])
        assert walk[0] == [38.5, -120.2, 40.7, -120.95, 1]
        # The overlapping Drive links carry the aggregated count.
        assert all(link[4] == 2 for link in links["Drive"])
        # Stats: Drive drew 2 routes (null-access leg + PNR-egress leg) that
        # aggregate to 2 distinct links. Nothing is dropped.
        assert stats["Walk"]["routes"] == 1
        assert stats["Drive"]["routes"] == 2
        assert stats["Drive"]["links"] == 2

    def test_long_walk_and_bike_split_into_flagged_bucket(self):
        """Walk/bike legs over their ceilings move to the flagged layer."""
        line = "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
        traces = pl.DataFrame({
            "access_mode": ["Walk", "Walk", "Bike", "Bike"],
            "egress_mode": [None, None, None, None],
            "geometry_orig_first_board_routed": [line, line, line, line],
            "geometry_last_alight_dest_routed": [None, None, None, None],
            # walk: plausible, implausible; bike: plausible (<10), implausible (>10)
            "distance_orig_first_board_routed": [0.5, 9.0, 4.0, 12.0],
            "distance_last_alight_dest_routed": [None, None, None, None],
        })

        _links, stats = report.build_trace_links(traces)
        assert stats["Walk"]["routes"] == 1   # the 0.5 mi walk
        assert stats["Bike"]["routes"] == 1   # the 4 mi bike
        assert stats[report._FLAG_BUCKET]["routes"] == 2  # 9 mi walk + 12 mi bike

    def test_empty_geometry_skipped(self):
        """Null/empty geometry produces no links and never errors."""
        traces = pl.DataFrame({
            "access_mode": ["Walk"],
            "egress_mode": ["Bike"],
            "geometry_orig_first_board_routed": [None],
            "geometry_last_alight_dest_routed": [""],
            "distance_orig_first_board_routed": [None],
            "distance_last_alight_dest_routed": [None],
        })

        links, stats = report.build_trace_links(traces)
        assert all(len(v) == 0 for v in links.values())
        assert all(s["links"] == 0 and s["routes"] == 0 for s in stats.values())

    def test_reverse_direction_aggregates(self):
        """A link traversed in opposite directions counts as one undirected link."""
        a, b = (38.5, -120.2), (40.7, -120.95)
        fwd = encode_polyline([a, b])
        rev = encode_polyline([b, a])
        traces = pl.DataFrame({
            "access_mode": ["Drive", "Drive"],
            "egress_mode": [None, None],
            "geometry_orig_first_board_routed": [fwd, rev],
            "geometry_last_alight_dest_routed": [None, None],
            "distance_orig_first_board_routed": [3.0, 3.0],
            "distance_last_alight_dest_routed": [None, None],
        })

        links, _stats = report.build_trace_links(traces)
        assert len(links["Drive"]) == 1
        assert links["Drive"][0][4] == 2


class TestComputeView:
    """Verify the deck.gl initial-view derivation."""

    def test_center_and_zoom_fit_bounds(self):
        """Center is the bbox midpoint; zoom is clamped to a sane range."""
        links = {"Walk": [[37.0, -122.5, 37.4, -122.1, 1]], "Bike": [], "Drive": []}
        view = report.compute_view(links)
        assert view["latitude"] == pytest.approx(37.2)
        assert view["longitude"] == pytest.approx(-122.3)
        assert 3.0 <= view["zoom"] <= 15.0

    def test_empty_falls_back_to_bay_area(self):
        """With no links, the view falls back to a Bay Area default."""
        view = report.compute_view({b: [] for b in report._BUCKETS})
        assert view["latitude"] == pytest.approx(37.8)
        assert view["longitude"] == pytest.approx(-122.3)


class TestRenderMap:
    """Verify the map HTML fragment rendering."""

    def test_no_traces_note(self):
        """All-empty buckets render the 'no traces' note, not a map div."""
        html = report.render_map({b: [] for b in report._BUCKETS})
        assert "No routed traces available" in html
        assert 'id="map"' not in html

    def test_map_fragment_has_payload(self):
        """Non-empty buckets render a map div with the JSON payload injected."""
        stats = {
            "Walk": {"routes": 2, "links": 1},
            "Bike": {"routes": 0, "links": 0},
            "Drive": {"routes": 0, "links": 0},
        }
        html = report.render_map(
            {"Walk": [[38.5, -120.2, 40.7, -120.95, 3]], "Bike": [], "Drive": []},
            stats,
        )
        assert 'id="map"' in html
        assert "deck.LineLayer" in html
        assert "LINKS_JSON_PLACEHOLDER" not in html  # placeholder replaced
        assert "VIEW_JSON_PLACEHOLDER" not in html  # view placeholder replaced
        assert "[38.5,-120.2,40.7,-120.95,3]" in html  # compact JSON, no spaces
        # Legend shows a toggle checkbox + route/link counts for the populated mode.
        assert 'id="chk-Walk"' in html
        assert "2 routes, 1 unique links" in html
