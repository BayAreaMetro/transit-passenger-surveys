"""Reference data loader for transit passenger survey pipeline.

Provides centralized access to canonical route crosswalk and shapefiles
used for geocoding and technology assignment.
"""

from typing import Optional

import geopandas as gpd
import polars as pl

from transit_passenger_tools.utils.config import get_config


class ReferenceData:
    """Singleton class for loading and caching reference data.
    
    Provides lazy-loaded access to:
    - Canonical route crosswalk (route names -> technology/operator)
    - Shapefiles for geocoding (TAZ, MAZ, counties, tracts, PUMA, stations)
    """

    _instance: Optional["ReferenceData"] = None
    _canonical_routes: pl.DataFrame | None = None
    _shapefiles: dict[str, gpd.GeoDataFrame] = {}

    def __new__(cls):
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def canonical_routes(self) -> pl.DataFrame:
        """Get canonical route crosswalk as Polars DataFrame.
        
        Loads from fixtures/canonical_route_crosswalk.csv on first access.
        Contains mappings from survey_route_name to:
        - canonical_route
        - canonical_operator  
        - operator_detail
        - technology
        - technology_detail
        
        Returns:
            Polars DataFrame with ~10,137 rows of route mappings.
        """
        if self._canonical_routes is None:
            config = get_config()
            self._canonical_routes = pl.read_csv(config.canonical_route_crosswalk_path)
        return self._canonical_routes

    def get_route_technology(self, operator: str, route: str) -> dict[str, str]:
        """Get technology and operator details for a route.
        
        Args:
            operator: Canonical operator name (e.g., "BART", "AC TRANSIT")
            route: Route name from survey
            
        Returns:
            Dictionary with keys: canonical_operator, technology, operator_detail
            
        Raises:
            ValueError: If route not found in canonical crosswalk
        """
        result = self.canonical_routes.filter(
            (pl.col("canonical_operator") == operator) &
            (pl.col("survey_route_name") == route)
        )

        if result.height == 0:
            raise ValueError(
                f"Route not found in canonical crosswalk: operator='{operator}', route='{route}'"
            )

        row = result.row(0, named=True)
        return {
            "canonical_operator": row["canonical_operator"],
            "technology": row["technology"],
            "operator_detail": row.get("operator_detail", row["canonical_operator"])
        }

    def get_zone_shapefile(self, zone_type: str) -> gpd.GeoDataFrame:
        """Get shapefile for a specific zone type.
        
        Loads and caches shapefiles on first access. Shapefiles are used for
        spatial joins to assign geographic zones to survey locations.
        
        Args:
            zone_type: Type of zone to load. One of:
                - 'tm1_taz': Travel Model 1 TAZ zones
                - 'tm2_taz': Travel Model 2 TAZ zones
                - 'tm2_maz': Travel Model 2 MAZ zones
                - 'counties': County boundaries
                - 'tracts': Census tracts
                - 'puma': PUMA boundaries
                - 'stations': Rail station points
        
        Returns:
            GeoDataFrame with zone geometry and attributes
            
        Raises:
            KeyError: If zone_type not recognized
            FileNotFoundError: If shapefile path doesn't exist
        """
        if zone_type not in self._shapefiles:
            config = get_config()
            shapefile_path = config.shapefiles[zone_type]

            # Load shapefile
            gdf = gpd.read_file(shapefile_path)

            # Transform to NAD83 California Zone 6 Feet for consistent CRS
            if gdf.crs.to_epsg() != config.crs["nad83_ca_zone6"]:
                gdf = gdf.to_crs(epsg=config.crs["nad83_ca_zone6"])

            self._shapefiles[zone_type] = gdf

        return self._shapefiles[zone_type]

    @property
    def shapefiles(self) -> dict[str, gpd.GeoDataFrame]:
        """Get all loaded shapefiles as dictionary.
        
        Returns:
            Dictionary mapping zone_type -> GeoDataFrame
        """
        return self._shapefiles


def get_reference_data() -> ReferenceData:
    """Get singleton ReferenceData instance.
    
    Returns:
        Shared ReferenceData instance
    """
    return ReferenceData()
