"""Configuration management for transit passenger survey pipeline."""

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationInfo, field_validator


class GeocodingZone(BaseModel):
    """Configuration for a single geocoding zone."""

    zone_type: str = Field(description="Output field prefix (e.g., 'tm1_taz')")
    shapefile_key: str = Field(description="Key in shapefiles dict")
    field_name: str = Field(description="Field name in shapefile")


class GeocodingDistance(BaseModel):
    """Configuration for a distance calculation pair."""

    from_: str = Field(alias="from", description="Source location")
    to: str = Field(description="Destination location")
    column: str = Field(description="Output column name")


class PipelineConfig(BaseModel):
    """Configuration for transit passenger survey processing pipeline.

    Loads from config/pipeline.yaml and validates that required paths exist.
    """

    # Shapefile paths
    shapefiles: dict[str, str] = Field(description="Paths to shapefiles for geocoding")

    # Geocoding zone configurations
    geocoding_zones: list[GeocodingZone] = Field(
        description="Zone types to geocode with their shapefile mappings"
    )

    # Geocoding distance pairs
    geocoding_distances: list[GeocodingDistance] = Field(
        description="Location pairs for distance calculations"
    )

    # Coordinate reference systems
    crs: dict[str, int] = Field(description="CRS EPSG codes")

    # Hive warehouse configuration
    hive_root: str = Field(description="Root path to Hive-partitioned storage")

    # Reference data paths
    canonical_route_crosswalk_path: str = Field(description="Path to canonical route crosswalk CSV")

    # Megaregion counties
    megaregion_counties: list[str] = Field(
        description="FIPS codes for Bay Area and adjacent counties"
    )

    # Validation thresholds
    validation_thresholds: dict[str, float] = Field(
        description="Thresholds for pipeline validation"
    )

    model_config = {"extra": "forbid", "populate_by_name": True}

    @field_validator("shapefiles")
    @classmethod
    def validate_shapefile_paths(cls, v: dict[str, str], info: ValidationInfo) -> dict[str, str]:
        """Validate that all shapefile paths exist, with M: to UNC fallback."""
        remote_name = info.data.get("remote_name")
        missing_files = []

        for name, path in v.items():
            # Handle .shp files, .zip files, and directories (for PUMA)
            path_obj = Path(path)
            shp_path = Path(f"{path}.shp")
            zip_path = Path(f"{path}.zip") if not path.endswith(".zip") else path_obj

            # Check if path exists as-is
            if path_obj.exists() or shp_path.exists() or zip_path.exists():
                continue

            # If M: path not found and remote_name provided, try UNC substitution
            if remote_name and path.startswith("M:"):
                unc_path = path.replace("M:", remote_name, 1)
                unc_path_obj = Path(unc_path)
                unc_shp_path = Path(f"{unc_path}.shp")
                unc_zip_path = (
                    Path(f"{unc_path}.zip") if not unc_path.endswith(".zip") else unc_path_obj
                )

                if unc_path_obj.exists() or unc_shp_path.exists() or unc_zip_path.exists():
                    # Update the dict with working UNC path
                    v[name] = unc_path
                    continue

            missing_files.append(f"{name}: {path}")

        if missing_files:
            msg = "Shapefile paths not found:\n" + "\n".join(missing_files)
            raise FileNotFoundError(msg)
        return v

    @field_validator("canonical_route_crosswalk_path")
    @classmethod
    def validate_crosswalk_path(cls, v: str) -> str:
        """Validate that canonical route crosswalk exists."""
        if not Path(v).exists():
            msg = f"Canonical route crosswalk not found: {v}"
            raise FileNotFoundError(msg)
        return v

    @field_validator("hive_root")
    @classmethod
    def validate_hive_path(cls, v: str) -> str:
        """Validate that Hive warehouse root exists."""
        if not Path(v).exists():
            msg = f"Hive warehouse root not found: {v}"
            raise FileNotFoundError(msg)
        return v

    @classmethod
    def from_yaml(cls, config_path: Path | str = "config/pipeline.yaml") -> "PipelineConfig":
        """Load configuration from YAML file.

        Args:
            config_path: Path to YAML configuration file
            (can be overridden by PIPELINE_CONFIG env var)

        Returns:
            Validated configuration instance
        """
        # Allow environment variable to override config path (useful for testing)
        config_path = os.getenv("PIPELINE_CONFIG", config_path)

        config_path = Path(config_path)
        if not config_path.exists():
            msg = f"Configuration file not found: {config_path}"
            raise FileNotFoundError(msg)

        with config_path.open() as f:
            config_data = yaml.safe_load(f)

        return cls(**config_data)


def get_config() -> PipelineConfig:
    """Get singleton configuration instance."""
    if not hasattr(get_config, "instance"):
        get_config.instance = PipelineConfig.from_yaml()
    return get_config.instance
