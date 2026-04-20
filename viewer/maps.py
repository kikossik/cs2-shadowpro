from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MapConfig:
    name: str            # "de_mirage"
    display_name: str    # "Mirage"
    pos_x: float
    pos_y: float
    scale: float
    has_lower_level: bool = False
    lower_level_max_z: float = -1_000_000.0

    def world_to_radar_px(self, wx: float, wy: float) -> tuple[float, float]:
        """World coords → radar image pixel coords (0,0 = top-left of image)."""
        rx = (wx - self.pos_x) / self.scale
        ry = (self.pos_y - wy) / self.scale   # game Y up → image Y down
        return rx, ry

    def world_r_to_px(self, radius_wu: float, img_w: int, disp_size: int) -> int:
        """World-unit radius → display pixels."""
        return max(2, int(radius_wu / self.scale * disp_size / img_w))

    def is_lower(self, z: float) -> bool:
        return self.has_lower_level and z <= self.lower_level_max_z

    @property
    def radar_path(self) -> Path:
        return Path.home() / ".awpy" / "maps" / f"{self.name}.png"

    @property
    def lower_radar_path(self) -> Path | None:
        if self.has_lower_level:
            return Path.home() / ".awpy" / "maps" / f"{self.name}_lower.png"
        return None


# Values from ~/.awpy/maps/map-data.json
MAPS: dict[str, MapConfig] = {
    "de_ancient":  MapConfig("de_ancient",  "Ancient",  -2953, 2164, 5.0),
    "de_anubis":   MapConfig("de_anubis",   "Anubis",   -2796, 3328, 5.22),
    "de_dust2":    MapConfig("de_dust2",    "Dust 2",   -2476, 3239, 4.4),
    "de_inferno":  MapConfig("de_inferno",  "Inferno",  -2087, 3870, 4.9),
    "de_mirage":   MapConfig("de_mirage",   "Mirage",   -3230, 1713, 5.0),
    "de_nuke":     MapConfig("de_nuke",     "Nuke",     -3453, 2887, 7.0,
                             has_lower_level=True, lower_level_max_z=-495.0),
    "de_overpass": MapConfig("de_overpass", "Overpass", -4831, 1781, 5.2),
}


def detect_map(demo_path: Path) -> MapConfig | None:
    """Read the demo header to find the map name, return the matching MapConfig."""
    try:
        from awpy import Demo
        dem = Demo(path=str(demo_path))
        dem.parse_header()
        map_name = dem.header.get("map_name", "")
        return MAPS.get(map_name)
    except Exception:
        return None
