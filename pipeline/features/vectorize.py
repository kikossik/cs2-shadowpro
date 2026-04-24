"""Convert a feature blob into a dense numeric vector for ANN similarity search."""
from __future__ import annotations

_SITUATIONS = (
    "retake", "post_plant", "trade_window", "clutch", "exec_like",
    "fight", "setup", "rotate", "contact", "mid_round", "late_opening", "default",
)
_WEAPON_FAMILIES = ("sniper", "rifle", "smg", "shotgun", "heavy", "pistol")

# 22 scalar dims + 12 weapon-profile dims + 12 situation dims + 2 site dims + 6 weapon dims
VECTOR_DIM = 54


def feature_blob_to_vector(feature: dict) -> list[float]:
    """Return a VECTOR_DIM-dimensional float list suitable for pgvector storage."""
    v = feature.get("vector") or {}

    dims: list[float] = [
        # Alive counts (3)
        v.get("alive_ct", 0) / 5.0,
        v.get("alive_t", 0) / 5.0,
        (v.get("alive_diff", 0) + 5) / 10.0,

        # HP (2)
        v.get("hp_ct_sum", 0) / 500.0,
        v.get("hp_t_sum", 0) / 500.0,

        # Defuser (1)
        v.get("defuser_ct_count", 0) / 5.0,

        # Centroids — map coords are roughly in [-4000, 4000] (4)
        v.get("ct_centroid_x", 0) / 4000.0,
        v.get("ct_centroid_y", 0) / 4000.0,
        v.get("t_centroid_x", 0) / 4000.0,
        v.get("t_centroid_y", 0) / 4000.0,

        # Spread (2)
        min(v.get("ct_spread", 0), 2000) / 2000.0,
        min(v.get("t_spread", 0), 2000) / 2000.0,

        # Combat (3)
        min(v.get("shots_total", 0), 50) / 50.0,
        min(v.get("deaths_ct", 0), 5) / 5.0,
        min(v.get("deaths_t", 0), 5) / 5.0,

        # Utility (1)
        min(v.get("utility_total", 0), 20) / 20.0,

        # Path distances (2)
        min(v.get("ct_path_distance", 0), 5000) / 5000.0,
        min(v.get("t_path_distance", 0), 5000) / 5000.0,

        # Time (3)
        min(max(0.0, v.get("time_since_freeze_end_s", 0) or 0.0), 115) / 115.0,
        max(0.0, v.get("time_since_bomb_plant_s", -1) or -1) / 40.0,
        min(v.get("seconds_remaining_s", 0) or 0.0, 115) / 115.0,

        # Planted flag (1)
        float(v.get("planted", 0)),
    ]

    # Weapon profiles — 2 sides × 6 families = 12
    ct_wp = feature.get("ct_weapon_profile") or {}
    t_wp = feature.get("t_weapon_profile") or {}
    for fam in _WEAPON_FAMILIES:
        dims.append(min(ct_wp.get(fam, 0), 5) / 5.0)
    for fam in _WEAPON_FAMILIES:
        dims.append(min(t_wp.get(fam, 0), 5) / 5.0)

    # Primary situation one-hot (12)
    primary = feature.get("primary_situation") or ""
    for s in _SITUATIONS:
        dims.append(1.0 if primary == s else 0.0)

    # Site one-hot (2)
    site = feature.get("site") or ""
    dims.append(1.0 if site == "a" else 0.0)
    dims.append(1.0 if site == "b" else 0.0)

    # Focus weapon family one-hot (6)
    focus = feature.get("focus_weapon_family") or ""
    for fam in _WEAPON_FAMILIES:
        dims.append(1.0 if focus == fam else 0.0)

    assert len(dims) == VECTOR_DIM, f"expected {VECTOR_DIM} dims, got {len(dims)}"
    return dims
