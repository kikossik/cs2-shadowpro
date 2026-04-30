"""Semantic Mirage zones for macro-scenario mapping.

Maps awpy `place` strings (last_place_name) to a coarse zone vocabulary,
plus a side bucket (a / mid / b / spawn) used for site-intent inference.
"""
from __future__ import annotations

# Canonical zones — keep small. Anything missing falls into "other".
MIRAGE_ZONES = {
    # A side
    "BombsiteA":       ("a_site",     "a"),
    "Tetris":          ("a_site",     "a"),
    "Ticketbooth":     ("a_site",     "a"),
    "TicketBooth":     ("a_site",     "a"),
    "Default":         ("a_site",     "a"),
    "PalaceInterior":  ("palace",     "a"),
    "PalaceAlley":     ("palace",     "a"),
    "TopofPalace":     ("palace",     "a"),
    "ARamp":           ("a_ramp",     "a"),
    "TRamp":           ("a_ramp",     "a"),
    "SnipersNest":     ("a_ramp",     "a"),
    # Mid
    "Middle":          ("mid",        "mid"),
    "TopofMid":        ("mid",        "mid"),
    "BottomofMid":     ("mid",        "mid"),
    "Catwalk":         ("catwalk",    "mid"),
    "Connector":       ("connector",  "mid"),
    "Jungle":          ("jungle",     "a"),
    "Window":          ("connector",  "mid"),
    "MidWindow":       ("connector",  "mid"),
    "Underpass":       ("underpass",  "b"),
    # B side
    "BombsiteB":       ("b_site",     "b"),
    "Apartments":      ("b_apps",     "b"),
    "Balcony":         ("b_apps",     "b"),
    "House":           ("b_apps",     "b"),
    "Stairs":          ("b_apps",     "b"),
    "Kitchen":         ("b_apps",     "b"),
    "Truck":           ("b_short",    "b"),
    "Shop":            ("b_short",    "b"),
    "BackAlley":       ("b_short",    "b"),
    "SideAlley":       ("b_short",    "b"),
    "Marketplace":     ("market",     "b"),
    "Market":          ("market",     "b"),
    # spawns / boring
    "TSpawn":          ("t_spawn",    "spawn"),
    "CTSpawn":         ("ct_spawn",   "spawn"),
    "Ladder":          ("ladder",     "mid"),
    "Scaffolding":     ("ladder",     "mid"),
}

ALL_ZONES = sorted({z for z, _ in MIRAGE_ZONES.values()} | {"other"})


def zone_for(place: str | None) -> tuple[str, str]:
    """Return (zone, side_bucket) for a raw awpy place name."""
    if not place:
        return ("other", "spawn")
    return MIRAGE_ZONES.get(place, ("other", "spawn"))
