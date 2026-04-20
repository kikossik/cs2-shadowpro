// ── Colours ────────────────────────────────────────────────────────────────────
export const CT_COLOR   = "#46a0ff";
export const T_COLOR    = "#ff5a46";
export const DEAD_COLOR = "#50505c";
export const WHITE      = "#ffffff";
export const DIM_COLOR  = "#64647a";

// ── Utility sizes (world units) ────────────────────────────────────────────────
export const SMOKE_RADIUS_WU   = 144;
export const INFERNO_RADIUS_WU = 100;
export const FLASH_VIS_TICKS   = 48;
export const TRAIL_FADE_TICKS  = 64;
export const TICKRATE          = 64;

// ── Grenade type → trail colour ────────────────────────────────────────────────
export const GREN_COLORS: Record<string, string> = {
  CSmokeGrenadeProjectile:      "rgba(175,182,210,",
  CMolotovProjectile:           "rgba(255,120,30,",
  CIncendiaryGrenadeProjectile: "rgba(255,120,30,",
  CFlashbangProjectile:         "rgba(255,240,110,",
  CHEGrenadeProjectile:         "rgba(80,210,90,",
};

// ── Weapon class → display name ────────────────────────────────────────────────
export const WPN_CLASS: Record<string, string> = {
  weapon_ak47: "AK-47", weapon_m4a1: "M4A4", weapon_m4a1_silencer: "M4A1-S",
  weapon_awp: "AWP", weapon_sg556: "SG 553", weapon_aug: "AUG",
  weapon_ssg08: "SSG 08", weapon_scar20: "SCAR-20", weapon_g3sg1: "G3SG1",
  weapon_galil: "Galil AR", weapon_galilar: "Galil AR", weapon_famas: "FAMAS",
  weapon_mp9: "MP9", weapon_mac10: "MAC-10", weapon_mp7: "MP7",
  weapon_mp5sd: "MP5-SD", weapon_ump45: "UMP-45", weapon_p90: "P90",
  weapon_bizon: "PP-Bizon", weapon_nova: "Nova", weapon_xm1014: "XM1014",
  weapon_mag7: "MAG-7", weapon_sawedoff: "Sawed-Off",
  weapon_negev: "Negev", weapon_m249: "M249",
  weapon_deagle: "Desert Eagle", weapon_glock: "Glock-18",
  weapon_usp_silencer: "USP-S", weapon_p250: "P250",
  weapon_tec9: "Tec-9", weapon_cz75a: "CZ75-Auto",
  weapon_fiveseven: "Five-SeveN", weapon_elite: "Dual Berettas",
  weapon_revolver: "R8 Revolver", weapon_p2000: "P2000",
  weapon_flashbang: "Flashbang", weapon_hegrenade: "HE Grenade",
  weapon_smokegrenade: "Smoke Grenade", weapon_molotov: "Molotov",
  weapon_incgrenade: "Incendiary Grenade", weapon_decoy: "Decoy Grenade",
  weapon_c4: "C4", weapon_knife: "Knife", weapon_taser: "Zeus x27",
};

const RIFLES   = new Set(["AWP","AK-47","M4A4","M4A1-S","AUG","SG 553","SSG 08","SCAR-20","G3SG1","Galil AR","FAMAS"]);
const SMGS     = new Set(["MP9","MAC-10","MP7","MP5-SD","UMP-45","P90","PP-Bizon"]);
const SHOTGUNS = new Set(["Nova","XM1014","MAG-7","Sawed-Off"]);
const HEAVY    = new Set(["Negev","M249"]);
const UTILITY  = new Set(["C4","Flashbang","HE Grenade","Smoke Grenade","Molotov","Incendiary Grenade","Decoy Grenade"]);

export function decodeWeapon(raw: string): string {
  let wpn = WPN_CLASS[raw];
  if (!wpn) {
    const base = raw.split("_").slice(0, 2).join("_");
    wpn = WPN_CLASS[base] ?? raw.replace("weapon_", "").replace(/_/g, " ");
  }
  return wpn;
}

export function bestWeapon(inv: string[]): string {
  for (const pool of [RIFLES, SMGS, SHOTGUNS, HEAVY]) {
    for (const w of inv) if (pool.has(w)) return w;
  }
  for (const w of inv) if (!UTILITY.has(w)) return w;
  return "";
}
