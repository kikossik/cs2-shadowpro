import type { Density, ImportState, Layout, MatchesTweakState } from "./types";

type TweaksPanelProps = {
  tweaks: MatchesTweakState;
  setTweaks: (t: MatchesTweakState) => void;
  visible: boolean;
  setVisible: (v: boolean) => void;
};

const LAYOUTS: Layout[] = ["ledger", "cards", "timeline"];
const IMPORT_STATES: ImportState[] = ["idle", "loading", "done"];
const DENSITIES: { k: Density; l: string }[] = [
  { k: "comfortable", l: "COMFORT" },
  { k: "compact", l: "COMPACT" },
];

function importLabel(k: ImportState): string {
  return k === "idle" ? "OFF" : k === "loading" ? "LOAD" : "DONE";
}

export function TweaksPanel({ tweaks, setTweaks, visible, setVisible }: TweaksPanelProps) {
  if (!visible) {
    return (
      <button className="tweaks-fab" onClick={() => setVisible(true)}>
        ◧ TWEAKS
      </button>
    );
  }
  const update = <K extends keyof MatchesTweakState>(k: K, v: MatchesTweakState[K]) =>
    setTweaks({ ...tweaks, [k]: v });

  return (
    <div className="tweaks">
      <div className="tw-head">
        <span>TWEAKS</span>
        <button className="tw-close" onClick={() => setVisible(false)}>×</button>
      </div>
      <div className="tw-row">
        <div className="tw-label">LAYOUT</div>
        <div className="tw-ctrl">
          <div className="tw-seg">
            {LAYOUTS.map((k) => (
              <button
                key={k}
                className={tweaks.layout === k ? "on" : ""}
                onClick={() => update("layout", k)}
              >
                {k.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="tw-row">
        <div className="tw-label">IMPORT</div>
        <div className="tw-ctrl">
          <div className="tw-seg">
            {IMPORT_STATES.map((k) => (
              <button
                key={k}
                className={tweaks.importState === k ? "on" : ""}
                onClick={() => update("importState", k)}
              >
                {importLabel(k)}
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="tw-row">
        <div className="tw-label">DENSITY</div>
        <div className="tw-ctrl">
          <div className="tw-seg">
            {DENSITIES.map((o) => (
              <button
                key={o.k}
                className={tweaks.density === o.k ? "on" : ""}
                onClick={() => update("density", o.k)}
              >
                {o.l}
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="tw-row">
        <div className="tw-label">ROUNDS</div>
        <div className="tw-ctrl">
          <button
            className={`tw-toggle ${tweaks.showRoundStrip ? "on" : ""}`}
            onClick={() => update("showRoundStrip", !tweaks.showRoundStrip)}
          >
            <span />
          </button>
        </div>
      </div>
      <div className="tw-row">
        <div className="tw-label">LOADING</div>
        <div className="tw-ctrl">
          <button
            className={`tw-toggle ${tweaks.skeleton ? "on" : ""}`}
            onClick={() => update("skeleton", !tweaks.skeleton)}
          >
            <span />
          </button>
        </div>
      </div>
    </div>
  );
}
