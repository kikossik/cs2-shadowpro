import { useEffect, useMemo, useState } from "react";
import type { ImportState, Match, MatchResult, MatchesTweakState } from "./types";
import { MOCK_MATCHES } from "./mockMatches";
import { FilterBar, ImportBanner, TopBar } from "./Shell";
import { LedgerLayout } from "./LedgerLayout";
import { CardsLayout } from "./CardsLayout";
import { TimelineLayout } from "./TimelineLayout";
import { TweaksPanel } from "./TweaksPanel";
import "./matches.css";

const TWEAK_DEFAULTS: MatchesTweakState = {
  layout: "ledger",
  importState: "done",
  density: "comfortable",
  showRoundStrip: true,
  skeleton: false,
};

const STORAGE_KEY = "sp_matches_tweaks";

function loadTweaks(): MatchesTweakState {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) return { ...TWEAK_DEFAULTS, ...JSON.parse(saved) };
  } catch {
    /* ignore */
  }
  return TWEAK_DEFAULTS;
}

type MatchesPageProps = {
  steamId: string;
  onOpenMatch: (matchId: string) => void;
  onSignOut: () => void;
};

export function MatchesPage({ steamId, onOpenMatch, onSignOut }: MatchesPageProps) {
  const [tweaks, setTweaks] = useState<MatchesTweakState>(loadTweaks);
  const [tweaksVisible, setTweaksVisible] = useState(false);
  const [mapFilter, setMapFilter] = useState("All maps");
  const [resultFilter, setResultFilter] = useState<"all" | MatchResult>("all");
  const [importState, setImportState] = useState<ImportState>(tweaks.importState);
  const [importProgress, setImportProgress] = useState(0);

  // Persist tweaks.
  useEffect(() => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(tweaks)); } catch { /* ignore */ }
  }, [tweaks]);

  // Keep local import state in sync with the tweak panel's preview control.
  useEffect(() => { setImportState(tweaks.importState); }, [tweaks.importState]);

  // Allow the matches page to scroll even though styles.css pins body to overflow:hidden
  // for the fixed-viewport Viewer layout.
  useEffect(() => {
    const prevBodyOverflow = document.body.style.overflow;
    const prevBodyHeight = document.body.style.height;
    const prevHtmlHeight = document.documentElement.style.height;
    document.body.style.overflow = "auto";
    document.body.style.height = "auto";
    document.documentElement.style.height = "auto";
    return () => {
      document.body.style.overflow = prevBodyOverflow;
      document.body.style.height = prevBodyHeight;
      document.documentElement.style.height = prevHtmlHeight;
    };
  }, []);

  // Simulate the import progress bar when the import state is "loading".
  useEffect(() => {
    if (importState !== "loading") return;
    setImportProgress(0);
    let p = 0;
    const id = setInterval(() => {
      p += 0.035 + Math.random() * 0.03;
      if (p >= 1) {
        setImportProgress(1);
        clearInterval(id);
        setTimeout(() => {
          setImportState("done");
          setTweaks((t) => ({ ...t, importState: "done" }));
        }, 500);
      } else {
        setImportProgress(p);
      }
    }, 220);
    return () => clearInterval(id);
  }, [importState]);

  const filtered = useMemo(() => {
    let arr: Match[] = MOCK_MATCHES;
    if (mapFilter !== "All maps") {
      arr = arr.filter((m) => m.map.display === mapFilter);
    }
    if (resultFilter !== "all") {
      arr = arr.filter((m) => m.result === resultFilter);
    }
    return arr;
  }, [mapFilter, resultFilter]);

  const onOpen = (m: Match) => onOpenMatch(m.id);

  const showSkeleton = tweaks.skeleton || importState === "loading";

  return (
    <div className="matches-root">
      <TopBar steamId={steamId} onSignOut={onSignOut} />
      <div>
        <ImportBanner
          state={importState}
          progress={importProgress}
          onDismiss={() => {
            setImportState("idle");
            setTweaks({ ...tweaks, importState: "idle" });
          }}
        />
        <FilterBar
          mapFilter={mapFilter}
          setMapFilter={setMapFilter}
          resultFilter={resultFilter}
          setResultFilter={setResultFilter}
          count={filtered.length}
        />
        <div className="matches-wrap">
          {showSkeleton ? (
            <SkeletonList />
          ) : tweaks.layout === "ledger" ? (
            <LedgerLayout
              matches={filtered}
              compact={tweaks.density === "compact"}
              showRoundStrip={tweaks.showRoundStrip}
              onOpen={onOpen}
            />
          ) : tweaks.layout === "cards" ? (
            <CardsLayout
              matches={filtered}
              showRoundStrip={tweaks.showRoundStrip}
              onOpen={onOpen}
            />
          ) : (
            <TimelineLayout
              matches={filtered}
              showRoundStrip={tweaks.showRoundStrip}
              onOpen={onOpen}
            />
          )}
        </div>
      </div>

      <TweaksPanel
        tweaks={tweaks}
        setTweaks={setTweaks}
        visible={tweaksVisible}
        setVisible={setTweaksVisible}
      />
    </div>
  );
}

function SkeletonList() {
  return (
    <div className="ledger">
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} className="skel-row">
          <div className="skel-thumb" />
          <div className="skel-block" style={{ width: 140 }} />
          <div className="skel-block" style={{ width: 70 }} />
          <div className="skel-block" style={{ width: 80 }} />
          <div className="skel-block" style={{ width: 40 }} />
          <div className="skel-block" style={{ width: 40 }} />
          <div className="skel-block" style={{ width: 110 }} />
          <div className="skel-block" style={{ width: 100, marginLeft: "auto" }} />
        </div>
      ))}
    </div>
  );
}
