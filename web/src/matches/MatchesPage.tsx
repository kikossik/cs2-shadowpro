import { useEffect, useMemo, useRef, useState } from "react";
import type { Match, MatchResult, MatchesTweakState, SteamProfile } from "./types";
import { FilterBar, ImportBanner, TopBar } from "./Shell";
import { LedgerLayout } from "./LedgerLayout";
import { CardsLayout } from "./CardsLayout";
import { TimelineLayout } from "./TimelineLayout";
import { TweaksPanel } from "./TweaksPanel";
import "./matches.css";

const TWEAK_DEFAULTS: MatchesTweakState = {
  layout: "ledger",
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

type ImportState = "idle" | "processing" | "done" | "error";

type ReplayExtra = { roundCount?: number | null; mapDisplay?: string | null };

type MatchesPageProps = {
  steamId: string;
  onOpenReplay: (demoId: string, roundNum: number, extra?: ReplayExtra) => void;
  onSignOut: () => void;
};

export function MatchesPage({ steamId, onOpenReplay, onSignOut }: MatchesPageProps) {
  const [tweaks, setTweaks] = useState<MatchesTweakState>(loadTweaks);
  const [tweaksVisible, setTweaksVisible] = useState(false);
  const [mapFilter, setMapFilter] = useState("All maps");
  const [resultFilter, setResultFilter] = useState<"all" | MatchResult>("all");

  const [matches, setMatches] = useState<Match[]>([]);
  const [profile, setProfile] = useState<SteamProfile | null>(null);
  const [loading, setLoading] = useState(true);

  const [importState, setImportState] = useState<ImportState>("idle");
  const [importProgress, setImportProgress] = useState(0);
  const [importSituations, setImportSituations] = useState<number | undefined>();
  const [importError, setImportError] = useState<string | undefined>();
  const [importJobId, setImportJobId] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

  // Persist tweaks.
  useEffect(() => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(tweaks)); } catch { /* ignore */ }
  }, [tweaks]);

  // Allow this page to scroll (the Viewer uses fixed viewport).
  useEffect(() => {
    const prev = { o: document.body.style.overflow, h: document.body.style.height, H: document.documentElement.style.height };
    document.body.style.overflow = "auto";
    document.body.style.height = "auto";
    document.documentElement.style.height = "auto";
    return () => {
      document.body.style.overflow = prev.o;
      document.body.style.height = prev.h;
      document.documentElement.style.height = prev.H;
    };
  }, []);

  // Load profile + matches on mount.
  useEffect(() => {
    Promise.all([
      fetch(`/api/profile/${steamId}`).then((r) => r.ok ? r.json() : null),
      fetch(`/api/matches/${steamId}`).then((r) => r.ok ? r.json() : []),
    ]).then(([prof, mlist]) => {
      setProfile(prof);
      setMatches(mlist ?? []);
    }).catch(() => {
      setMatches([]);
    }).finally(() => setLoading(false));
  }, [steamId]);

  // Poll import job status.
  useEffect(() => {
    if (!importJobId || importState !== "processing") return;
    // Simulated progress increments while waiting.
    const tick = setInterval(() => {
      setImportProgress((p) => Math.min(p + 0.015, 0.9));
    }, 400);

    const poll = setInterval(async () => {
      try {
        const r = await fetch(`/api/import/${importJobId}`);
        if (!r.ok) return;
        const data = await r.json();
        if (data.status === "done") {
          clearInterval(tick);
          clearInterval(poll);
          setImportProgress(1);
          setImportSituations(data.situations);
          setImportState("done");
          // Reload matches list.
          const updated = await fetch(`/api/matches/${steamId}`).then((r) => r.ok ? r.json() : []);
          setMatches(updated ?? []);
        } else if (data.status === "error") {
          clearInterval(tick);
          clearInterval(poll);
          setImportState("error");
          setImportError(data.error ?? "Processing failed");
        }
      } catch { /* ignore transient errors */ }
    }, 2000);

    return () => { clearInterval(tick); clearInterval(poll); };
  }, [importJobId, importState, steamId]);

  const handleFileSelect = async (file: File) => {
    if (!file.name.endsWith(".dem")) {
      setImportState("error");
      setImportError("Only .dem files are accepted.");
      return;
    }
    setImportState("processing");
    setImportProgress(0);
    setImportSituations(undefined);
    setImportError(undefined);
    setImportJobId(null);

    const form = new FormData();
    form.append("steam_id", steamId);
    form.append("file", file);

    try {
      const r = await fetch("/api/import", { method: "POST", body: form });
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: "Upload failed" }));
        setImportState("error");
        setImportError(err.detail ?? "Upload failed");
        return;
      }
      const { job_id } = await r.json();
      setImportJobId(job_id);
    } catch (e) {
      setImportState("error");
      setImportError(String(e));
    }
  };

  const openFileDialog = () => fileInputRef.current?.click();

  const filtered = useMemo(() => {
    let arr = matches;
    if (mapFilter !== "All maps") {
      arr = arr.filter((m) => m.map.display === mapFilter);
    }
    if (resultFilter !== "all") {
      arr = arr.filter((m) => m.result === resultFilter);
    }
    return arr;
  }, [matches, mapFilter, resultFilter]);

  const onOpen = (m: Match) =>
    onOpenReplay(m.id, 1, { roundCount: m.round_count, mapDisplay: m.map.display });
  const showSkeleton = tweaks.skeleton || loading;

  return (
    <div className="matches-root">
      <input
        ref={fileInputRef}
        type="file"
        accept=".dem"
        style={{ display: "none" }}
        onChange={(e) => { const f = e.target.files?.[0]; if (f) handleFileSelect(f); e.target.value = ""; }}
      />
      <TopBar steamId={steamId} profile={profile} onSignOut={onSignOut} onImport={openFileDialog} />
      <div>
        <ImportBanner
          state={importState}
          progress={importProgress}
          situationsFound={importSituations}
          errorMessage={importError}
          onDismiss={() => { setImportState("idle"); setImportJobId(null); }}
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
          ) : matches.length === 0 ? (
            <EmptyState onImport={openFileDialog} />
          ) : tweaks.layout === "ledger" ? (
            <LedgerLayout
              matches={filtered}
              compact={tweaks.density === "compact"}
              showRoundStrip={tweaks.showRoundStrip}
              onOpen={onOpen}
              onOpenReplay={(demoId) => onOpenReplay(demoId, 1)}
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

function EmptyState({ onImport }: { onImport: () => void }) {
  return (
    <div className="empty-state" style={{ paddingTop: 64 }}>
      <pre className="empty-ascii">{`
  ┌─────────────┐
  │  NO MATCHES │
  │    YET      │
  └─────────────┘`}</pre>
      <div className="empty-title">No demos imported yet</div>
      <div className="empty-sub" style={{ marginBottom: 20 }}>
        Download a demo from CS2 (Watch → Your Matches → Download),<br />
        then import the .dem file below.
      </div>
      <button className="import-fab" onClick={onImport} style={{ fontSize: 13, padding: "8px 18px" }}>
        + IMPORT DEMO
      </button>
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
          <div className="skel-block" style={{ width: 110 }} />
          <div className="skel-block" style={{ width: 100, marginLeft: "auto" }} />
        </div>
      ))}
    </div>
  );
}
