import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { LoginPage } from "./LoginPage";
import { SetupPage } from "./SetupPage";
import { MatchesPage } from "./matches/MatchesPage";
import { Viewer } from "./Viewer";
import "./styles.css";

type AppPhase = "login" | "checking" | "setup" | "app";

function parseSteamId(claimedId: string): string | null {
  const m = claimedId.match(/https?:\/\/steamcommunity\.com\/openid\/id\/(\d+)/);
  return m ? m[1] : null;
}

function getInitialSteamId(): string | null {
  const params = new URLSearchParams(window.location.search);
  if (params.get("openid.mode") === "id_res") {
    const steamId = parseSteamId(params.get("openid.claimed_id") ?? "");
    if (steamId) {
      // NOTE: server-side openid.check_authentication verification is deferred to M6.
      localStorage.setItem("steam_id", steamId);
      window.history.replaceState({}, "", window.location.pathname);
      return steamId;
    }
  }
  return localStorage.getItem("steam_id");
}

type ReplayTarget = {
  demoId: string;
  roundNum: number;
  roundCount?: number | null;
  mapDisplay?: string | null;
};

function App() {
  const [steamId, setSteamId] = useState<string | null>(getInitialSteamId);
  const [phase, setPhase] = useState<AppPhase>(() => getInitialSteamId() ? "checking" : "login");
  const [replayTarget, setReplayTarget] = useState<ReplayTarget | null>(null);

  const signOut = () => {
    localStorage.removeItem("steam_id");
    setReplayTarget(null);
    setSteamId(null);
    setPhase("login");
  };

  // After login (or on page reload with existing session), check if setup is complete.
  useEffect(() => {
    if (!steamId) { setPhase("login"); return; }
    setPhase("checking");
    fetch(`/api/user/${steamId}`)
      .then((r) => setPhase(r.ok ? "app" : "setup"))
      .catch(() => setPhase("setup"));
  }, [steamId]);

  if (!steamId || phase === "login") {
    return <LoginPage onSignedIn={setSteamId} />;
  }
  if (phase === "checking") {
    return (
      <div className="login-root">
        <div style={{ fontFamily: "var(--fontMono)", fontSize: 11, color: "var(--dim)", letterSpacing: "0.08em" }}>
          LOADING…
        </div>
      </div>
    );
  }
  if (phase === "setup") {
    return <SetupPage steamId={steamId} onComplete={() => setPhase("app")} onSignOut={signOut} />;
  }
  if (replayTarget) {
    return (
      <Viewer
        matchId={replayTarget.demoId}
        steamId={steamId}
        initialRound={replayTarget.roundNum}
        roundCount={replayTarget.roundCount ?? null}
        mapDisplay={replayTarget.mapDisplay ?? null}
        onBack={() => setReplayTarget(null)}
        onSignOut={signOut}
      />
    );
  }
  return (
    <MatchesPage
      steamId={steamId}
      onOpenReplay={(demoId, roundNum, extra) => setReplayTarget({ demoId, roundNum, ...extra })}
      onSignOut={signOut}
    />
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
