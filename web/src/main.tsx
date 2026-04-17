import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { Viewer } from "./Viewer";
import { LoginPage } from "./LoginPage";
import "./styles.css";

function parseSteamId(claimedId: string): string | null {
  const m = claimedId.match(/https?:\/\/steamcommunity\.com\/openid\/id\/(\d+)/);
  return m ? m[1] : null;
}

/** Resolve initial Steam ID from URL return params or sessionStorage. */
function getInitialSteamId(): string | null {
  const params = new URLSearchParams(window.location.search);
  if (params.get("openid.mode") === "id_res") {
    const steamId = parseSteamId(params.get("openid.claimed_id") ?? "");
    if (steamId) {
      // NOTE: server-side openid.check_authentication verification is deferred to M6.
      sessionStorage.setItem("steam_id", steamId);
      window.history.replaceState({}, "", window.location.pathname);
      return steamId;
    }
  }
  return sessionStorage.getItem("steam_id");
}

function App() {
  const [steamId, setSteamId] = useState<string | null>(getInitialSteamId);

  const signOut = () => {
    sessionStorage.removeItem("steam_id");
    setSteamId(null);
  };

  if (!steamId) {
    return <LoginPage onSignedIn={setSteamId} />;
  }
  return <Viewer steamId={steamId} onSignOut={signOut} />;
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
