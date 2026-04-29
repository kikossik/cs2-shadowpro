import { useState } from "react";

type Phase = "form" | "syncing" | "error";

type SetupPageProps = {
  steamId: string;
  onComplete: () => void;
  onSignOut: () => void;
};

export function SetupPage({ steamId, onComplete, onSignOut }: SetupPageProps) {
  const [authCode, setAuthCode] = useState("");
  const [shareCode, setShareCode] = useState("");
  const [phase, setPhase] = useState<Phase>("form");
  const [error, setError] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setPhase("syncing");
    setError("");

    try {
      const form = new FormData();
      form.append("steam_id", steamId);
      form.append("match_auth_code", authCode.trim());
      form.append("last_share_code", shareCode.trim());

      const r = await fetch("/api/setup", { method: "POST", body: form });
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: "Setup failed" }));
        throw new Error(err.detail ?? "Setup failed");
      }
      // Setup runs sync in the background; navigate to the matches page
      // immediately. Imports will appear there as the worker processes them.
      onComplete();
    } catch (err) {
      setPhase("error");
      setError(String(err).replace(/^Error:\s*/, ""));
    }
  };

  if (phase === "syncing") {
    return (
      <div className="login-root">
        <div className="login-card">
          <div className="login-brand">
            <span className="login-mark">SP</span>
            <span>ShadowPro</span>
          </div>
          <div style={{ display: "grid", gap: 14, justifyItems: "center" }}>
            <div className="setup-spinner" />
            <p className="login-tagline">
              Importing your match history…
              <br />
              This may take a few minutes.
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="login-root">
      <div className="login-card setup-card">
        <div className="login-brand">
          <span className="login-mark">SP</span>
          <span>ShadowPro</span>
        </div>

        <div>
          <p className="setup-step-tag">CONNECT MATCH HISTORY</p>
          <p className="setup-heading">One-time setup</p>
          <p className="login-tagline" style={{ textAlign: "left", margin: 0 }}>
            Go to{" "}
            <a
              href="https://help.steampowered.com/en/wizard/HelpWithGameIssue/?appid=730&issueid=128"
              target="_blank"
              rel="noreferrer"
              className="setup-link"
            >
              Steam CS2 match history page
            </a>{" "}
            and copy the two values below.
          </p>
        </div>

        <form onSubmit={handleSubmit} style={{ display: "grid", gap: 14 }}>
          <div className="setup-field">
            <label className="setup-label">MATCH HISTORY AUTHENTICATION CODE</label>
            <input
              className="setup-input"
              type="text"
              placeholder="XXXX-XXXXX-XXXX"
              value={authCode}
              onChange={(e) => setAuthCode(e.target.value)}
              required
              spellCheck={false}
              autoComplete="off"
            />
          </div>
          <div className="setup-field">
            <label className="setup-label">MOST RECENT SHARE CODE</label>
            <input
              className="setup-input"
              type="text"
              placeholder="CSGO-XXXXX-XXXXX-XXXXX-XXXXX-XXXXX"
              value={shareCode}
              onChange={(e) => setShareCode(e.target.value)}
              required
              spellCheck={false}
              autoComplete="off"
            />
          </div>
          {phase === "error" && <p className="setup-error">{error}</p>}
          <button className="login-btn" type="submit">
            IMPORT MY MATCHES
          </button>
        </form>

        <button className="setup-skip" onClick={onSignOut}>
          sign out
        </button>
      </div>
    </div>
  );
}
