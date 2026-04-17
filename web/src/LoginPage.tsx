function buildSteamAuthUrl(): string {
  const returnTo = window.location.origin + window.location.pathname;
  const params = new URLSearchParams({
    "openid.ns": "http://specs.openid.net/auth/2.0",
    "openid.mode": "checkid_setup",
    "openid.return_to": returnTo,
    "openid.realm": window.location.origin + "/",
    "openid.identity": "http://specs.openid.net/auth/2.0/identifier_select",
    "openid.claimed_id": "http://specs.openid.net/auth/2.0/identifier_select",
  });
  return `https://steamcommunity.com/openid/login?${params}`;
}

// SVG from Bootstrap Icons — steam
function SteamIcon() {
  return (
    <svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M.329 10.333A8.01 8.01 0 0 0 7.99 16C12.414 16 16 12.418 16 8s-3.586-8-8.009-8A8.006 8.006 0 0 0 0 7.468l.003.006 4.304 1.769A2.198 2.198 0 0 1 5.62 8.88l1.96-2.844-.001-.04a3.046 3.046 0 0 1 3.042-3.043 3.046 3.046 0 0 1 3.042 3.043 3.047 3.047 0 0 1-3.111 3.044l-2.804 2a2.223 2.223 0 0 1-3.075 2.11 2.217 2.217 0 0 1-1.312-1.568L.33 10.333Z"/>
      <path d="M4.868 12.683a1.715 1.715 0 0 0 1.342-3.166 1.71 1.71 0 0 0-1.342 3.166Zm7.807-8.909a2.028 2.028 0 1 0-4.056 0 2.028 2.028 0 0 0 4.056 0Z"/>
    </svg>
  );
}

type LoginPageProps = {
  onSignedIn: (steamId: string) => void;
};

export function LoginPage({ onSignedIn: _ }: LoginPageProps) {
  return (
    <div className="login-root">
      <div className="login-card">
        <div className="login-brand">
          <span className="login-mark">SP</span>
          <span>ShadowPro</span>
        </div>

        <p className="login-tagline">
          See what the pros did in your exact situation.
          <br />
          Connect your Steam account to get started.
        </p>

        <a className="login-btn" href={buildSteamAuthUrl()}>
          <SteamIcon />
          SIGN IN WITH STEAM
        </a>

        <p className="login-fine">
          Steam sign-in is used only to identify you.
          <br />
          No personal data is stored without your consent.
        </p>
      </div>
    </div>
  );
}
