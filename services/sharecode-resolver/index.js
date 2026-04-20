#!/usr/bin/env node
/**
 * CS2 ShadowPro — Share Code Resolver Microservice
 *
 * Connects to the CS2 Game Coordinator as the bot account and resolves
 * CS2 share codes into .dem.bz2 download URLs.
 *
 * Endpoints:
 *   POST /resolve  { "shareCode": "CSGO-xxxxx-xxxxx-xxxxx-xxxxx-xxxxx" }
 *                → { "demoUrl": "http://replay105.valve.net/730/...dem.bz2" }
 *
 *   GET  /health  → { "gcReady": true, "queueLength": 0 }
 *
 * Start: npm start  (or: node index.js)
 * Env:   STEAM_USERNAME, STEAM_PASSWORD, STEAM_SHARED_SECRET, RESOLVER_PORT
 */

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", "..", ".env") });

const SteamUser = require("steam-user");
const GlobalOffensive = require("globaloffensive");
const SteamTotp = require("steam-totp");
const express = require("express");

const PORT = parseInt(process.env.RESOLVER_PORT || "3001", 10);

// ── State ────────────────────────────────────────────────────────────────────

const user = new SteamUser();
const csgo = new GlobalOffensive(user);
const app = express();
app.use(express.json());

let gcReady = false;

// Serial request queue — one GC request at a time to avoid matchList collisions.
/** @type {Array<{shareCode: string, resolve: Function, reject: Function}>} */
const queue = [];
let processing = false;

// ── Queue processor ───────────────────────────────────────────────────────────

function processNext() {
  if (processing || queue.length === 0 || !gcReady) return;

  processing = true;
  const { shareCode, resolve, reject } = queue.shift();

  const timeout = setTimeout(() => {
    csgo.removeListener("matchList", onMatchList);
    processing = false;
    reject(new Error("GC did not respond within 20s"));
    processNext();
  }, 20_000);

  function onMatchList(matches) {
    clearTimeout(timeout);
    csgo.removeListener("matchList", onMatchList);
    processing = false;

    try {
      if (!matches || matches.length === 0) throw new Error("Empty matchList response");
      const rounds = matches[0].roundstatsall;
      if (!rounds || rounds.length === 0) throw new Error("No round stats in match");
      const demoUrl = rounds[rounds.length - 1].map;
      if (!demoUrl) throw new Error("No demo URL in match data");
      resolve(demoUrl);
    } catch (err) {
      reject(err);
    }

    processNext();
  }

  csgo.on("matchList", onMatchList);
  csgo.requestGame(shareCode);
}

// ── Steam login ───────────────────────────────────────────────────────────────

function login() {
  const { STEAM_USERNAME, STEAM_PASSWORD, STEAM_SHARED_SECRET } = process.env;

  if (!STEAM_USERNAME || !STEAM_PASSWORD) {
    console.error("[steam] STEAM_USERNAME / STEAM_PASSWORD not set in .env");
    process.exit(1);
  }

  const opts = { accountName: STEAM_USERNAME, password: STEAM_PASSWORD };
  if (STEAM_SHARED_SECRET) {
    opts.twoFactorCode = SteamTotp.generateAuthCode(STEAM_SHARED_SECRET);
  }

  console.log(`[steam] Logging in as ${STEAM_USERNAME}...`);
  user.logOn(opts);
}

user.on("loggedOn", () => {
  console.log("[steam] Logged in");
  user.setPersona(SteamUser.EPersonaState.Online);
  console.log("[steam] Launching CS2 (appid 730) to trigger GC connection...");
  user.gamesPlayed([730]);
  setTimeout(() => {
    if (!gcReady) console.log("[gc] Still waiting for GC connection (can take up to 60s)...");
  }, 15_000);
});

user.on("steamGuard", (_domain, _callback) => {
  console.error(
    "[steam] Steam Guard 2FA required. Run `npm run enable-2fa` to set up " +
      "headless auth, then add STEAM_SHARED_SECRET to .env."
  );
  process.exit(1);
});

user.on("error", (err) => {
  console.error("[steam] Error:", err.message);
  gcReady = false;
  console.log("[steam] Reconnecting in 30s...");
  setTimeout(login, 30_000);
});

user.on("disconnected", (_eresult, msg) => {
  console.warn("[steam] Disconnected:", msg);
  gcReady = false;
  setTimeout(login, 10_000);
});

// ── GC events ─────────────────────────────────────────────────────────────────

csgo.on("connectedToGC", () => {
  console.log("[gc] Connected to CS2 Game Coordinator");
  gcReady = true;
  processNext();
});

csgo.on("disconnectedFromGC", (reason) => {
  console.warn("[gc] Disconnected, reason:", reason);
  gcReady = false;
});

csgo.on("error", (err) => {
  console.error("[gc] Error:", err.message);
});

csgo.on("connectionStatus", (status) => {
  console.log("[gc] Connection status:", status);
});

// ── HTTP endpoints ─────────────────────────────────────────────────────────────

app.post("/resolve", (req, res) => {
  const { shareCode } = req.body;

  if (!shareCode || typeof shareCode !== "string") {
    return res.status(400).json({ error: "shareCode (string) required" });
  }

  if (!gcReady) {
    return res.status(503).json({ error: "GC not ready, try again in a few seconds" });
  }

  new Promise((resolve, reject) => {
    queue.push({ shareCode, resolve, reject });
    processNext();
  })
    .then((demoUrl) => res.json({ demoUrl }))
    .catch((err) => res.status(500).json({ error: err.message }));
});

app.get("/health", (_req, res) => {
  res.json({ gcReady, queueLength: queue.length });
});

// ── Start ─────────────────────────────────────────────────────────────────────

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[resolver] Listening on http://0.0.0.0:${PORT}`);
  login();
});
