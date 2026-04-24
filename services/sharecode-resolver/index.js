#!/usr/bin/env node
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", "..", ".env") });

const express = require("express");
const SteamUser = require("steam-user");
const GlobalOffensive = require("globaloffensive");
const SteamTotp = require("steam-totp");

const PORT = parseInt(process.env.RESOLVER_PORT || "3001", 10);
const REQUEST_TIMEOUT_MS = parseInt(process.env.RESOLVER_REQUEST_TIMEOUT_MS || "20000", 10);

const app = express();
const user = new SteamUser();
const csgo = new GlobalOffensive(user);

app.use(express.json());

let gcReady = false;
let processing = false;
const queue = [];

function processNext() {
  if (processing || queue.length === 0 || !gcReady) return;

  processing = true;
  const { shareCode, resolve, reject } = queue.shift();

  const timeout = setTimeout(() => {
    csgo.removeListener("matchList", onMatchList);
    processing = false;
    reject(new Error("GC did not respond in time"));
    processNext();
  }, REQUEST_TIMEOUT_MS);

  function onMatchList(matches) {
    clearTimeout(timeout);
    csgo.removeListener("matchList", onMatchList);
    processing = false;

    try {
      if (!matches || matches.length === 0) throw new Error("empty matchList response");
      const rounds = matches[0].roundstatsall || [];
      if (rounds.length === 0) throw new Error("match response has no round stats");
      const demoUrl = rounds[rounds.length - 1].map;
      if (!demoUrl) throw new Error("match response did not include a demo URL");
      resolve(demoUrl);
    } catch (err) {
      reject(err);
    }

    processNext();
  }

  csgo.on("matchList", onMatchList);
  csgo.requestGame(shareCode);
}

function login() {
  const { STEAM_USERNAME, STEAM_PASSWORD, STEAM_SHARED_SECRET } = process.env;
  if (!STEAM_USERNAME || !STEAM_PASSWORD) {
    console.warn("[steam] STEAM_USERNAME and STEAM_PASSWORD not set; resolver is running but GC is disabled");
    return;
  }

  const options = { accountName: STEAM_USERNAME, password: STEAM_PASSWORD };
  if (STEAM_SHARED_SECRET) {
    options.twoFactorCode = SteamTotp.generateAuthCode(STEAM_SHARED_SECRET);
  }

  console.log(`[steam] logging in as ${STEAM_USERNAME}`);
  user.logOn(options);
}

user.on("loggedOn", () => {
  console.log("[steam] logged on");
  user.setPersona(SteamUser.EPersonaState.Online);
  user.gamesPlayed([730]);
});

user.on("steamGuard", () => {
  console.error("[steam] Steam Guard code required. Set STEAM_SHARED_SECRET for headless auth.");
  process.exit(1);
});

user.on("error", (err) => {
  console.error("[steam] error:", err.message);
  gcReady = false;
  setTimeout(login, 30000);
});

user.on("disconnected", (_eresult, message) => {
  console.warn("[steam] disconnected:", message);
  gcReady = false;
  setTimeout(login, 10000);
});

csgo.on("connectedToGC", () => {
  console.log("[gc] connected");
  gcReady = true;
  processNext();
});

csgo.on("disconnectedFromGC", (reason) => {
  console.warn("[gc] disconnected:", reason);
  gcReady = false;
});

csgo.on("error", (err) => {
  console.error("[gc] error:", err.message);
});

app.post("/resolve", (req, res) => {
  const { shareCode } = req.body || {};
  if (!shareCode || typeof shareCode !== "string") {
    return res.status(400).json({ error: "shareCode string required" });
  }
  if (!gcReady) {
    return res.status(503).json({ error: "GC not ready" });
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

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[resolver] listening on ${PORT}`);
  login();
});
