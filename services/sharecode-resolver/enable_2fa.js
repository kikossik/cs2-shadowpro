#!/usr/bin/env node
/**
 * One-time script: enables Steam Mobile Authenticator on the bot account
 * and prints shared_secret + identity_secret to save in .env.
 *
 * Uses steam-session with MobileApp platform type, which gives us the
 * mobile access token that steamcommunity.enableTwoFactor() now requires.
 * Phone number is NOT required — activation code is sent to the account email.
 *
 * Run ONCE: npm run enable-2fa
 * After running, copy STEAM_SHARED_SECRET + STEAM_IDENTITY_SECRET into .env.
 */

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", "..", ".env") });

const { LoginSession, EAuthTokenPlatformType, EAuthSessionGuardType } = require("steam-session");
const SteamCommunity = require("steamcommunity");
const readline = require("readline");

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise((res) => rl.question(q, res));

async function main() {
  const { STEAM_USERNAME, STEAM_PASSWORD } = process.env;
  if (!STEAM_USERNAME || !STEAM_PASSWORD) {
    console.error("Set STEAM_USERNAME and STEAM_PASSWORD in .env first.");
    process.exit(1);
  }

  // Log in as MobileApp — this gives us a mobile access token
  const session = new LoginSession(EAuthTokenPlatformType.MobileApp);

  console.log(`\nLogging in as ${STEAM_USERNAME} (mobile app session)...`);

  const startResult = await session.startWithCredentials({
    accountName: STEAM_USERNAME,
    password: STEAM_PASSWORD,
  });

  if (startResult.actionRequired) {
    const hasEmail = startResult.validActions?.some(
      (a) => a.type === EAuthSessionGuardType.EmailCode
    );
    const hasEmailConf = startResult.validActions?.some(
      (a) => a.type === EAuthSessionGuardType.EmailConfirmation
    );

    if (hasEmail) {
      const code = await ask("Steam Guard code sent to account email: ");
      await session.submitSteamGuardCode(code.trim());
    } else if (hasEmailConf) {
      console.log("Check your email and click the confirmation link, then press Enter.");
      await ask("Press Enter once confirmed: ");
    } else {
      console.log("Guard actions available:", startResult.validActions);
      const code = await ask("Enter Steam Guard code: ");
      await session.submitSteamGuardCode(code.trim());
    }
  }

  await new Promise((resolve, reject) => {
    session.on("authenticated", resolve);
    session.on("error", reject);
    session.on("timeout", () => reject(new Error("Login timed out")));
  });

  console.log("Authenticated. Getting web cookies...");
  const cookies = await session.getWebCookies();
  const accessToken = session.accessToken;

  const community = new SteamCommunity();
  community.setCookies(cookies);
  community.setMobileAppAccessToken(accessToken);

  console.log("Enabling 2FA via steamcommunity...");
  console.log("Steam will send an activation code to the account email.\n");

  const result = await new Promise((resolve, reject) => {
    community.enableTwoFactor((err, response) => {
      if (err) return reject(err);
      resolve(response);
    });
  });

  if (!result.shared_secret) {
    throw new Error(`enableTwoFactor failed. Response: ${JSON.stringify(result)}`);
  }

  console.log("\n=== SAVE THESE TO YOUR .env NOW (before finalizing!) ===");
  console.log(`STEAM_SHARED_SECRET=${result.shared_secret}`);
  console.log(`STEAM_IDENTITY_SECRET=${result.identity_secret}`);
  console.log("=======================================================");
  console.log(`\nRevocation code (store safely — needed to remove 2FA later):`);
  console.log(`  ${result.revocation_code}\n`);

  const emailCode = await ask("Enter the activation code Steam sent to the account email: ");

  await new Promise((resolve, reject) => {
    community.finalizeTwoFactor(result.shared_secret, emailCode.trim(), (err) => {
      if (err) return reject(err);
      resolve();
    });
  });

  console.log("\n2FA enabled successfully!");
  console.log("Add STEAM_SHARED_SECRET and STEAM_IDENTITY_SECRET to .env now.");

  rl.close();
}

main().catch((err) => {
  console.error("\nError:", err.message);
  rl.close();
  process.exit(1);
});
