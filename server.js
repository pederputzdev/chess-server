// server.js (Node 18+)
import http from "http";
import url from "url";
import { WebSocketServer } from "ws";
import { Chess } from "chess.js";

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;

// --- ENV ---
const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim();
const SUPABASE_ANON_KEY = (process.env.SUPABASE_ANON_KEY || "").trim();

// IMPORTANT: trim removes invisible newline/space bugs that cause auth mismatches
const GAME_SERVER_API_KEY = (process.env.GAME_SERVER_API_KEY || "").trim();

// Always derive endpoint from SUPABASE_URL unless explicitly overridden
const GAME_SERVER_ENDPOINT =
  (process.env.GAME_SERVER_ENDPOINT || "").trim() ||
  `${SUPABASE_URL}/functions/v1/game-server`;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_ANON_KEY env vars.");
}
if (!GAME_SERVER_API_KEY) {
  throw new Error("Missing GAME_SERVER_API_KEY env var (or it is whitespace).");
}

// --- Process-level error handlers (prevent silent crashes) ---
process.on("unhandledRejection", (reason, promise) => {
  console.error("[Server] Unhandled Promise Rejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
    stack: reason instanceof Error ? reason.stack : undefined,
    code: reason instanceof Error ? reason.code : undefined,
    timestamp: new Date().toISOString(),
  });
  // Don't crash - log and continue
  // In production, you might want to gracefully shutdown here
});

process.on("uncaughtException", (error) => {
  console.error("[Server] Uncaught Exception", {
    error: error.message,
    stack: error.stack,
    code: error.code,
    timestamp: new Date().toISOString(),
  });
  // Log but don't crash immediately - allow graceful shutdown
  // In production, you might want to exit(1) after logging
});

// --- Simple helpers ---
function safeSend(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function parseWager(x) {
  const n = Number(x);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (!Number.isInteger(n)) return null;
  return n;
}

// Accept either "e2e4" UCI string OR {from,to,promotion}
function parseMove(input) {
  if (!input) return null;

  if (typeof input === "string") {
    const s = input.trim().toLowerCase();
    if (!/^[a-h][1-8][a-h][1-8][qrbn]?$/.test(s)) return null;
    const from = s.slice(0, 2);
    const to = s.slice(2, 4);
    const promotion = s.length === 5 ? s[4] : undefined;
    return promotion ? { from, to, promotion } : { from, to };
  }

  if (typeof input === "object") {
    const { from, to, promotion } = input;
    if (typeof from !== "string" || typeof to !== "string") return null;
    const f = from.trim().toLowerCase();
    const t = to.trim().toLowerCase();
    if (!/^[a-h][1-8]$/.test(f) || !/^[a-h][1-8]$/.test(t)) return null;
    if (promotion && !/^[qrbn]$/.test(String(promotion).toLowerCase())) return null;
    return promotion
      ? { from: f, to: t, promotion: String(promotion).toLowerCase() }
      : { from: f, to: t };
  }

  return null;
}

// --- Auth: verify Supabase access token -> user id ---
async function getUserIdFromToken(token) {
  if (!token) return null;

  const AUTH_TIMEOUT_MS = 5000; // 5 second timeout
  const MAX_RETRIES = 2;
  const RETRY_DELAYS = [100, 200]; // Exponential backoff in ms

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Create AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), AUTH_TIMEOUT_MS);

      try {
        const res = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
          method: "GET",
          headers: {
            apikey: SUPABASE_ANON_KEY,
            Authorization: `Bearer ${token}`,
          },
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        if (!res.ok) {
          // Non-200 response - don't retry, just return null
          console.log("[Auth] token verification failed", {
            reason: "http_error",
            code: res.status,
            message: `HTTP ${res.status}`,
            attempt: attempt + 1,
          });
          return null;
        }

        const user = await res.json();
        return user?.id || null;
      } catch (fetchError) {
        clearTimeout(timeoutId);
        throw fetchError; // Re-throw to outer catch
      }
    } catch (error) {
      const errorCode = error.code || error.name || "UNKNOWN";
      const errorMessage = error.message || String(error);
      
      // Check if this is a network error that we should retry
      const isNetworkError = 
        errorCode === "ECONNRESET" ||
        errorCode === "ETIMEDOUT" ||
        errorCode === "ENOTFOUND" ||
        errorCode === "ECONNREFUSED" ||
        errorCode === "AbortError" ||
        error.message?.includes("fetch failed") ||
        error.message?.includes("network");

      if (isNetworkError && attempt < MAX_RETRIES) {
        // Retry with exponential backoff
        const delay = RETRY_DELAYS[attempt] || 200;
        console.log("[Auth] token verification retry", {
          reason: "network_error",
          code: errorCode,
          message: errorMessage,
          attempt: attempt + 1,
          maxAttempts: MAX_RETRIES + 1,
          retryDelay: delay,
        });
        await new Promise(resolve => setTimeout(resolve, delay));
        continue; // Retry
      }

      // Final attempt failed or non-retryable error
      console.log("[Auth] token verification failed", {
        reason: isNetworkError ? "network_error_final" : "error",
        code: errorCode,
        message: errorMessage,
        attempt: attempt + 1,
        willRetry: isNetworkError && attempt < MAX_RETRIES,
      });
      
      // Return null instead of throwing - never crash the server
      return null;
    }
  }

  // Should never reach here, but safety fallback
  return null;
}

// --- Normalize player identifier fields for edge function compatibility ---
function withPlayerIdVariants(userId, extra = {}) {
  // Some implementations expect user_id, some expect player_id, some expect player_ids: []
  return {
    ...extra,
    user_id: userId,
    player_id: userId,
    player_ids: [userId],
  };
}

function withTwoPlayerVariants(whiteId, blackId, extra = {}) {
  return {
    ...extra,
    white_player_id: whiteId,
    black_player_id: blackId,
    // some edge functions prefer just player_ids
    player_ids: [whiteId, blackId],
    // and sometimes generic names:
    white_id: whiteId,
    black_id: blackId,
  };
}

// --- Call Supabase Edge Function (game-server) ---
async function callGameServer(action, params = {}) {
  const res = await fetch(GAME_SERVER_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",

      // Helps with Supabase gateway consistency
      apikey: SUPABASE_ANON_KEY,

      // Keep Bearer auth (what you wanted)
      Authorization: `Bearer ${GAME_SERVER_API_KEY}`,

      // Also send common alternates to avoid mismatch
      "x-api-key": GAME_SERVER_API_KEY,
      "x-game-server-key": GAME_SERVER_API_KEY,
      "x-server-key": GAME_SERVER_API_KEY,
    },
    body: JSON.stringify({ action, ...params }),
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const msg =
      json?.error ||
      json?.message ||
      (typeof json?.raw === "string" ? json.raw : null) ||
      `Edge function error (${res.status})`;
    const err = new Error(msg);
    err.status = res.status;
    err.payload = json;
    throw err;
  }

  return json;
}

// --- State ---
// simple FIFO matchmaking by wager: Map<wager, WebSocket[]>
const queues = new Map();

// games: localId -> { chess, whiteWs, blackWs, supabaseGameId, wager, whiteId, blackId }
const games = new Map();

// Pending private games waiting for both players to connect via WebSocket
// supabaseGameId -> { localGameId, supabaseGameId, chess, wager, whiteUserId, blackUserId, whiteName, blackName, whiteWs, blackWs, loading, queuedClients }
const pendingPrivateGames = new Map();

// PART C: In-memory cache for game metadata (avoids duplicate Supabase reads on join_game)
// supabaseGameId -> { game data, timestamp }
const gameMetadataCache = new Map();
const GAME_METADATA_CACHE_TTL_MS = 60_000; // 60 seconds

function getCachedGameMetadata(supabaseGameId) {
  const entry = gameMetadataCache.get(supabaseGameId);
  if (!entry) return null;
  if (Date.now() - entry.cachedAt > GAME_METADATA_CACHE_TTL_MS) {
    gameMetadataCache.delete(supabaseGameId);
    return null;
  }
  return entry.data;
}

function setCachedGameMetadata(supabaseGameId, data) {
  gameMetadataCache.set(supabaseGameId, { data, cachedAt: Date.now() });
}

// Helper: promote a pending private game to an active game once both players connect
async function promoteToActiveGame(pending, supabaseGameId) {
  const nowMs = Date.now();
  const activeGame = {
    localGameId: pending.localGameId,
    supabaseGameId: pending.supabaseGameId,
    wager: pending.wager,
    chess: pending.chess,
    whiteWs: pending.whiteWs,
    blackWs: pending.blackWs,
    whiteId: pending.whiteUserId,
    blackId: pending.blackUserId,
    whiteTimeMs: 60000,
    blackTimeMs: 60000,
    isEnded: false,
    createdAt: nowMs,
    // Clock state (server-authoritative)
    startedAt: null,           // null until first legal move
    clockRunning: false,       // clocks frozen until first move
    lastTurnStartedAt: null,   // server timestamp when current turn began
    disconnectedAt: null,
    disconnectedPlayer: null,
    disconnectTimer: null,
  };

  games.set(pending.localGameId, activeGame);
  pendingPrivateGames.delete(supabaseGameId);

  console.log("[Server] join_game: both players connected, game started", {
    supabaseGameId,
    localGameId: pending.localGameId,
    whiteUserId: pending.whiteUserId,
    blackUserId: pending.blackUserId,
    timestamp: new Date().toISOString(),
  });

  // Update DB game status to 'active' so end_game settlement works
  try {
    await callGameServer("activate_game", { game_id: supabaseGameId });
    console.log("[Server] join_game: DB game status set to active", { supabaseGameId });
  } catch (e) {
    console.error("[Server] join_game: failed to activate DB game", {
      supabaseGameId,
      error: e.message || String(e),
    });
    // Continue anyway - game can still be played
  }

  const serverNow = Date.now();

  safeSend(pending.whiteWs, {
    type: "match_found",
    gameId: pending.localGameId,
    dbGameId: supabaseGameId,
    color: "w",
    wager: pending.wager,
    fen: pending.chess.fen(),
    turn: "w",
    // New ms-precision clock snapshot
    wMs: 60000,
    bMs: 60000,
    clockRunning: false,
    serverNow,
    lastMoveAt: null,
    whiteId: pending.whiteUserId,
    blackId: pending.blackUserId,
    // Legacy seconds fields
    whiteTime: 60,
    blackTime: 60,
    serverTimeMs: serverNow,
    opponent: {
      user_id: pending.blackUserId,
      name: pending.blackName,
    },
  });

  safeSend(pending.blackWs, {
    type: "match_found",
    gameId: pending.localGameId,
    dbGameId: supabaseGameId,
    color: "b",
    wager: pending.wager,
    fen: pending.chess.fen(),
    turn: "w",
    // New ms-precision clock snapshot
    wMs: 60000,
    bMs: 60000,
    clockRunning: false,
    serverNow,
    lastMoveAt: null,
    whiteId: pending.whiteUserId,
    blackId: pending.blackUserId,
    // Legacy seconds fields
    whiteTime: 60,
    blackTime: 60,
    serverTimeMs: serverNow,
    opponent: {
      user_id: pending.whiteUserId,
      name: pending.whiteName,
    },
  });
}

function enqueue(ws, wager) {
  const q = queues.get(wager) || [];
  q.push(ws);
  queues.set(wager, q);
  ws.inQueue = true;
  ws.wager = wager;
}

function removeFromQueue(ws) {
  if (!ws.inQueue || !ws.wager) return;
  const q = queues.get(ws.wager);
  if (!q) return;
  const idx = q.indexOf(ws);
  if (idx !== -1) q.splice(idx, 1);
  if (q.length === 0) queues.delete(ws.wager);
  ws.inQueue = false;
  ws.wager = null;
}

async function tryMatchmake(wager) {
  const q = queues.get(wager);
  if (!q || q.length < 2) return;

  // Pull first two open sockets
  let a = q.shift();
  while (a && a.readyState !== a.OPEN) a = q.shift();

  let b = q.shift();
  while (b && b.readyState !== b.OPEN) b = q.shift();

  if (!a || !b) {
    if (a) q.unshift(a);
    if (b) q.unshift(b);
    if (q.length === 0) queues.delete(wager);
    else queues.set(wager, q);
    return;
  }

  // Not searching anymore
  a.inQueue = false;
  b.inQueue = false;

  // If somehow userIds are missing, fail fast (prevents "undefined" reaching edge function)
  if (!a.userId || !b.userId) {
    safeSend(a, { type: "error", code: "AUTH_LOST", message: "Missing user id (please sign in again)." });
    safeSend(b, { type: "error", code: "AUTH_LOST", message: "Missing user id (please sign in again)." });
    return;
  }

  // 1) Verify both can afford wager
  try {
    await callGameServer("verify_wager", withPlayerIdVariants(a.userId, { wager }));
    await callGameServer("verify_wager", withPlayerIdVariants(b.userId, { wager }));
  } catch (e) {
    safeSend(a, { type: "error", code: "WAGER_DENIED", message: String(e.message || e) });
    safeSend(b, { type: "error", code: "WAGER_DENIED", message: String(e.message || e) });
    return;
  }

  // 2) Create game record in DB
  let supabaseGameId;
  try {
    const created = await callGameServer(
      "create_game",
      withTwoPlayerVariants(a.userId, b.userId, { wager })
    );

    supabaseGameId = created?.game_id || created?.id || created?.data?.id;
    if (!supabaseGameId) throw new Error("create_game did not return game_id");
  } catch (e) {
    safeSend(a, { type: "error", code: "CREATE_GAME_FAILED", message: String(e.message || e) });
    safeSend(b, { type: "error", code: "CREATE_GAME_FAILED", message: String(e.message || e) });
    return;
  }

  // 2.5) Lock wager (deduct from both players' profiles.skilled_coins)
  try {
    const lockResult = await callGameServer("lock_wager", { game_id: supabaseGameId });
    if (!lockResult?.success) {
      const errorMsg = lockResult?.details || lockResult?.error || "Failed to lock wager";
      console.error("lock_wager failed:", { lockResult, supabaseGameId });
      throw new Error(errorMsg);
    }
  } catch (e) {
    const errorMsg = e.payload?.details || e.payload?.error || e.message || String(e);
    console.error("lock_wager exception:", { 
      message: e.message, 
      payload: e.payload, 
      supabaseGameId 
    });
    safeSend(a, { type: "error", code: "WAGER_LOCK_FAILED", message: errorMsg });
    safeSend(b, { type: "error", code: "WAGER_LOCK_FAILED", message: errorMsg });
    return;
  }

  // 3) Fetch names (optional)
  let aProfile = null;
  let bProfile = null;
  try {
    aProfile = await callGameServer("get_player", withPlayerIdVariants(a.userId));
    bProfile = await callGameServer("get_player", withPlayerIdVariants(b.userId));
  } catch {
    // non-fatal
  }

  // 4) Create in-memory chess state + assign colors
  const localGameId = `g_${Math.random().toString(36).slice(2, 10)}`;
  const chess = new Chess();

  a.gameId = localGameId;
  b.gameId = localGameId;
  a.color = "w";
  b.color = "b";

  // Initialize timer state: 60 seconds (60000ms) for each player
  const nowMs = Date.now();
  games.set(localGameId, {
    localGameId,
    supabaseGameId,
    wager,
    chess,
    whiteWs: a,
    blackWs: b,
    whiteId: a.userId,
    blackId: b.userId,
    // Timer state (server-authoritative)
    whiteTimeMs: 60000,
    blackTimeMs: 60000,
    isEnded: false,
    createdAt: nowMs,
    // Clock state
    startedAt: null,           // null until first legal move
    clockRunning: false,       // clocks frozen until first move
    lastTurnStartedAt: null,   // server timestamp when current turn began
    // Disconnect grace period state
    disconnectedAt: null,
    disconnectedPlayer: null,
    disconnectTimer: null,
  });

  // Get the game object to access timer state
  const game = games.get(localGameId);
  const serverNow = Date.now();

  safeSend(a, {
    type: "match_found",
    gameId: localGameId,
    dbGameId: supabaseGameId,
    color: "w",
    wager,
    fen: chess.fen(),
    turn: "w",
    // New ms-precision clock snapshot
    wMs: 60000,
    bMs: 60000,
    clockRunning: false,
    serverNow,
    lastMoveAt: null,
    whiteId: a.userId,
    blackId: b.userId,
    // Legacy seconds fields
    whiteTime: 60,
    blackTime: 60,
    serverTimeMs: serverNow,
    opponent: {
      user_id: b.userId,
      name: bProfile?.name || bProfile?.username || bProfile?.display_name || null,
    },
  });

  safeSend(b, {
    type: "match_found",
    gameId: localGameId,
    dbGameId: supabaseGameId,
    color: "b",
    wager,
    fen: chess.fen(),
    turn: "w",
    // New ms-precision clock snapshot
    wMs: 60000,
    bMs: 60000,
    clockRunning: false,
    serverNow,
    lastMoveAt: null,
    whiteId: a.userId,
    blackId: b.userId,
    // Legacy seconds fields
    whiteTime: 60,
    blackTime: 60,
    serverTimeMs: serverNow,
    opponent: {
      user_id: a.userId,
      name: aProfile?.name || aProfile?.username || aProfile?.display_name || null,
    },
  });

  // Add connection stability check after 2 seconds
  setTimeout(() => {
    const game = games.get(localGameId);
    if (!game || game.isEnded) return;
    
    const whiteOpen = game.whiteWs && game.whiteWs.readyState === game.whiteWs.OPEN;
    const blackOpen = game.blackWs && game.blackWs.readyState === game.blackWs.OPEN;
    
    if (!whiteOpen && !blackOpen) {
      // Both disconnected immediately - likely network issue, not a real disconnect
      console.log("[Server] Both players disconnected immediately after match - likely network issue", {
        gameId: localGameId,
        timestamp: new Date().toISOString(),
      });
      // Don't end game - let them reconnect via existing reconnection logic
      return;
    }
    
    if (!whiteOpen || !blackOpen) {
      const disconnectedColor = !whiteOpen ? "w" : "b";
      // Only log - don't start timer here, let the close handler manage it
      console.log("[Server] One player connection unstable after match", {
        gameId: localGameId,
        disconnectedColor,
        timestamp: new Date().toISOString(),
      });
    }
  }, 2000);

  if (q.length === 0) queues.delete(wager);
  else queues.set(wager, q);
}

async function endGame(localGameId, reason, winnerColor = null) {
  const game = games.get(localGameId);
  if (!game) return;
  
  // Clear any disconnect timer if game is ending
  if (game.disconnectTimer) {
    clearTimeout(game.disconnectTimer);
    game.disconnectTimer = null;
  }
  
  // Mark game as ended to prevent duplicate processing
  if (game.isEnded) {
    // Game already ended - send current state (idempotent)
    console.log("[Server] endGame called but game already ended (idempotent)", {
      gameId: localGameId,
      dbGameId: game.supabaseGameId,
      lastEndReason: game.lastEndReason,
      lastWinnerColor: game.lastWinnerColor,
      newReason: reason,
      newWinnerColor: winnerColor,
    });
    const idempotentPayload = {
      type: "game_ended",
      reason: game.lastEndReason || reason,
      winnerColor: game.lastWinnerColor || winnerColor,
      gameId: localGameId,  // Always include gameId
      dbGameId: game.supabaseGameId,  // Always include dbGameId
      wager: game.wager || 0,
      creditsUpdated: true,  // DB was already updated on first endGame call
    };
    console.log("[Server] Sending idempotent game_ended to both players", {
      gameId: localGameId,
      payload: idempotentPayload,
    });
    safeSend(game.whiteWs, idempotentPayload);
    safeSend(game.blackWs, idempotentPayload);
    return;
  }
  
  game.isEnded = true;
  game.lastEndReason = reason;
  game.lastWinnerColor = winnerColor;

  // Winner user id
  let winnerId = null;
  if (winnerColor === "w") winnerId = game.whiteId;
  if (winnerColor === "b") winnerId = game.blackId;

  // ── IMMEDIATELY broadcast game_ended to BOTH players ──
  // This MUST happen BEFORE the async DB call so neither player is left
  // waiting for the slow Supabase edge function to complete.
  // Credits settlement happens in the background afterward.
  const payload = {
    type: "game_ended",
    reason,
    winnerColor,
    gameId: localGameId,
    dbGameId: game.supabaseGameId,
    wager: game.wager || 0,
    creditsUpdated: false,  // DB not yet updated — client will poll/retry
  };

  console.log("[Server] Sending game_ended to both players IMMEDIATELY", {
    gameId: localGameId,
    dbGameId: game.supabaseGameId,
    reason,
    winnerColor,
    wager: game.wager,
    whitePlayerId: game.whiteId,
    blackPlayerId: game.blackId,
    timestamp: new Date().toISOString(),
  });

  // Capture WS refs before any async work (they could close during await)
  const whiteWs = game.whiteWs;
  const blackWs = game.blackWs;

  safeSend(whiteWs, payload);
  safeSend(blackWs, payload);

  // ── Now settle credits in the background (DB update) ──
  // If this fails, the game_ended was already sent — clients can still
  // show the result. Balance sync will retry on the client side.
  let creditsUpdated = false;
  try {
    await callGameServer("end_game", {
      game_id: game.supabaseGameId,
      reason,
      // Send variants because edge implementations differ
      ...(winnerId ? withPlayerIdVariants(winnerId) : {}),
      winner_id: winnerId,
    });
    creditsUpdated = true;
    console.log("[Server] end_game DB update succeeded", {
      gameId: localGameId,
      dbGameId: game.supabaseGameId,
      timestamp: new Date().toISOString(),
    });
    // Notify clients that credits are now settled — they can refresh balance
    safeSend(whiteWs, { type: "credits_settled", gameId: localGameId, dbGameId: game.supabaseGameId });
    safeSend(blackWs, { type: "credits_settled", gameId: localGameId, dbGameId: game.supabaseGameId });
  } catch (e) {
    console.error("[Server] end_game DB update failed (game_ended already sent)", {
      gameId: localGameId,
      dbGameId: game.supabaseGameId,
      error: e.message || String(e),
      timestamp: new Date().toISOString(),
    });
    safeSend(whiteWs, { type: "error", code: "END_GAME_FAILED", message: String(e.message || e) });
    safeSend(blackWs, { type: "error", code: "END_GAME_FAILED", message: String(e.message || e) });
  }

  // Clean up WS tracking
  if (whiteWs) {
    whiteWs.gameId = null;
    whiteWs.color = null;
  }
  if (blackWs) {
    blackWs.gameId = null;
    blackWs.color = null;
  }

  // DON'T delete the game immediately — keep it for 60s so a player who
  // reconnects after alt-tab can still receive the game result.
  // The reconnect loop checks `game.isEnded` and sends the result.
  // The clock interval already skips ended games (`if (game.isEnded) continue`).
  setTimeout(() => {
    if (games.has(localGameId)) {
      games.delete(localGameId);
      console.log("[Server] Cleaned up ended game from map after 60s", { gameId: localGameId });
    }
  }, 60_000);
}

// REMOVED: updateGameState was being called on EVERY move, causing excessive Supabase writes
// Game state is now only written to DB at game creation and game end
// Live game state is authoritative in the WebSocket server memory
// This reduces Supabase writes by ~50-100 per game
/*
async function updateGameState(game) {
  try {
    await callGameServer("update_game_state", {
      game_id: game.supabaseGameId,
      fen: game.chess.fen(),
      current_turn: game.chess.turn(),
    });
  } catch {
    // non-fatal
  }
}
*/

// Calculate current clocks from server time (server-authoritative, PURE READ — no mutation)
// Uses lastTurnStartedAt to compute elapsed time for the active side
function calculateCurrentClocks(game, nowMs = Date.now()) {
  let wMs = game.whiteTimeMs;
  let bMs = game.blackTimeMs;
  const turn = game.chess.turn();

  // If clock hasn't started yet, return stored values (frozen)
  if (!game.clockRunning || !game.lastTurnStartedAt) {
    return {
      wMs,
      bMs,
      whiteTime: Math.ceil(wMs / 1000),
      blackTime: Math.ceil(bMs / 1000),
      turn,
      clockRunning: false,
      serverNow: nowMs,
      lastMoveAt: null,
      whiteId: game.whiteId,
      blackId: game.blackId,
      // Legacy
      whiteTimeMs: wMs,
      blackTimeMs: bMs,
      serverTimeMs: nowMs,
    };
  }

  // Deduct elapsed since the current turn started (read-only, does NOT mutate game)
  const elapsed = nowMs - game.lastTurnStartedAt;
  if (turn === 'w') {
    wMs = Math.max(0, wMs - elapsed);
  } else {
    bMs = Math.max(0, bMs - elapsed);
  }

  return {
    wMs,
    bMs,
    whiteTime: Math.ceil(wMs / 1000),
    blackTime: Math.ceil(bMs / 1000),
    turn,
    clockRunning: true,
    serverNow: nowMs,
    lastMoveAt: game.lastTurnStartedAt,
    whiteId: game.whiteId,
    blackId: game.blackId,
    // Legacy
    whiteTimeMs: wMs,
    blackTimeMs: bMs,
    serverTimeMs: nowMs,
  };
}

function handleMove(ws, data) {
  const localGameId = ws.gameId;
  if (!localGameId) {
    safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "You are not in a game." });
    return;
  }

  const game = games.get(localGameId);
  if (!game) {
    safeSend(ws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found." });
    return;
  }

  const chess = game.chess;
  const turn = chess.turn();
  if (ws.color !== turn) {
    safeSend(ws, { type: "error", code: "NOT_YOUR_TURN", message: "Not your turn." });
    return;
  }

  const moveObj = parseMove(data.move);
  if (!moveObj) {
    safeSend(ws, {
      type: "error",
      code: "BAD_MOVE_FORMAT",
      message: "Move must be UCI like 'e2e4' or {from,to,promotion}.",
    });
    return;
  }

  const nowMs = Date.now();
  const previousTurn = turn;  // The side that is making this move
  const isFirstMove = !game.clockRunning;

  // --- Clock accounting (server-authoritative) ---
  if (isFirstMove) {
    // First move of the game: start the clock
    game.clockRunning = true;
    game.startedAt = nowMs;
    game.lastTurnStartedAt = nowMs;
    console.log("[Server] First move — clock started", {
      gameId: localGameId,
      startedAt: new Date(nowMs).toISOString(),
      moverColor: previousTurn,
    });
    // No time deduction on first move (clock was frozen)
  } else {
    // Subsequent moves: deduct elapsed from the player who just moved
    const elapsed = nowMs - game.lastTurnStartedAt;
    if (previousTurn === 'w') {
      game.whiteTimeMs = Math.max(0, game.whiteTimeMs - elapsed);
      game.whiteTimeMs += 5000;  // Fischer increment (5s)
    } else {
      game.blackTimeMs = Math.max(0, game.blackTimeMs - elapsed);
      game.blackTimeMs += 5000;  // Fischer increment (5s)
    }
  }

  // Apply move via chess.js
  const result = chess.move(moveObj);
  if (!result) {
    // Illegal move — revert clock changes
    if (isFirstMove) {
      game.clockRunning = false;
      game.startedAt = null;
      game.lastTurnStartedAt = null;
    }
    safeSend(ws, { type: "error", code: "ILLEGAL_MOVE", message: "Illegal move." });
    return;
  }

  // Update lastTurnStartedAt for the NEW turn (the side that now has to move)
  game.lastTurnStartedAt = nowMs;

  // Calculate clock snapshot for broadcast (pure read, no mutation)
  const clocks = calculateCurrentClocks(game, nowMs);
  const newTurn = chess.turn();

  const broadcast = {
    type: "move_applied",
    gameId: localGameId,
    dbGameId: game.supabaseGameId,
    move: result,
    fen: chess.fen(),
    turn: newTurn,
    // New ms-precision clock snapshot fields
    wMs: clocks.wMs,
    bMs: clocks.bMs,
    clockRunning: clocks.clockRunning,
    serverNow: clocks.serverNow,
    lastMoveAt: clocks.lastMoveAt,
    whiteId: game.whiteId,
    blackId: game.blackId,
    // Legacy seconds fields (backward compat)
    whiteTime: clocks.whiteTime,
    blackTime: clocks.blackTime,
    serverTimeMs: clocks.serverTimeMs,
  };

  safeSend(game.whiteWs, broadcast);
  safeSend(game.blackWs, broadcast);

  // Check for game over conditions
  if (chess.isGameOver()) {
    let reason = "game_over";
    let winnerColor = null;

    if (chess.isCheckmate()) {
      reason = "checkmate";
      winnerColor = chess.turn() === "w" ? "b" : "w";
    } else if (chess.isStalemate()) {
      reason = "stalemate";
    } else if (chess.isDraw()) {
      reason = "draw";
    }

    endGame(localGameId, reason, winnerColor);
  }
}

// --- HTTP server ---
const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Chess WebSocket server is running. Connect via WS at /ws?token=...");
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not found");
});

// --- WebSocket server mounted at /ws ---
const wss = new WebSocketServer({ noServer: true });

// Upgrade HTTP -> WS only for /ws
server.on("upgrade", (req, socket, head) => {
  const parsed = url.parse(req.url, true);
  if (parsed.pathname !== "/ws") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

// Keepalive
const KEEPALIVE_MS = 25000;
function heartbeat() {
  this.isAlive = true;
}
const interval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  }
}, KEEPALIVE_MS);

wss.on("close", () => clearInterval(interval));

// --- WS connections ---
wss.on("connection", async (ws, req) => {
  // Wrap entire connection handler in try/catch to prevent crashes
  try {
    ws.isAlive = true;
    ws.on("pong", heartbeat);

    // CRITICAL: Register message handler IMMEDIATELY before any async work.
    // Messages that arrive during auth (e.g. join_game) are queued and
    // replayed after auth completes. Without this, messages are silently lost.
    let authComplete = false;
    const earlyMessageQueue = [];
    
    ws.on("message", async (raw) => {
      if (!authComplete) {
        earlyMessageQueue.push(raw);
        return;
      }
      handleWsMessage(ws, raw);
    });

    const parsed = url.parse(req.url, true);

    // token can be string | string[] | undefined
    const tokenRaw = parsed.query?.token;
    const token = Array.isArray(tokenRaw) ? tokenRaw[0] : tokenRaw;

    let userId;
    try {
      userId = await getUserIdFromToken(token);
    } catch (authError) {
      // Auth function should never throw, but catch just in case
      console.error("[Server] Unexpected error in getUserIdFromToken:", {
        error: authError.message,
        code: authError.code,
        stack: authError.stack,
      });
      userId = null;
    }

    if (!userId) {
      // Auth failed - send error and close gracefully
      const errorCode = token ? "AUTH_UNAVAILABLE" : "UNAUTHORIZED";
      const errorMessage = token 
        ? "Could not authenticate. Please retry." 
        : "Missing/invalid token.";
      
      console.log("[Server] Connection rejected - auth failed", {
        errorCode,
        hasToken: !!token,
        timestamp: new Date().toISOString(),
      });
      
      safeSend(ws, { 
        type: "error", 
        code: errorCode, 
        message: errorMessage 
      });
      ws.close(4001, errorMessage); // 4001 = Going Away (auth failed)
      return;
    }

    ws.userId = userId;
    ws.gameId = null;
    ws.color = null;
    ws.inQueue = false;
    ws.wager = null;

    // Check if user has an active game they can reconnect to,
    // OR an ended game whose result they missed (e.g. alt-tabbed away).
    let reconnectedGame = null;
    for (const [gameId, game] of games.entries()) {
      // Check if this user is a player in this game
      const isWhite = game.whiteId === userId;
      const isBlack = game.blackId === userId;
      
      if (!isWhite && !isBlack) continue;
      
      const playerColor = isWhite ? "w" : "b";
      
      // ── CASE A: Game already ended while player was disconnected ──
      // Send them the game result they missed. This handles alt-tab scenarios
      // where the WS dropped and game_ended was lost.
      if (game.isEnded) {
        console.log("[Server] Player reconnected to ENDED game — sending missed game_ended", {
          gameId,
          userId,
          playerColor,
          reason: game.lastEndReason,
          winnerColor: game.lastWinnerColor,
          timestamp: new Date().toISOString(),
        });
        safeSend(ws, {
          type: "game_ended",
          reason: game.lastEndReason || "unknown",
          winnerColor: game.lastWinnerColor || null,
          gameId,
          dbGameId: game.supabaseGameId,
          wager: game.wager || 0,
          creditsUpdated: true,  // DB was already settled by the time they reconnect
        });
        reconnectedGame = game;
        break;
      }
      
      // ── CASE B: Game is still active — reconnect within grace period ──
      if (game.disconnectedPlayer === playerColor && game.disconnectedAt) {
        const timeSinceDisconnect = Date.now() - game.disconnectedAt;
        const GRACE_PERIOD_MS = 30000;
        
        if (timeSinceDisconnect < GRACE_PERIOD_MS) {
          // Player reconnected within grace period - restore game state
          console.log("[Server] Player reconnected to existing game", {
            gameId,
            userId,
            playerColor,
            timeSinceDisconnect,
            timestamp: new Date().toISOString(),
          });
          
          // Restore WebSocket reference
          if (playerColor === "w") {
            game.whiteWs = ws;
          } else {
            game.blackWs = ws;
          }
          
          // Clear disconnect state
          if (game.disconnectTimer) {
            clearTimeout(game.disconnectTimer);
            game.disconnectTimer = null;
          }
          game.disconnectedAt = null;
          game.disconnectedPlayer = null;
          
          // Set WebSocket game state
          ws.gameId = gameId;
          ws.color = playerColor;
          
          // Notify opponent that player reconnected
          const opponentWs = playerColor === "w" ? game.blackWs : game.whiteWs;
          if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
            safeSend(opponentWs, {
              type: "opponent_reconnected",
              message: "Opponent reconnected.",
            });
          }
          
          // Send game sync to reconnected player
          const clocks = calculateCurrentClocks(game);
          safeSend(ws, {
            type: "game_reconnected",
            message: "Reconnected to your game.",
            gameId,
            dbGameId: game.supabaseGameId,
            fen: game.chess.fen(),
            turn: clocks.turn,
            color: playerColor,
            status: "active",
            wager: game.wager,
            // New ms-precision clock snapshot
            wMs: clocks.wMs,
            bMs: clocks.bMs,
            clockRunning: clocks.clockRunning,
            serverNow: clocks.serverNow,
            lastMoveAt: clocks.lastMoveAt,
            whiteId: game.whiteId,
            blackId: game.blackId,
            // Legacy seconds fields
            whiteTime: clocks.whiteTime,
            blackTime: clocks.blackTime,
            serverTimeMs: clocks.serverTimeMs,
          });
          
          reconnectedGame = game;
          break;
        }
      }
    }

    if (!reconnectedGame) {
      // Normal new connection - send welcome
      safeSend(ws, {
        type: "welcome",
        message: "Authed. Send {type:'find_match', wager:<int>} to enter matchmaking.",
        userId,
      });
    }

    // Auth is done — mark complete and replay any messages that arrived during auth
    authComplete = true;
    if (earlyMessageQueue.length > 0) {
      console.log("[Server] Replaying", earlyMessageQueue.length, "early message(s) for user", userId);
      for (const queuedRaw of earlyMessageQueue) {
        await handleWsMessage(ws, queuedRaw);
      }
      earlyMessageQueue.length = 0;
    }

  async function handleWsMessage(ws, raw) {
    // Wrap entire message handler in try/catch to prevent crashes
    try {
      let data;
      try {
        data = JSON.parse(raw.toString());
      } catch {
        safeSend(ws, { type: "error", code: "BAD_JSON", message: "Invalid JSON." });
        return;
      }

      if (!data || typeof data.type !== "string") {
        safeSend(ws, { type: "error", code: "MISSING_TYPE", message: "Message must include a string 'type'." });
        return;
      }

      // Handle each message type with individual try/catch for granular error handling
      try {
            if (data.type === "find_match") {
          if (ws.gameId) {
            safeSend(ws, { type: "error", code: "ALREADY_IN_GAME", message: "You are already in a game." });
            return;
          }
          if (ws.inQueue) {
            safeSend(ws, { type: "status", message: "Already searching..." });
            return;
          }

          const wager = parseWager(data.wager);
          if (!wager) {
            safeSend(ws, { type: "error", code: "BAD_WAGER", message: "Provide wager as a positive integer." });
            return;
          }

          enqueue(ws, wager);
          safeSend(ws, { type: "searching", message: "Searching for opponent...", wager });
          tryMatchmake(wager);
          return;
        }

        if (data.type === "cancel_search") {
          if (ws.inQueue) {
            removeFromQueue(ws);
            safeSend(ws, { type: "search_cancelled" });
          }
          return;
        }

        if (data.type === "move") {
          handleMove(ws, data);
          return;
        }

        if (data.type === "sync_game") {
          if (!ws.gameId) {
            safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "You are not in a game." });
            return;
          }
          const game = games.get(ws.gameId);
          if (!game) {
            safeSend(ws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found." });
            return;
          }
          
          // Calculate current clocks from server time (pure read)
          const clocks = calculateCurrentClocks(game);
          
          safeSend(ws, {
            type: "game_sync",
            gameId: ws.gameId,
            dbGameId: game.supabaseGameId,
            fen: game.chess.fen(),
            turn: clocks.turn,
            status: game.isEnded ? "ended" : "active",
            wager: game.wager,
            // New ms-precision clock snapshot
            wMs: clocks.wMs,
            bMs: clocks.bMs,
            clockRunning: clocks.clockRunning,
            serverNow: clocks.serverNow,
            lastMoveAt: clocks.lastMoveAt,
            whiteId: game.whiteId,
            blackId: game.blackId,
            // Legacy seconds fields
            whiteTime: clocks.whiteTime,
            blackTime: clocks.blackTime,
            serverTimeMs: clocks.serverTimeMs,
          });
          return;
        }

        if (data.type === "join_game") {
          // Private game: player sends join_game with a Supabase game UUID
          const supabaseGameId = data.gameId;
          if (!supabaseGameId) {
            safeSend(ws, { type: "error", code: "MISSING_GAME_ID", message: "gameId is required for join_game." });
            return;
          }

          // If player is already in a game for this same supabaseGameId, send sync
          if (ws.gameId) {
            const existingGame = games.get(ws.gameId);
            if (existingGame && existingGame.supabaseGameId === supabaseGameId) {
              const clocks = calculateCurrentClocks(existingGame);
              safeSend(ws, {
                type: "game_sync",
                gameId: ws.gameId,
                dbGameId: existingGame.supabaseGameId,
                fen: existingGame.chess.fen(),
                turn: clocks.turn,
                color: ws.color,
                status: existingGame.isEnded ? "ended" : "active",
                wager: existingGame.wager,
                // New ms-precision clock snapshot
                wMs: clocks.wMs,
                bMs: clocks.bMs,
                clockRunning: clocks.clockRunning,
                serverNow: clocks.serverNow,
                lastMoveAt: clocks.lastMoveAt,
                whiteId: existingGame.whiteId,
                blackId: existingGame.blackId,
                // Legacy seconds fields
                whiteTime: clocks.whiteTime,
                blackTime: clocks.blackTime,
                serverTimeMs: clocks.serverTimeMs,
              });
              return;
            }
            // In a different game - error
            safeSend(ws, { type: "error", code: "ALREADY_IN_GAME", message: "You are already in a different game." });
            return;
          }

          // Check if there's already a pending entry for this game
          let pending = pendingPrivateGames.get(supabaseGameId);

          if (!pending) {
            // First player arriving — IMMEDIATELY reserve the slot BEFORE any async work
            // This prevents a race condition where both players enter this block
            // simultaneously during the await callGameServer("get_game") call.
            const localGameId = `g_${Math.random().toString(36).slice(2, 10)}`;
            pending = {
              localGameId,
              supabaseGameId,
              loading: true,          // Still fetching game data from DB
              queuedClients: [],      // Other clients that arrive while loading
              chess: null,
              wager: 0,
              whiteUserId: null,
              blackUserId: null,
              whiteName: null,
              blackName: null,
              whiteWs: null,
              blackWs: null,
            };
            pendingPrivateGames.set(supabaseGameId, pending);

            try {
              // PART C: Check in-memory cache first to avoid duplicate Supabase reads
              let gameData = getCachedGameMetadata(supabaseGameId);
              if (!gameData) {
                gameData = await callGameServer("get_game", { game_id: supabaseGameId });
                if (gameData && gameData.success && gameData.game) {
                  setCachedGameMetadata(supabaseGameId, gameData);
                }
              }
              if (!gameData.success || !gameData.game) {
                pendingPrivateGames.delete(supabaseGameId);
                safeSend(ws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found in database." });
                for (const qws of pending.queuedClients) {
                  safeSend(qws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found in database." });
                }
                return;
              }

              const gd = gameData.game;

              // Populate the pending entry with game data
              pending.chess = new Chess(gd.fen || "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
              pending.wager = gd.wager || 0;
              pending.whiteUserId = gd.white_user_id;
              pending.blackUserId = gd.black_user_id;
              pending.whiteName = gd.white_name || null;
              pending.blackName = gd.black_name || null;
              pending.loading = false;

              // Assign THIS player (the one who triggered the fetch)
              const isWhite = ws.userId === pending.whiteUserId;
              const isBlack = ws.userId === pending.blackUserId;
              if (!isWhite && !isBlack) {
                pendingPrivateGames.delete(supabaseGameId);
                safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "You are not a player in this game." });
                for (const qws of pending.queuedClients) {
                  safeSend(qws, { type: "error", code: "NOT_IN_GAME", message: "You are not a player in this game." });
                }
                return;
              }

              if (isWhite) pending.whiteWs = ws;
              else pending.blackWs = ws;
              ws.gameId = localGameId;
              ws.color = isWhite ? "w" : "b";

              console.log("[Server] join_game: first player loaded", {
                supabaseGameId,
                localGameId,
                userId: ws.userId,
                color: ws.color,
                queuedCount: pending.queuedClients.length,
                timestamp: new Date().toISOString(),
              });

              // Process any queued clients that arrived during the await
              for (const queuedWs of pending.queuedClients) {
                if (queuedWs.readyState !== queuedWs.OPEN) continue;
                const qIsWhite = queuedWs.userId === pending.whiteUserId;
                const qIsBlack = queuedWs.userId === pending.blackUserId;
                if (!qIsWhite && !qIsBlack) {
                  safeSend(queuedWs, { type: "error", code: "NOT_IN_GAME", message: "You are not a player in this game." });
                  continue;
                }
                if (qIsWhite) pending.whiteWs = queuedWs;
                else pending.blackWs = queuedWs;
                queuedWs.gameId = pending.localGameId;
                queuedWs.color = qIsWhite ? "w" : "b";
                console.log("[Server] join_game: queued player assigned", {
                  supabaseGameId,
                  userId: queuedWs.userId,
                  color: queuedWs.color,
                });
              }
              pending.queuedClients = [];

              // Check if both players are now connected — promote to active game
              if (pending.whiteWs && pending.blackWs &&
                  pending.whiteWs.readyState === pending.whiteWs.OPEN &&
                  pending.blackWs.readyState === pending.blackWs.OPEN) {
                promoteToActiveGame(pending, supabaseGameId);
              } else {
                // Send waiting to whoever is connected
                if (pending.whiteWs && pending.whiteWs.readyState === pending.whiteWs.OPEN) {
                  safeSend(pending.whiteWs, { type: "waiting_for_opponent", message: "Connected. Waiting for opponent...", gameId: localGameId, dbGameId: supabaseGameId });
                }
                if (pending.blackWs && pending.blackWs.readyState === pending.blackWs.OPEN) {
                  safeSend(pending.blackWs, { type: "waiting_for_opponent", message: "Connected. Waiting for opponent...", gameId: localGameId, dbGameId: supabaseGameId });
                }
              }
              return;
            } catch (e) {
              pendingPrivateGames.delete(supabaseGameId);
              console.error("[Server] join_game: error fetching game", {
                supabaseGameId,
                error: e.message || String(e),
              });
              safeSend(ws, { type: "error", code: "GAME_FETCH_FAILED", message: "Failed to load game: " + (e.message || String(e)) });
              for (const qws of pending.queuedClients) {
                safeSend(qws, { type: "error", code: "GAME_FETCH_FAILED", message: "Failed to load game." });
              }
              return;
            }
          }

          // Pending entry exists but game data is still being fetched
          if (pending.loading) {
            console.log("[Server] join_game: game still loading, queuing client", {
              supabaseGameId,
              userId: ws.userId,
              queueLength: pending.queuedClients.length,
            });
            pending.queuedClients.push(ws);
            safeSend(ws, {
              type: "waiting_for_opponent",
              message: "Loading game data...",
              gameId: pending.localGameId,
              dbGameId: supabaseGameId,
            });
            return;
          }

          // Pending entry exists and is loaded - second player (or reconnecting first player)
          const isWhite = ws.userId === pending.whiteUserId;
          const isBlack = ws.userId === pending.blackUserId;

          if (!isWhite && !isBlack) {
            safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "You are not a player in this game." });
            return;
          }

          // Assign WebSocket
          if (isWhite) {
            pending.whiteWs = ws;
          } else {
            pending.blackWs = ws;
          }
          ws.gameId = pending.localGameId;
          ws.color = isWhite ? "w" : "b";

          console.log("[Server] join_game: player joined existing pending", {
            supabaseGameId,
            userId: ws.userId,
            color: ws.color,
            timestamp: new Date().toISOString(),
          });

          // Check if both players are now connected
          if (pending.whiteWs && pending.blackWs &&
              pending.whiteWs.readyState === pending.whiteWs.OPEN &&
              pending.blackWs.readyState === pending.blackWs.OPEN) {
            promoteToActiveGame(pending, supabaseGameId);
          } else {
            safeSend(ws, {
              type: "waiting_for_opponent",
              message: "Connected to game. Waiting for opponent...",
              gameId: pending.localGameId,
              dbGameId: supabaseGameId,
            });
          }
          return;
        }

        if (data.type === "clock_sync_request") {
          // Client requests a fresh clock snapshot (e.g. after alt-tab / focus)
          if (!ws.gameId) {
            safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "Not in a game." });
            return;
          }
          const game = games.get(ws.gameId);
          if (!game || game.isEnded) {
            safeSend(ws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found." });
            return;
          }
          const clocks = calculateCurrentClocks(game);
          safeSend(ws, {
            type: "clock_snapshot",
            gameId: ws.gameId,
            wMs: clocks.wMs,
            bMs: clocks.bMs,
            turn: clocks.turn,
            clockRunning: clocks.clockRunning,
            serverNow: clocks.serverNow,
            lastMoveAt: clocks.lastMoveAt,
          });
          return;
        }

        if (data.type === "resign") {
          if (!ws.gameId) {
            console.log("[Server] RESIGN received - NOT_IN_GAME", { userId: ws.userId, wsReadyState: ws.readyState });
            safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "You are not in a game." });
            return;
          }
          const game = games.get(ws.gameId);
          if (!game) {
            console.log("[Server] RESIGN received - GAME_NOT_FOUND", { gameId: ws.gameId, userId: ws.userId });
            safeSend(ws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found." });
            return;
          }
          
          console.log("[Server] RESIGN received", {
            gameId: ws.gameId,
            dbGameId: game.supabaseGameId,
            resigningPlayerId: ws.userId,
            resigningPlayerColor: ws.color,
            gameIsEnded: game.isEnded,
            timestamp: new Date().toISOString(),
          });
          
          // Idempotent: if game already ended, send current ended state
          if (game.isEnded) {
            console.log("[Server] RESIGN - game already ended, sending current state", {
              gameId: ws.gameId,
              lastEndReason: game.lastEndReason,
              lastWinnerColor: game.lastWinnerColor,
            });
            safeSend(ws, {
              type: "game_ended",
              reason: game.lastEndReason || "resign",
              winnerColor: game.lastWinnerColor,
              gameId: ws.gameId,
              dbGameId: game.supabaseGameId,
            });
            return;
          }
          
          const winnerColor = ws.color === "w" ? "b" : "w";
          console.log("[Server] RESIGN applied - calling endGame", {
            gameId: ws.gameId,
            dbGameId: game.supabaseGameId,
            resigningPlayerColor: ws.color,
            winnerColor,
            reason: "resign",
          });
          endGame(ws.gameId, "resign", winnerColor);
          return;
        }

        safeSend(ws, { type: "error", code: "UNKNOWN_TYPE", message: `Unknown type '${data.type}'.` });
      } catch (messageError) {
        // Error handling for individual message type
        console.error("[Server] Error handling message", {
          error: messageError.message,
          code: messageError.code,
          stack: messageError.stack,
          messageType: data?.type || "unknown",
          gameId: ws.gameId || "none",
          userId: ws.userId || "unknown",
          timestamp: new Date().toISOString(),
        });
        
        // Send error to client but don't crash
        safeSend(ws, {
          type: "error",
          code: "SERVER_ERROR",
          message: "Something went wrong processing your message. Please try again.",
        });
        // Do NOT end game for opponent, do NOT crash
      }
    } catch (outerError) {
      // Catch any errors in message parsing or outer handler
      console.error("[Server] Unhandled error in message handler", {
        error: outerError.message,
        code: outerError.code,
        stack: outerError.stack,
        userId: ws.userId || "unknown",
        timestamp: new Date().toISOString(),
      });
      
      // Send error to client
      safeSend(ws, {
        type: "error",
        code: "SERVER_ERROR",
        message: "An error occurred. Please reconnect.",
      });
      // Do NOT crash, do NOT end game
    }
  }

  ws.on("close", () => {
    if (ws.inQueue) removeFromQueue(ws);

    // Handle disconnect with grace period (30 seconds)
    if (ws.gameId) {
      const game = games.get(ws.gameId);
      if (game && !game.isEnded) {
        const disconnectedColor = ws.color;
        const disconnectedUserId = ws.userId;
        const gameAge = Date.now() - game.createdAt;
        const MATCH_ESTABLISHMENT_GRACE_MS = 5000; // 5 seconds
        
        console.log("[Server] Player disconnected", {
          gameId: ws.gameId,
          disconnectedPlayerColor: disconnectedColor,
          disconnectedPlayerId: disconnectedUserId,
          gameAge: gameAge,
          isDuringEstablishment: gameAge < MATCH_ESTABLISHMENT_GRACE_MS,
          timestamp: new Date().toISOString(),
        });
        
        // If game was just created, don't start disconnect timer immediately
        // This handles cases where connection closes before match_found is received
        if (gameAge < MATCH_ESTABLISHMENT_GRACE_MS) {
          console.log("[Server] Player disconnected during match establishment - waiting before starting timer", {
            gameId: ws.gameId,
            gameAge,
            disconnectedPlayerColor: disconnectedColor,
            timestamp: new Date().toISOString(),
          });
          
          // Wait for match establishment period to pass, then check if still disconnected
          setTimeout(() => {
            const currentGame = games.get(ws.gameId);
            if (!currentGame || currentGame.isEnded) return;
            
            const disconnectedWs = disconnectedColor === "w" ? currentGame.whiteWs : currentGame.blackWs;
            const isStillDisconnected = !disconnectedWs || 
                                       disconnectedWs.readyState !== disconnectedWs.OPEN ||
                                       disconnectedWs.userId !== disconnectedUserId;
            
            if (isStillDisconnected && !currentGame.disconnectedPlayer) {
              // Still disconnected after grace period - start normal disconnect handling
              console.log("[Server] Player still disconnected after match establishment period - starting disconnect timer", {
                gameId: ws.gameId,
                disconnectedPlayerColor: disconnectedColor,
                timestamp: new Date().toISOString(),
              });
              
              // Clear any existing disconnect timer
              if (currentGame.disconnectTimer) {
                clearTimeout(currentGame.disconnectTimer);
                currentGame.disconnectTimer = null;
              }
              
              // Mark player as disconnected
              currentGame.disconnectedAt = Date.now();
              currentGame.disconnectedPlayer = disconnectedColor;
              
              // Clear the WebSocket reference for the disconnected player
              if (disconnectedColor === "w") {
                currentGame.whiteWs = null;
              } else {
                currentGame.blackWs = null;
              }
              
              // Notify opponent that player disconnected (but game not ended yet)
              const opponentWs = disconnectedColor === "w" ? currentGame.blackWs : currentGame.whiteWs;
              if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
                safeSend(opponentWs, {
                  type: "opponent_disconnected",
                  message: "Opponent disconnected. Waiting for reconnection...",
                  gracePeriodSeconds: 30,
                });
              }
              
              // Set grace period timer (30 seconds)
              const GRACE_PERIOD_MS = 30000; // 30 seconds
              const gameIdForTimer = ws.gameId;
              currentGame.disconnectTimer = setTimeout(() => {
                // Check if player reconnected
                const finalGame = games.get(gameIdForTimer);
                if (!finalGame || finalGame.isEnded) {
                  return; // Game already ended or doesn't exist
                }
                
                // Check if the disconnected player reconnected
                const disconnectedWs = disconnectedColor === "w" ? finalGame.whiteWs : finalGame.blackWs;
                const isReconnected = disconnectedWs && 
                                      disconnectedWs.readyState === disconnectedWs.OPEN &&
                                      disconnectedWs.userId === disconnectedUserId;
                
                if (!isReconnected) {
                  // Player did not reconnect within grace period - end game
                  console.log("[Server] Disconnect grace period expired - ending game", {
                    gameId: gameIdForTimer,
                    disconnectedPlayerColor: disconnectedColor,
                    timestamp: new Date().toISOString(),
                  });
                  
                  const winnerColor = disconnectedColor === "w" ? "b" : "w";
                  endGame(gameIdForTimer, "disconnect_timeout", winnerColor);
                } else {
                  // Player reconnected - clear disconnect state
                  console.log("[Server] Player reconnected within grace period", {
                    gameId: gameIdForTimer,
                    reconnectedPlayerColor: disconnectedColor,
                    timestamp: new Date().toISOString(),
                  });
                  
                  finalGame.disconnectedAt = null;
                  finalGame.disconnectedPlayer = null;
                  finalGame.disconnectTimer = null;
                  
                  // Notify opponent that player reconnected
                  const opponentWs = disconnectedColor === "w" ? finalGame.blackWs : finalGame.whiteWs;
                  if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
                    safeSend(opponentWs, {
                      type: "opponent_reconnected",
                      message: "Opponent reconnected.",
                    });
                  }
                }
              }, GRACE_PERIOD_MS);
            }
          }, MATCH_ESTABLISHMENT_GRACE_MS - gameAge);
          return;
        }
        
        // Existing disconnect handling for established games
        // Clear any existing disconnect timer
        if (game.disconnectTimer) {
          clearTimeout(game.disconnectTimer);
          game.disconnectTimer = null;
        }
        
        // Mark player as disconnected
        game.disconnectedAt = Date.now();
        game.disconnectedPlayer = disconnectedColor;
        
        // Clear the WebSocket reference for the disconnected player
        if (disconnectedColor === "w") {
          game.whiteWs = null;
        } else {
          game.blackWs = null;
        }
        
        // Notify opponent that player disconnected (but game not ended yet)
        const opponentWs = disconnectedColor === "w" ? game.blackWs : game.whiteWs;
        if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
          safeSend(opponentWs, {
            type: "opponent_disconnected",
            message: "Opponent disconnected. Waiting for reconnection...",
            gracePeriodSeconds: 30,
          });
        }
        
        // Set grace period timer (30 seconds)
        const GRACE_PERIOD_MS = 30000; // 30 seconds
        const gameIdForTimer = ws.gameId;
        game.disconnectTimer = setTimeout(() => {
          // Check if player reconnected
          const currentGame = games.get(gameIdForTimer);
          if (!currentGame || currentGame.isEnded) {
            return; // Game already ended or doesn't exist
          }
          
          // Check if the disconnected player reconnected
          const disconnectedWs = disconnectedColor === "w" ? currentGame.whiteWs : currentGame.blackWs;
          const isReconnected = disconnectedWs && 
                                disconnectedWs.readyState === disconnectedWs.OPEN &&
                                disconnectedWs.userId === disconnectedUserId;
          
          if (!isReconnected) {
            // Player did not reconnect within grace period - end game
            console.log("[Server] Disconnect grace period expired - ending game", {
              gameId: gameIdForTimer,
              disconnectedPlayerColor: disconnectedColor,
              timestamp: new Date().toISOString(),
            });
            
            const winnerColor = disconnectedColor === "w" ? "b" : "w";
            endGame(gameIdForTimer, "disconnect_timeout", winnerColor);
          } else {
            // Player reconnected - clear disconnect state
            console.log("[Server] Player reconnected within grace period", {
              gameId: gameIdForTimer,
              reconnectedPlayerColor: disconnectedColor,
              timestamp: new Date().toISOString(),
            });
            
            currentGame.disconnectedAt = null;
            currentGame.disconnectedPlayer = null;
            currentGame.disconnectTimer = null;
            
            // Notify opponent that player reconnected
            const opponentWs = disconnectedColor === "w" ? currentGame.blackWs : currentGame.whiteWs;
            if (opponentWs && opponentWs.readyState === opponentWs.OPEN) {
              safeSend(opponentWs, {
                type: "opponent_reconnected",
                message: "Opponent reconnected.",
              });
            }
          }
        }, GRACE_PERIOD_MS);
      }
    }
  });
  
  } catch (error) {
    // Catch any unhandled errors in connection handler
    console.error("[Server] Unhandled error in connection handler:", {
      error: error.message,
      code: error.code,
      stack: error.stack,
      userId: ws.userId || "unknown",
      timestamp: new Date().toISOString(),
    });
    
    // Send error to client if socket is still open
    try {
      safeSend(ws, {
        type: "error",
        code: "SERVER_ERROR",
        message: "Connection error occurred. Please reconnect.",
      });
      ws.close(1011, "Internal server error"); // 1011 = Internal Error
    } catch (closeError) {
      // Socket might already be closed, ignore
      console.log("[Server] Could not send error to client (socket closed)");
    }
  }
});

// Periodic clock sync and time-loss detection (1 Hz)
// IMPORTANT: This interval does NOT mutate game.whiteTimeMs / game.blackTimeMs.
// Clock deduction happens exclusively in handleMove. This interval only READS
// via calculateCurrentClocks and broadcasts a sync snapshot for drift correction.
const CLOCK_TICK_MS = 1000;
const clockInterval = setInterval(() => {
  for (const [localGameId, game] of games.entries()) {
    if (game.isEnded) continue;
    if (!game.clockRunning || !game.lastTurnStartedAt) continue;

    // Pure read — no mutation
    const clocks = calculateCurrentClocks(game);

    // Time-loss detection
    if (clocks.wMs <= 0 && clocks.turn === 'w') {
      // Finalize clock before ending
      const now = Date.now();
      const elapsed = now - game.lastTurnStartedAt;
      game.whiteTimeMs = Math.max(0, game.whiteTimeMs - elapsed);
      game.lastTurnStartedAt = now;
      endGame(localGameId, "time_loss", "b");
      continue;
    }
    if (clocks.bMs <= 0 && clocks.turn === 'b') {
      const now = Date.now();
      const elapsed = now - game.lastTurnStartedAt;
      game.blackTimeMs = Math.max(0, game.blackTimeMs - elapsed);
      game.lastTurnStartedAt = now;
      endGame(localGameId, "time_loss", "w");
      continue;
    }

    // Broadcast lightweight clock snapshot to both players (1Hz drift correction)
    const snapshot = {
      type: "clock_snapshot",
      gameId: localGameId,
      wMs: clocks.wMs,
      bMs: clocks.bMs,
      turn: clocks.turn,
      clockRunning: clocks.clockRunning,
      serverNow: clocks.serverNow,
      lastMoveAt: clocks.lastMoveAt,
    };

    safeSend(game.whiteWs, snapshot);
    safeSend(game.blackWs, snapshot);
  }
}, CLOCK_TICK_MS);

server.listen(PORT, () => {
  console.log(`HTTP server on :${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}/ws?token=...`);
});

// Clean up clock interval on server shutdown
process.on('SIGTERM', () => {
  clearInterval(clockInterval);
});
process.on('SIGINT', () => {
  clearInterval(clockInterval);
});
