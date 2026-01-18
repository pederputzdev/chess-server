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

  const res = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    method: "GET",
    headers: {
      apikey: SUPABASE_ANON_KEY,
      Authorization: `Bearer ${token}`,
    },
  });

  if (!res.ok) return null;

  const user = await res.json();
  return user?.id || null;
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
      throw new Error(lockResult?.error || "Failed to lock wager");
    }
  } catch (e) {
    safeSend(a, { type: "error", code: "WAGER_LOCK_FAILED", message: String(e.message || e) });
    safeSend(b, { type: "error", code: "WAGER_LOCK_FAILED", message: String(e.message || e) });
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

  games.set(localGameId, {
    localGameId,
    supabaseGameId,
    wager,
    chess,
    whiteWs: a,
    blackWs: b,
    whiteId: a.userId,
    blackId: b.userId,
  });

  safeSend(a, {
    type: "match_found",
    gameId: localGameId,
    dbGameId: supabaseGameId,
    color: "w",
    wager,
    fen: chess.fen(),
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
    opponent: {
      user_id: a.userId,
      name: aProfile?.name || aProfile?.username || aProfile?.display_name || null,
    },
  });

  if (q.length === 0) queues.delete(wager);
  else queues.set(wager, q);
}

async function endGame(localGameId, reason, winnerColor = null) {
  const game = games.get(localGameId);
  if (!game) return;

  const payload = {
    type: "game_ended",
    reason,
    winnerColor,
    gameId: localGameId,
    dbGameId: game.supabaseGameId,
  };

  safeSend(game.whiteWs, payload);
  safeSend(game.blackWs, payload);

  // Winner user id
  let winnerId = null;
  if (winnerColor === "w") winnerId = game.whiteId;
  if (winnerColor === "b") winnerId = game.blackId;

  try {
    await callGameServer("end_game", {
      game_id: game.supabaseGameId,
      reason,
      // Send variants because edge implementations differ
      ...(winnerId ? withPlayerIdVariants(winnerId) : {}),
      winner_id: winnerId,
    });
  } catch (e) {
    safeSend(game.whiteWs, { type: "error", code: "END_GAME_FAILED", message: String(e.message || e) });
    safeSend(game.blackWs, { type: "error", code: "END_GAME_FAILED", message: String(e.message || e) });
  }

  if (game.whiteWs) {
    game.whiteWs.gameId = null;
    game.whiteWs.color = null;
  }
  if (game.blackWs) {
    game.blackWs.gameId = null;
    game.blackWs.color = null;
  }

  games.delete(localGameId);
}

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

  const result = chess.move(moveObj);
  if (!result) {
    safeSend(ws, { type: "error", code: "ILLEGAL_MOVE", message: "Illegal move." });
    return;
  }

  const broadcast = {
    type: "move_applied",
    gameId: localGameId,
    dbGameId: game.supabaseGameId,
    move: result,
    fen: chess.fen(),
    turn: chess.turn(),
  };

  safeSend(game.whiteWs, broadcast);
  safeSend(game.blackWs, broadcast);

  updateGameState(game);

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
  ws.isAlive = true;
  ws.on("pong", heartbeat);

  const parsed = url.parse(req.url, true);

  // token can be string | string[] | undefined
  const tokenRaw = parsed.query?.token;
  const token = Array.isArray(tokenRaw) ? tokenRaw[0] : tokenRaw;

  const userId = await getUserIdFromToken(token);
  if (!userId) {
    safeSend(ws, { type: "error", code: "UNAUTHORIZED", message: "Missing/invalid token." });
    ws.close();
    return;
  }

  ws.userId = userId;
  ws.gameId = null;
  ws.color = null;
  ws.inQueue = false;
  ws.wager = null;

  safeSend(ws, {
    type: "welcome",
    message: "Authed. Send {type:'find_match', wager:<int>} to enter matchmaking.",
    userId,
  });

  ws.on("message", (raw) => {
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

    if (data.type === "resign") {
      if (!ws.gameId) return;
      const game = games.get(ws.gameId);
      if (!game) return;
      const winnerColor = ws.color === "w" ? "b" : "w";
      endGame(ws.gameId, "resign", winnerColor);
      return;
    }

    safeSend(ws, { type: "error", code: "UNKNOWN_TYPE", message: `Unknown type '${data.type}'.` });
  });

  ws.on("close", () => {
    if (ws.inQueue) removeFromQueue(ws);

    // Disconnect = opponent wins
    if (ws.gameId) {
      const game = games.get(ws.gameId);
      if (game) {
        const winnerColor = ws.color === "w" ? "b" : "w";
        endGame(ws.gameId, "disconnect", winnerColor);
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`HTTP server on :${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}/ws?token=...`);
});
