import http from "http";
import { WebSocketServer } from "ws";
import { Chess } from "chess.js";

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;

// --- Simple helpers ---
function safeSend(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function makeId(prefix = "") {
  return prefix + Math.random().toString(36).slice(2, 10);
}

// Accept either "e2e4" UCI string OR {from,to,promotion}
function parseMove(input) {
  if (!input) return null;

  if (typeof input === "string") {
    // UCI: e2e4 or e7e8q
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
    return promotion ? { from: f, to: t, promotion: String(promotion).toLowerCase() } : { from: f, to: t };
  }

  return null;
}

// --- State ---
/**
 * waitingQueue holds WS connections that have requested matchmaking.
 * We keep it FIFO.
 */
const waitingQueue = [];
/**
 * games: gameId -> { chess, white, black }
 */
const games = new Map();

// --- HTTP server (for Fly health checks + debugging) ---
const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Chess WebSocket server is running. Connect via WS at /ws");
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not found");
});

// --- WebSocket server mounted at /ws ---
const wss = new WebSocketServer({ noServer: true });

// Upgrade HTTP -> WS only for /ws
server.on("upgrade", (req, socket, head) => {
  if (req.url !== "/ws") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

// Keepalive (helps proxies keep WS alive)
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

// --- Matchmaking + gameplay ---
function removeFromQueue(ws) {
  const idx = waitingQueue.indexOf(ws);
  if (idx !== -1) waitingQueue.splice(idx, 1);
}

function tryMatchmake() {
  // Pair the first two valid sockets in queue
  while (waitingQueue.length >= 2) {
    const a = waitingQueue.shift();
    const b = waitingQueue.shift();

    // If either has closed, skip and continue
    if (!a || a.readyState !== a.OPEN) continue;
    if (!b || b.readyState !== b.OPEN) continue;

    const gameId = makeId("g_");
    const chess = new Chess();

    // Assign colors (first = white, second = black)
    a.gameId = gameId;
    b.gameId = gameId;
    a.color = "w";
    b.color = "b";

    games.set(gameId, { chess, white: a, black: b });

    safeSend(a, { type: "match_found", gameId, color: "w", fen: chess.fen() });
    safeSend(b, { type: "match_found", gameId, color: "b", fen: chess.fen() });

    return; // one match at a time
  }
}

function endGame(gameId, reason, winnerColor = null) {
  const game = games.get(gameId);
  if (!game) return;

  const payload = { type: "game_ended", reason, winnerColor };
  safeSend(game.white, payload);
  safeSend(game.black, payload);

  // Clean fields on sockets
  if (game.white) {
    game.white.gameId = null;
    game.white.color = null;
  }
  if (game.black) {
    game.black.gameId = null;
    game.black.color = null;
  }

  games.delete(gameId);
}

function handleMove(ws, data) {
  const gameId = ws.gameId;
  if (!gameId) {
    safeSend(ws, { type: "error", code: "NOT_IN_GAME", message: "You are not in a game." });
    return;
  }

  const game = games.get(gameId);
  if (!game) {
    safeSend(ws, { type: "error", code: "GAME_NOT_FOUND", message: "Game not found." });
    return;
  }

  const chess = game.chess;
  const turn = chess.turn(); // 'w' or 'b'
  if (ws.color !== turn) {
    safeSend(ws, { type: "error", code: "NOT_YOUR_TURN", message: "Not your turn." });
    return;
  }

  const moveObj = parseMove(data.move);
  if (!moveObj) {
    safeSend(ws, { type: "error", code: "BAD_MOVE_FORMAT", message: "Move must be UCI like 'e2e4' or {from,to,promotion}." });
    return;
  }

  const result = chess.move(moveObj);
  if (!result) {
    safeSend(ws, { type: "error", code: "ILLEGAL_MOVE", message: "Illegal move." });
    return;
  }

  const broadcast = {
    type: "move_applied",
    gameId,
    move: result,        // includes san, from, to, captured, promotion, etc.
    fen: chess.fen(),
    turn: chess.turn()
  };

  safeSend(game.white, broadcast);
  safeSend(game.black, broadcast);

  // Game over?
  if (chess.isGameOver()) {
    let reason = "game_over";
    let winnerColor = null;

    if (chess.isCheckmate()) {
      reason = "checkmate";
      // If game is over by checkmate, side to move is checkmated
      // So winner is the opposite of chess.turn()
      winnerColor = chess.turn() === "w" ? "b" : "w";
    } else if (chess.isStalemate()) {
      reason = "stalemate";
    } else if (chess.isDraw()) {
      reason = "draw";
    }

    endGame(gameId, reason, winnerColor);
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", heartbeat);

  // Per-connection fields
  ws.gameId = null;
  ws.color = null;
  ws.inQueue = false;

  safeSend(ws, {
    type: "welcome",
    message: "Connected. Send {type:'find_match'} to enter matchmaking."
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
      ws.inQueue = true;
      waitingQueue.push(ws);
      safeSend(ws, { type: "searching", message: "Searching for opponent..." });
      tryMatchmake();
      return;
    }

    if (data.type === "cancel_search") {
      if (ws.inQueue) {
        ws.inQueue = false;
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
    // If queued, remove
    if (ws.inQueue) {
      ws.inQueue = false;
      removeFromQueue(ws);
    }

    // If in game, opponent wins by disconnect
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
  console.log(`WebSocket endpoint: ws://localhost:${PORT}/ws`);
});
