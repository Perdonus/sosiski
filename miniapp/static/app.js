const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;

const els = {
  balanceRub: document.getElementById("balanceRub"),
  balanceStars: document.getElementById("balanceStars"),
  freeSpins: document.getElementById("freeSpins"),
  paidSpins: document.getElementById("paidSpins"),
  resetTimer: document.getElementById("resetTimer"),
  spinHint: document.getElementById("spinHint"),
  spinBtn: document.getElementById("spinBtn"),
  kazikPanel: document.getElementById("kazikPanel"),
  resultCard: document.getElementById("resultCard"),
  resultBody: document.getElementById("resultBody"),
  resultTitle: document.getElementById("resultTitle"),
  toastContainer: document.getElementById("toastContainer"),
  upgradeGrid: document.getElementById("upgradeGrid"),
  upgradeTotal: document.getElementById("upgradeTotal"),
  upgradeChance: document.getElementById("upgradeChance"),
  upgradeSpin: document.getElementById("upgradeSpin"),
  upgradeWheel: document.getElementById("upgradeWheel"),
  upgradeWheelArc: document.getElementById("upgradeWheelArc"),
  upgradeWheelGroup: document.getElementById("upgradeWheelGroup"),
  upgradePointer: document.getElementById("upgradePointer"),
  upgradeContinue: document.getElementById("upgradeContinue"),
  upgradeBackStep: document.getElementById("upgradeBackStep"),
  upgradeStepTitle: document.getElementById("upgradeStepTitle"),
  upgradeStepHint: document.getElementById("upgradeStepHint"),
  upgradeFilterGroup: document.getElementById("upgradeFilterGroup"),
  openCards: document.getElementById("openCards"),
  openChess: document.getElementById("openChess"),
  cardsLobbies: document.getElementById("cardsLobbies"),
  cardsCreate: document.getElementById("cardsCreate"),
  cardsDeck: document.getElementById("cardsDeck"),
  cardsMode: document.getElementById("cardsMode"),
  cardsBetType: document.getElementById("cardsBetType"),
  cardsBetAmount: document.getElementById("cardsBetAmount"),
  cardsLobbyMeta: document.getElementById("cardsLobbyMeta"),
  cardsGameMeta: document.getElementById("cardsGameMeta"),
  cardsPlayers: document.getElementById("cardsPlayers"),
  cardsDeckStack: document.getElementById("cardsDeckStack"),
  cardsDeckCount: document.getElementById("cardsDeckCount"),
  cardsTrump: document.getElementById("cardsTrump"),
  cardsTurnTimer: document.getElementById("cardsTurnTimer"),
  cardsTable: document.getElementById("cardsTable"),
  cardsHand: document.getElementById("cardsHand"),
  cardsStart: document.getElementById("cardsStart"),
  cardsTake: document.getElementById("cardsTake"),
  cardsPass: document.getElementById("cardsPass"),
  cardsLeave: document.getElementById("cardsLeave"),
  cardsStakeModal: document.getElementById("cardsStakeModal"),
  cardsStakeList: document.getElementById("cardsStakeList"),
  cardsStakeClose: document.getElementById("cardsStakeClose"),
  backCardsGame: document.getElementById("backCardsGame"),
  chessLobbies: document.getElementById("chessLobbies"),
  chessCreate: document.getElementById("chessCreate"),
  chessBetType: document.getElementById("chessBetType"),
  chessBetAmount: document.getElementById("chessBetAmount"),
  chessLobbyMeta: document.getElementById("chessLobbyMeta"),
  chessGameMeta: document.getElementById("chessGameMeta"),
  chessTurnTimer: document.getElementById("chessTurnTimer"),
  chessPlayers: document.getElementById("chessPlayers"),
  chessBoard: document.getElementById("chessBoard"),
  chessLeave: document.getElementById("chessLeave"),
  backChessGame: document.getElementById("backChessGame"),
  backChess: document.getElementById("backChess"),
};

const screens = {
  home: document.getElementById("screen-home"),
  kazik: document.getElementById("screen-kazik"),
  upgrade: document.getElementById("screen-upgrade"),
  cards: document.getElementById("screen-cards"),
  cardsGame: document.getElementById("screen-cards-game"),
  chess: document.getElementById("screen-chess"),
  chessGame: document.getElementById("screen-chess-game"),
};

const state = {
  data: null,
  busy: false,
};

let kazikTimer = null;
let kazikTimerLeft = 0;

const upgradeState = {
  inventory: [],
  targets: [],
  selectedIds: [],
  selectedItem: null,
  selectedTarget: null,
  filter: 75,
  totalValue: 0,
  chance: 0,
  rotation: 0,
  step: "inventory",
};

const cardsState = {
  lobbies: [],
  currentLobbyId: null,
  currentState: null,
  selectedAttackIndex: null,
  lobbyTimer: null,
  gameTimer: null,
  turnTimer: null,
  turnEndsAt: null,
  turnPrefix: "",
  pendingLobbyId: null,
  prevHandIds: [],
  prevDeckCount: null,
  prevTable: [],
  finishedShown: false,
};

const chessState = {
  lobbies: [],
  currentLobbyId: null,
  currentState: null,
  lobbyTimer: null,
  gameTimer: null,
  turnTimer: null,
  turnEndsAt: null,
  turnPrefix: "",
  selectedCell: null,
  availableMoves: [],
  availableMoveSet: new Set(),
  finishedShown: false,
};

function setScreen(target) {
  Object.values(screens).forEach((screen) => {
    screen.classList.toggle("screen-active", screen === target);
  });
}

function formatSeconds(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const parts = [];
  if (h) parts.push(`${h}ч`);
  if (m) parts.push(`${m}м`);
  if (s || parts.length === 0) parts.push(`${s}с`);
  return parts.join(" ");
}

function stopKazikTimer() {
  if (kazikTimer) {
    clearInterval(kazikTimer);
    kazikTimer = null;
  }
}

function startKazikTimer(seconds) {
  stopKazikTimer();
  kazikTimerLeft = Math.max(0, Math.floor(seconds));
  if (!els.resetTimer) return;
  if (!kazikTimerLeft) {
    els.resetTimer.textContent = "";
    return;
  }
  els.resetTimer.textContent = `Сброс через ${formatSeconds(kazikTimerLeft)}`;
  kazikTimer = setInterval(() => {
    kazikTimerLeft = Math.max(0, kazikTimerLeft - 1);
    if (!kazikTimerLeft) {
      stopKazikTimer();
      if (els.resetTimer) {
        els.resetTimer.textContent = "";
      }
      return;
    }
    if (els.resetTimer) {
      els.resetTimer.textContent = `Сброс через ${formatSeconds(kazikTimerLeft)}`;
    }
  }, 1000);
}

function getBotUsername() {
  if (tg && tg.initDataUnsafe && tg.initDataUnsafe.receiver) {
    const receiver = tg.initDataUnsafe.receiver;
    if (receiver && receiver.username) {
      return receiver.username;
    }
  }
  const raw = document.body.dataset.bot || "";
  return raw.replace("@", "");
}

async function openStarsMenu() {
  if (!tg) {
    showResult("Нет Telegram", "Открой mini app внутри Telegram.");
    return;
  }
  try {
    tg.sendData(JSON.stringify({ action: "open_stars" }));
  } catch (err) {
    // Ignore sendData errors; we'll still open the chat.
  }
  try {
    const response = await api("/miniapp/api/open_stars", {
      method: "POST",
      body: JSON.stringify({}),
    });
    if (response && response.ok) {
      const bot = getBotUsername();
      if (bot) {
        const chatLink = `https://t.me/${bot}`;
        if (tg.openTelegramLink) {
          tg.openTelegramLink(chatLink);
        } else if (tg.openLink) {
          tg.openLink(chatLink);
        } else {
          window.location.href = chatLink;
        }
        return;
      }
    }
  } catch (error) {
    // Fall back to opening the bot chat if the API fails.
  }
  const bot = getBotUsername();
  if (!bot) {
    showResult("Ошибка", "Не удалось открыть меню звёзд.");
    return;
  }
  const deepLink = `https://t.me/${bot}?start=pay`;
  if (tg.openTelegramLink) {
    tg.openTelegramLink(deepLink);
  } else if (tg.openLink) {
    tg.openLink(deepLink);
  } else {
    window.location.href = deepLink;
  }
}

function setBusy(value) {
  state.busy = value;
  document.body.classList.toggle("is-spinning", value);
}

function showToast(title, bodyHtml, options = {}) {
  if (!els.toastContainer) return;
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.innerHTML = `
    <div class="toast-header">
      <div class="toast-title">${title}</div>
      <button class="toast-close" type="button">✕</button>
    </div>
    <div class="toast-body">${bodyHtml}</div>
  `;
  const closeBtn = toast.querySelector(".toast-close");
  const dismiss = () => {
    toast.classList.add("dismissing");
    setTimeout(() => toast.remove(), 200);
  };
  if (closeBtn) {
    closeBtn.addEventListener("click", dismiss);
  }
  let startX = 0;
  let startY = 0;
  let tracking = false;
  toast.addEventListener("pointerdown", (event) => {
    startX = event.clientX;
    startY = event.clientY;
    tracking = true;
  });
  toast.addEventListener("pointermove", (event) => {
    if (!tracking) return;
    const dx = event.clientX - startX;
    const dy = event.clientY - startY;
    if (Math.abs(dx) > 60 && Math.abs(dx) > Math.abs(dy)) {
      tracking = false;
      dismiss();
    }
  });
  toast.addEventListener("pointerup", () => {
    tracking = false;
  });
  els.toastContainer.appendChild(toast);
  if (options.auto !== false) {
    setTimeout(() => {
      if (toast.isConnected) dismiss();
    }, 6000);
  }
}

function showModal(title, bodyHtml) {
  if (els.resultTitle) {
    els.resultTitle.textContent = title;
  }
  if (els.resultBody) {
    els.resultBody.innerHTML = bodyHtml;
  }
  if (els.resultCard) {
    els.resultCard.classList.remove("hidden");
  }
}

function showResult(title, bodyHtml) {
  showToast(title, bodyHtml);
}

function hideResult() {
  if (els.resultCard) {
    els.resultCard.classList.add("hidden");
  }
}

function showUpgradeResult(title, bodyHtml) {
  showModal(title, bodyHtml);
}

function hideUpgradeResult() {
  hideResult();
}

function updateState(data) {
  state.data = data;
  const payload = data.state;
  els.balanceRub.textContent = payload.balance;
  els.balanceStars.textContent = payload.stars;
  const freeTotal = payload.kazik.daily_free_left + payload.kazik.bonus_spins;
  els.freeSpins.textContent = freeTotal;
  els.paidSpins.textContent = payload.kazik.bonus_spins;

  if (payload.kazik.reset_seconds) {
    startKazikTimer(payload.kazik.reset_seconds);
  } else {
    stopKazikTimer();
    if (els.resetTimer) {
      els.resetTimer.textContent = "";
    }
  }

  if (freeTotal > 0) {
    els.spinHint.textContent = "Фри крутки активны";
    els.spinBtn.textContent = "Крутить";
  } else {
    els.spinHint.textContent = `Крутка за ${payload.kazik.spin_cost}⭐`;
    els.spinBtn.textContent = `Крутить за ${payload.kazik.spin_cost}⭐`;
  }
}

function formatPrice(value) {
  const num = Number(value || 0);
  if (Number.isNaN(num)) return "0";
  return `${num}р`;
}

function updateWheelArc(chancePercent) {
  if (!els.upgradeWheelArc) return;
  const radius = 88;
  const circumference = 2 * Math.PI * radius;
  const pct = Math.max(0, Math.min(100, Number(chancePercent || 0)));
  const dash = (circumference * pct) / 100;
  els.upgradeWheelArc.style.strokeDasharray = `${dash} ${circumference}`;
}

function updateUpgradeStepUI() {
  const isTarget = upgradeState.step === "target";
  if (els.upgradeFilterGroup) {
    els.upgradeFilterGroup.classList.toggle("hidden", !isTarget);
  }
  if (els.upgradeBackStep) {
    els.upgradeBackStep.classList.toggle("hidden", !isTarget);
  }
  if (els.upgradeContinue) {
    els.upgradeContinue.classList.toggle("hidden", isTarget);
    els.upgradeContinue.disabled = upgradeState.selectedIds.length === 0;
  }
  if (els.upgradeStepTitle) {
    els.upgradeStepTitle.textContent = isTarget ? "Выбери цель апгрейда" : "Выбери сосиску";
  }
  if (els.upgradeStepHint) {
    els.upgradeStepHint.textContent = isTarget
      ? "Выбери награду"
      : `${upgradeState.selectedIds.length}/1`;
  }
}

function syncUpgradeSummary() {
  const count = upgradeState.selectedIds.length;
  if (els.upgradeTotal) {
    els.upgradeTotal.textContent = `Сумма: ${formatPrice(upgradeState.totalValue)}`;
  }
  if (els.upgradeChance) {
    const chance = upgradeState.selectedTarget
      ? `${upgradeState.selectedTarget.chance}%`
      : "0%";
    els.upgradeChance.textContent = `Шанс: ${chance}`;
  }
  updateWheelArc(upgradeState.selectedTarget ? upgradeState.selectedTarget.chance : 0);
  if (els.upgradeSpin) {
    els.upgradeSpin.disabled = !(
      upgradeState.step === "target" && upgradeState.selectedTarget
    );
  }
  updateUpgradeStepUI();
}

function setUpgradeStep(step) {
  upgradeState.step = step;
  if (step === "inventory") {
    upgradeState.selectedTarget = null;
  }
  if (step === "inventory") {
    renderUpgradeInventory();
  } else {
    renderUpgradeTargets();
  }
  syncUpgradeSummary();
}

function renderUpgradeInventory() {
  if (!els.upgradeGrid) return;
  els.upgradeGrid.innerHTML = "";
  if (!upgradeState.inventory.length) {
    const empty = document.createElement("div");
    empty.className = "card-item";
    empty.textContent = "Нет подходящих сосисок.";
    els.upgradeGrid.appendChild(empty);
    return;
  }
  upgradeState.inventory.forEach((item) => {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "card-item";
    if (upgradeState.selectedIds.includes(item.id)) {
      card.classList.add("selected");
    }
    card.innerHTML = `
      <img class="card-thumb" src="${item.media_url}" alt="${item.name}" />
      <div class="card-meta">
        <div class="card-title">${item.name}</div>
        <div class="card-info">${item.rarity_label}</div>
        <div class="card-info">${formatPrice(item.price)}</div>
      </div>
    `;
    card.addEventListener("click", () => toggleUpgradeItem(item.id));
    els.upgradeGrid.appendChild(card);
  });
}

function renderUpgradeTargets() {
  if (!els.upgradeGrid) return;
  els.upgradeGrid.innerHTML = "";
  if (!upgradeState.targets.length) {
    const empty = document.createElement("div");
    empty.className = "card-item";
    empty.textContent = "Нет доступных целей.";
    els.upgradeGrid.appendChild(empty);
    return;
  }
  upgradeState.targets.forEach((item) => {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "card-item";
    if (upgradeState.selectedTarget && upgradeState.selectedTarget.file === item.file) {
      card.classList.add("selected");
    }
    card.innerHTML = `
      <img class="card-thumb" src="${item.media_url}" alt="${item.name}" />
      <div class="card-meta">
        <div class="card-title">${item.name}</div>
        <div class="card-info">${item.rarity_label}</div>
        <div class="card-info">${formatPrice(item.price)}</div>
        <div class="card-chance">${item.chance}% шанс</div>
      </div>
    `;
    card.addEventListener("click", () => selectUpgradeTarget(item));
    els.upgradeGrid.appendChild(card);
  });
}

function getInitData() {
  if (tg && tg.initData) {
    return tg.initData;
  }
  const hash = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : "";
  if (hash) {
    const params = new URLSearchParams(hash);
    const data = params.get("tgWebAppData");
    if (data) {
      return data;
    }
  }
  return "";
}

async function api(path, options = {}) {
  const headers = options.headers || {};
  const initData = getInitData();
  if (initData) {
    headers["X-Init-Data"] = initData;
  }
  if (options.body && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(path, { ...options, headers });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw payload;
  }
  return payload;
}

async function loadUpgradeInventory() {
  try {
    const data = await api("/miniapp/api/upgrade/inventory");
    if (!data.ok) {
      showResult("Ошибка", "Не удалось получить инвентарь.");
      return;
    }
    upgradeState.inventory = data.items || [];
    const preserved = upgradeState.selectedIds.find((id) =>
      upgradeState.inventory.some((item) => item.id === id)
    );
    upgradeState.selectedIds = preserved ? [preserved] : [];
    upgradeState.selectedItem = preserved
      ? upgradeState.inventory.find((item) => item.id === preserved) || null
      : null;
    upgradeState.totalValue = upgradeState.selectedItem
      ? Number(upgradeState.selectedItem.price || 0)
      : 0;
    setUpgradeStep("inventory");
  } catch (error) {
    showResult("Ошибка", "Не удалось получить инвентарь.");
  }
}

async function loadUpgradeTargets() {
  if (!upgradeState.selectedIds.length) {
    upgradeState.targets = [];
    upgradeState.totalValue = 0;
    renderUpgradeTargets();
    syncUpgradeSummary();
    return;
  }
  try {
    const data = await api("/miniapp/api/upgrade/targets", {
      method: "POST",
      body: JSON.stringify({
        item_ids: upgradeState.selectedIds,
        filter: upgradeState.filter,
      }),
    });
    if (!data.ok) {
      showResult("Ошибка", "Не удалось получить цели.");
      return;
    }
    upgradeState.targets = data.targets || [];
    upgradeState.totalValue = data.total_value || 0;
    if (
      upgradeState.selectedTarget &&
      !upgradeState.targets.some(
        (item) => item.file === upgradeState.selectedTarget.file
      )
    ) {
      upgradeState.selectedTarget = null;
    }
    renderUpgradeTargets();
    syncUpgradeSummary();
  } catch (error) {
    showResult("Ошибка", "Не удалось получить цели.");
  }
}

function toggleUpgradeItem(itemId) {
  if (upgradeState.step !== "inventory") {
    upgradeState.step = "inventory";
    upgradeState.selectedTarget = null;
  }
  if (upgradeState.selectedIds[0] === itemId) {
    upgradeState.selectedIds = [];
    upgradeState.selectedItem = null;
    upgradeState.totalValue = 0;
  } else {
    upgradeState.selectedIds = [itemId];
    upgradeState.selectedItem =
      upgradeState.inventory.find((item) => item.id === itemId) || null;
    upgradeState.totalValue = upgradeState.selectedItem
      ? Number(upgradeState.selectedItem.price || 0)
      : 0;
  }
  renderUpgradeInventory();
  syncUpgradeSummary();
}

function selectUpgradeTarget(target) {
  upgradeState.selectedTarget = target;
  renderUpgradeTargets();
  syncUpgradeSummary();
}

function setUpgradeFilter(value, skipLoad = false) {
  upgradeState.filter = value;
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    btn.classList.toggle("active", Number(btn.dataset.filter) === value);
  });
  if (!skipLoad && upgradeState.step === "target") {
    loadUpgradeTargets();
  }
}

function pickAngle(chance, success) {
  const zone = Math.max(0, Math.min(100, chance)) * 3.6;
  const margin = Math.min(6, Math.max(2, zone * 0.08));
  if (success) {
    if (zone <= margin * 2) {
      return zone / 2;
    }
    return margin + Math.random() * (zone - margin * 2);
  }
  const start = zone + margin;
  const rest = 360 - start;
  if (rest <= 1) {
    return 359;
  }
  return start + Math.random() * rest;
}

function animateUpgradeWheel(chance, success) {
  if (!els.upgradePointer) {
    return Promise.resolve();
  }
  const extra = 720 + Math.random() * 540;
  const targetAngle = pickAngle(chance, success);
  const current = ((upgradeState.rotation % 360) + 360) % 360;
  const projected = (current + extra) % 360;
  const delta = (targetAngle - projected + 360) % 360;
  const finalRotation = upgradeState.rotation + extra + delta;
  els.upgradePointer.style.transition =
    "transform 2.6s cubic-bezier(0.2, 0.8, 0.2, 1)";
  els.upgradePointer.style.transform = `rotate(${finalRotation}deg)`;
  upgradeState.rotation = finalRotation;
  return new Promise((resolve) => setTimeout(resolve, 2700));
}

async function continueUpgrade() {
  if (!upgradeState.selectedIds.length) {
    showResult("Выбор", "Сначала выбери сосиску.");
    return;
  }
  upgradeState.step = "target";
  updateUpgradeStepUI();
  await loadUpgradeTargets();
}

function backUpgradeStep() {
  upgradeState.step = "inventory";
  upgradeState.selectedTarget = null;
  renderUpgradeInventory();
  syncUpgradeSummary();
}

async function spinUpgrade() {
  if (state.busy) return;
  if (!upgradeState.selectedIds.length) {
    showResult("Выбор", "Сначала выбери сосиску.");
    return;
  }
  if (upgradeState.step !== "target") {
    showResult("Выбор", "Теперь выбери цель апгрейда.");
    return;
  }
  if (!upgradeState.selectedTarget) {
    showResult("Выбор", "Теперь выбери цель апгрейда.");
    return;
  }
  setBusy(true);
  hideUpgradeResult();
  let response;
  try {
    response = await api("/miniapp/api/upgrade/roll", {
      method: "POST",
      body: JSON.stringify({
        item_ids: upgradeState.selectedIds,
        target_file: upgradeState.selectedTarget.file,
      }),
    });
  } catch (error) {
    showResult("Ошибка", "Не удалось запустить апгрейд.");
    setBusy(false);
    return;
  }

  if (!response.ok) {
    showResult("Ошибка", "Апгрейд не запустился.");
    setBusy(false);
    return;
  }

  const chance = Number(response.chance || 0);
  updateWheelArc(chance);
  if (els.upgradeChance) {
    els.upgradeChance.textContent = `Шанс: ${chance}%`;
  }
  await animateUpgradeWheel(chance, response.success);
  const reward = response.reward;
  if (response.success) {
    showModal(
      "Успех!",
      `<div class="result-highlight">${reward.name}</div>
       <div>${reward.rarity_label}</div>
       <img src="${reward.media_url}" alt="${reward.name}" />`
    );
  } else {
    showResult("Провал", "Не повезло. Сет сгорел.");
  }
  upgradeState.selectedIds = [];
  upgradeState.selectedTarget = null;
  upgradeState.selectedItem = null;
  upgradeState.step = "inventory";
  await loadUpgradeInventory();
  await loadState();
  setBusy(false);
}

function getUserId() {
  if (tg && tg.initDataUnsafe && tg.initDataUnsafe.user) {
    return Number(tg.initDataUnsafe.user.id);
  }
  return null;
}

const suitSymbols = {
  S: "♠",
  H: "♥",
  D: "♦",
  C: "♣",
};

const chessPieceSymbols = {
  wK: "♔",
  wQ: "♕",
  wR: "♖",
  wB: "♗",
  wN: "♘",
  wP: "♙",
  bK: "♚",
  bQ: "♛",
  bR: "♜",
  bB: "♝",
  bN: "♞",
  bP: "♟",
};

function chessPieceColor(piece) {
  if (!piece) return null;
  return String(piece)[0];
}

function chessPieceKind(piece) {
  if (!piece) return "";
  return String(piece)[1] || "";
}

function chessInBounds(row, col) {
  return row >= 0 && row < 8 && col >= 0 && col < 8;
}

function chessPathClear(board, fr, fc, tr, tc) {
  const dr = tr - fr;
  const dc = tc - fc;
  const stepR = dr === 0 ? 0 : dr > 0 ? 1 : -1;
  const stepC = dc === 0 ? 0 : dc > 0 ? 1 : -1;
  let r = fr + stepR;
  let c = fc + stepC;
  while (r !== tr || c !== tc) {
    if (board[r] && board[r][c]) {
      return false;
    }
    r += stepR;
    c += stepC;
  }
  return true;
}

function chessMovesFor(board, row, col, color) {
  const piece = board[row] ? board[row][col] : null;
  if (!piece || chessPieceColor(piece) !== color) {
    return [];
  }
  const kind = chessPieceKind(piece);
  const moves = [];
  const addMove = (r, c) => {
    if (!chessInBounds(r, c)) return;
    const target = board[r] ? board[r][c] : null;
    if (!target || chessPieceColor(target) !== color) {
      moves.push({ row: r, col: c });
    }
  };
  if (kind === "P") {
    const dir = color === "w" ? -1 : 1;
    const startRow = color === "w" ? 6 : 1;
    const forwardRow = row + dir;
    if (chessInBounds(forwardRow, col) && !(board[forwardRow] && board[forwardRow][col])) {
      moves.push({ row: forwardRow, col });
      const doubleRow = row + dir * 2;
      if (
        row === startRow &&
        chessInBounds(doubleRow, col) &&
        !(board[doubleRow] && board[doubleRow][col])
      ) {
        moves.push({ row: doubleRow, col });
      }
    }
    const captureCols = [col - 1, col + 1];
    captureCols.forEach((c) => {
      if (!chessInBounds(forwardRow, c)) return;
      const target = board[forwardRow] ? board[forwardRow][c] : null;
      if (target && chessPieceColor(target) !== color) {
        moves.push({ row: forwardRow, col: c });
      }
    });
    return moves;
  }
  if (kind === "N") {
    const offsets = [
      [1, 2],
      [2, 1],
      [2, -1],
      [1, -2],
      [-1, -2],
      [-2, -1],
      [-2, 1],
      [-1, 2],
    ];
    offsets.forEach(([dr, dc]) => addMove(row + dr, col + dc));
    return moves;
  }
  if (kind === "B" || kind === "R" || kind === "Q") {
    const directions = [];
    if (kind === "B" || kind === "Q") {
      directions.push([1, 1], [1, -1], [-1, 1], [-1, -1]);
    }
    if (kind === "R" || kind === "Q") {
      directions.push([1, 0], [-1, 0], [0, 1], [0, -1]);
    }
    directions.forEach(([dr, dc]) => {
      let r = row + dr;
      let c = col + dc;
      while (chessInBounds(r, c)) {
        const target = board[r] ? board[r][c] : null;
        if (!target) {
          moves.push({ row: r, col: c });
        } else {
          if (chessPieceColor(target) !== color) {
            moves.push({ row: r, col: c });
          }
          break;
        }
        r += dr;
        c += dc;
      }
    });
    return moves;
  }
  if (kind === "K") {
    for (let dr = -1; dr <= 1; dr += 1) {
      for (let dc = -1; dc <= 1; dc += 1) {
        if (dr === 0 && dc === 0) continue;
        addMove(row + dr, col + dc);
      }
    }
    return moves;
  }
  return moves;
}

function setChessMoves(moves) {
  chessState.availableMoves = moves;
  chessState.availableMoveSet = new Set(moves.map((move) => `${move.row},${move.col}`));
}

function buildCardElement(card, options = {}) {
  const el = document.createElement("button");
  el.type = "button";
  el.className = "cards-card";
  if (options.mini) {
    el.classList.add("mini");
  }
  if (options.back) {
    el.classList.add("back");
    el.disabled = true;
    return el;
  }
  const suit = String(card.suit || "").toUpperCase();
  const rank = String(card.rank || "");
  if (suit === "H" || suit === "D") {
    el.classList.add("red");
  }
  el.dataset.cardId = `${rank}${suit}`;
  const rankEl = document.createElement("div");
  rankEl.className = "rank";
  rankEl.textContent = rank;
  const suitEl = document.createElement("div");
  suitEl.className = "suit";
  suitEl.textContent = suitSymbols[suit] || suit;
  el.appendChild(rankEl);
  el.appendChild(suitEl);
  return el;
}

function cardKey(card) {
  if (!card) return "";
  return `${card.rank || ""}${card.suit || ""}`;
}

function formatTurnTimer(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function updateTurnTimerDisplay() {
  if (!els.cardsTurnTimer) return;
  if (!cardsState.turnEndsAt) {
    els.cardsTurnTimer.textContent = "";
    return;
  }
  const remaining = cardsState.turnEndsAt - Date.now();
  if (remaining <= 0) {
    els.cardsTurnTimer.textContent = `${cardsState.turnPrefix}0:00`;
    return;
  }
  els.cardsTurnTimer.textContent = `${cardsState.turnPrefix}${formatTurnTimer(remaining)}`;
}

function startTurnTimer(state) {
  if (!els.cardsTurnTimer) return;
  const started = Number(state.turn_started_at || 0);
  const timeout = Number(state.turn_timeout_sec || 0);
  if (!started || !timeout) {
    cardsState.turnEndsAt = null;
    cardsState.turnPrefix = "";
    updateTurnTimerDisplay();
    return;
  }
  const turnOwner = (state.players || []).find(
    (player) => Number(player.user_id) === Number(state.turn_owner_id)
  );
  const name = turnOwner ? turnOwner.name || turnOwner.user_id : "Игрок";
  cardsState.turnPrefix = `Ход: ${name} · `;
  cardsState.turnEndsAt = started * 1000 + timeout * 1000;
  updateTurnTimerDisplay();
  if (!cardsState.turnTimer) {
    cardsState.turnTimer = setInterval(updateTurnTimerDisplay, 1000);
  }
}

function stopCardsPolling() {
  if (cardsState.lobbyTimer) {
    clearInterval(cardsState.lobbyTimer);
    cardsState.lobbyTimer = null;
  }
  if (cardsState.gameTimer) {
    clearInterval(cardsState.gameTimer);
    cardsState.gameTimer = null;
  }
  if (cardsState.turnTimer) {
    clearInterval(cardsState.turnTimer);
    cardsState.turnTimer = null;
  }
}

async function loadCardsLobbies() {
  try {
    const response = await api("/miniapp/api/cards/lobbies");
    if (!response.ok) {
      if (els.cardsLobbies) {
        els.cardsLobbies.innerHTML = "<div class=\"cards-lobby\">Лобби не загрузились.</div>";
      }
      return;
    }
    cardsState.lobbies = response.lobbies || [];
    renderCardsLobbies();
    if (response.current_lobby && !cardsState.currentLobbyId) {
      openCardsLobby(response.current_lobby);
    }
  } catch (error) {
    if (els.cardsLobbies) {
      els.cardsLobbies.innerHTML = "<div class=\"cards-lobby\">Лобби не загрузились.</div>";
    }
  }
}

function renderCardsLobbies() {
  if (!els.cardsLobbies) return;
  els.cardsLobbies.innerHTML = "";
  if (!cardsState.lobbies.length) {
    els.cardsLobbies.innerHTML = "<div class=\"cards-lobby\">Пока нет лобби.</div>";
    return;
  }
  const modeLabels = {
    classic: "Обычные",
    podkidnoy: "Подкидной",
    transfer: "Переводной",
  };
  cardsState.lobbies.forEach((lobby) => {
    const card = document.createElement("div");
    card.className = "cards-lobby";
    const meta = document.createElement("div");
    meta.className = "cards-lobby-row";
    meta.innerHTML = `
      <span>Колода: ${lobby.deck_size}</span>
      <span>Режим: ${modeLabels[lobby.mode] || lobby.mode}</span>
      <span>Ставка: ${lobby.bet_amount} (${lobby.bet_type === "sausage" ? "сосиска" : "балик"})</span>
    `;
    const row = document.createElement("div");
    row.className = "cards-lobby-row";
    const players = document.createElement("span");
    players.textContent = `Игроки: ${lobby.players}/4`;
    const action = document.createElement("button");
    action.type = "button";
    action.className = "ghost-btn";
    if (lobby.joined) {
      action.textContent = "Открыть";
      action.addEventListener("click", () => openCardsLobby(lobby.lobby_id));
    } else if (lobby.status === "open") {
      action.textContent = "Войти";
      action.addEventListener("click", () => joinCardsLobby(lobby));
    } else {
      action.textContent = "Идет игра";
      action.disabled = true;
    }
    row.appendChild(players);
    row.appendChild(action);
    card.appendChild(meta);
    card.appendChild(row);
    els.cardsLobbies.appendChild(card);
  });
}

function startCardsLobbyPolling() {
  stopCardsPolling();
  loadCardsLobbies();
  cardsState.lobbyTimer = setInterval(loadCardsLobbies, 4000);
}

function cardsErrorMessage(code, fallback) {
  const mapping = {
    funds: "Недостаточно балика для ставки.",
    item: "Выбери сосиску для ставки.",
    item_price: "Сосиска дешевле ставки. Понизь сумму.",
    create_failed: "Не удалось создать лобби.",
    join_failed: "Не удалось войти в лобби.",
    full: "Лобби уже заполнено.",
    closed: "Лобби уже запущено.",
    not_found: "Лобби не найдено.",
  };
  return mapping[code] || fallback;
}

async function createCardsLobby() {
  const deck = Number(els.cardsDeck.value || 36);
  const mode = els.cardsMode.value || "classic";
  const betType = els.cardsBetType.value || "balance";
  const betAmount = Number(els.cardsBetAmount.value || 0);
  if (!betAmount || betAmount <= 0) {
    showResult("Ошибка", "Укажи сумму ставки.");
    return;
  }
  if (betType === "sausage") {
    openStakePicker(betAmount, async (item) => {
      await submitCardsCreate(deck, mode, betType, betAmount, item.id);
    });
    return;
  }
  await submitCardsCreate(deck, mode, betType, betAmount, "");
}

async function submitCardsCreate(deck, mode, betType, betAmount, itemId) {
  try {
    const response = await api("/miniapp/api/cards/create", {
      method: "POST",
      body: JSON.stringify({
        deck_size: deck,
        mode,
        bet_type: betType,
        bet_amount: betAmount,
        item_id: itemId,
      }),
    });
    if (!response.ok) {
      showResult("Ошибка", cardsErrorMessage(response.error, "Не удалось создать лобби."));
      return;
    }
    openCardsLobby(response.lobby_id);
  } catch (error) {
    showResult("Ошибка", "Не удалось создать лобби.");
  }
}

async function joinCardsLobby(lobby) {
  if (lobby.bet_type === "sausage") {
    openStakePicker(lobby.bet_amount, async (item) => {
      await submitCardsJoin(lobby.lobby_id, item.id);
    });
    return;
  }
  await submitCardsJoin(lobby.lobby_id, "");
}

async function submitCardsJoin(lobbyId, itemId) {
  try {
    const response = await api("/miniapp/api/cards/join", {
      method: "POST",
      body: JSON.stringify({ lobby_id: lobbyId, item_id: itemId }),
    });
    if (!response.ok) {
      showResult("Ошибка", cardsErrorMessage(response.error, "Не получилось войти."));
      return;
    }
    openCardsLobby(lobbyId);
  } catch (error) {
    showResult("Ошибка", "Не получилось войти.");
  }
}

async function openCardsLobby(lobbyId) {
  cardsState.currentLobbyId = lobbyId;
  cardsState.selectedAttackIndex = null;
  cardsState.prevHandIds = [];
  cardsState.prevDeckCount = null;
  cardsState.prevTable = [];
  cardsState.finishedShown = false;
  setScreen(screens.cardsGame);
  stopCardsPolling();
  await loadCardsState();
  cardsState.gameTimer = setInterval(loadCardsState, 2000);
}

async function loadCardsState() {
  if (!cardsState.currentLobbyId) return;
  try {
    const response = await api(
      `/miniapp/api/cards/state?lobby_id=${encodeURIComponent(cardsState.currentLobbyId)}`
    );
    if (!response.ok) {
      showResult("Ошибка", cardsErrorMessage(response.error, "Лобби не загрузилось."));
      return;
    }
    cardsState.currentState = response.state;
    renderCardsState(response.state);
  } catch (error) {
    showResult("Ошибка", "Лобби не загрузилось.");
  }
}

function renderCardsState(state) {
  if (!state) return;
  if (cardsState.selectedAttackIndex !== null) {
    const table = state.table || [];
    if (cardsState.selectedAttackIndex >= table.length) {
      cardsState.selectedAttackIndex = null;
    }
  }
  const myId = getUserId();
  if (els.cardsLobbyMeta) {
    const statusText =
      state.status === "open"
        ? "Ожидание игроков"
        : state.status === "finished"
        ? "Игра завершена"
        : "Игра идет";
    els.cardsLobbyMeta.textContent = statusText;
  }
  if (els.cardsGameMeta) {
    if (state.status === "open") {
      els.cardsGameMeta.innerHTML = "";
    } else {
      const trump = state.trump
        ? `${state.trump.rank}${suitSymbols[state.trump.suit] || state.trump.suit}`
        : "-";
      els.cardsGameMeta.innerHTML = `
        <span>Колода: ${state.deck_size}</span>
        <span>Режим: ${state.mode}</span>
        <span>Ставка: ${state.bet_amount}</span>
        <span>Козырь: ${trump}</span>
      `;
    }
  }
  renderCardsPlayers(state);
  renderCardsTable(state);
  renderCardsHand(state);
  renderCardsDeck(state);
  startTurnTimer(state);
  if (state.status === "finished") {
    if (!cardsState.finishedShown) {
      const winner = (state.players || []).find(
        (player) => Number(player.user_id) === Number(state.winner_id)
      );
      const title = "Игра завершена";
      const body = winner
        ? `<div class="result-highlight">Победитель: ${winner.name || winner.user_id}</div>`
        : "Победитель не определён.";
      showModal(title, body);
      cardsState.finishedShown = true;
    }
    if (els.cardsStart) {
      els.cardsStart.style.display = "none";
    }
    if (els.cardsTake) {
      els.cardsTake.style.display = "none";
    }
    if (els.cardsPass) {
      els.cardsPass.style.display = "none";
    }
    stopCardsPolling();
    return;
  }
  const isOwner = myId && Number(state.owner_id) === Number(myId);
  if (els.cardsStart) {
    els.cardsStart.style.display = state.status === "open" && isOwner ? "inline-flex" : "none";
  }
  if (els.cardsTake) {
    const isDef = myId && Number(state.defender_id) === Number(myId);
    const allow = state.status === "active" && isDef && state.phase === "defend";
    els.cardsTake.style.display = allow ? "inline-flex" : "none";
  }
  if (els.cardsPass) {
    const isDef = myId && Number(state.defender_id) === Number(myId);
    const allow = state.status === "active" && !isDef && ["throw", "throw_take"].includes(state.phase);
    els.cardsPass.style.display = allow ? "inline-flex" : "none";
  }
  const activeGame = state.status === "active";
  if (els.cardsLeave) {
    els.cardsLeave.style.display = activeGame ? "none" : "inline-flex";
  }
  if (els.backCardsGame) {
    els.backCardsGame.style.display = activeGame ? "none" : "inline-flex";
  }
}

function renderCardsPlayers(state) {
  if (!els.cardsPlayers) return;
  els.cardsPlayers.innerHTML = "";
  const attackerId = state.attacker_id;
  const defenderId = state.defender_id;
  const turnOwner = state.turn_owner_id;
  (state.players || []).forEach((player) => {
    const row = document.createElement("div");
    row.className = "cards-player";
    if (player.user_id === attackerId || player.user_id === defenderId) {
      row.classList.add("active");
    }
    if (player.user_id === turnOwner) {
      row.classList.add("turn");
    }
    const role =
      player.user_id === attackerId
        ? "Атакует"
        : player.user_id === defenderId
        ? "Защищается"
        : "В ожидании";
    const initials = String(player.name || player.user_id || "?")
      .trim()
      .slice(0, 1)
      .toUpperCase();
    row.innerHTML = `
      <div class="player-avatar">${initials}</div>
      <div class="player-meta">
        <div class="player-name">${player.name || player.user_id}</div>
        <div class="player-role">${role}</div>
      </div>
      <div class="player-hand"></div>
    `;
    const handWrap = row.querySelector(".player-hand");
    const count = Number(player.hand_count || 0);
    const visible = Math.min(count, 5);
    for (let i = 0; i < visible; i += 1) {
      const back = document.createElement("div");
      back.className = "hand-back";
      handWrap.appendChild(back);
    }
    const countEl = document.createElement("span");
    countEl.className = "hand-count";
    countEl.textContent = `${count}`;
    handWrap.appendChild(countEl);
    els.cardsPlayers.appendChild(row);
  });
}

function renderCardsDeck(state) {
  if (els.cardsDeckCount) {
    els.cardsDeckCount.textContent = String(state.deck_count || 0);
  }
  if (els.cardsTrump) {
    els.cardsTrump.innerHTML = "";
    if (state.trump) {
      const cardEl = buildCardElement(state.trump, { mini: true });
      els.cardsTrump.appendChild(cardEl);
    }
  }
  if (els.cardsDeckStack) {
    const deckCount = Number(state.deck_count || 0);
    const prev = cardsState.prevDeckCount;
    if (prev !== null && deckCount < prev) {
      els.cardsDeckStack.classList.remove("draw");
      void els.cardsDeckStack.offsetWidth;
      els.cardsDeckStack.classList.add("draw");
    }
    cardsState.prevDeckCount = deckCount;
  }
}

function renderCardsTable(state) {
  if (!els.cardsTable) return;
  els.cardsTable.innerHTML = "";
  const myId = getUserId();
  const prevTable = cardsState.prevTable || [];
  (state.table || []).forEach((entry, index) => {
    const pair = document.createElement("div");
    pair.className = "cards-pair";
    const attackId = entry.attack ? cardKey(entry.attack) : "";
    const defenseId = entry.defense ? cardKey(entry.defense) : "";
    const prevEntry = prevTable[index] || {};
    const isNewAttack = attackId && attackId !== prevEntry.attack;
    const isNewDefense = defenseId && defenseId !== prevEntry.defense;

    const attackCard = buildCardElement(entry.attack || {}, { back: !entry.attack });
    attackCard.classList.add("attack-card");
    if (isNewAttack) {
      attackCard.classList.add("attack-anim");
    }
    if (myId && Number(state.defender_id) === Number(myId) && entry.attack) {
      attackCard.addEventListener("click", () => {
        cardsState.selectedAttackIndex = index;
        renderCardsTable(state);
      });
      if (cardsState.selectedAttackIndex === index) {
        attackCard.classList.add("selected");
      }
    }
    pair.appendChild(attackCard);
    if (entry.defense) {
      const defenseCard = buildCardElement(entry.defense);
      defenseCard.classList.add("defense-card");
      if (isNewDefense) {
        defenseCard.classList.add("defense-anim");
      }
      pair.appendChild(defenseCard);
      if (entry.attack) {
        const peek = document.createElement("div");
        peek.className = "attack-peek";
        peek.textContent = entry.attack.rank || "";
        pair.appendChild(peek);
      }
    }
    els.cardsTable.appendChild(pair);
  });
  cardsState.prevTable = (state.table || []).map((entry) => ({
    attack: entry.attack ? cardKey(entry.attack) : "",
    defense: entry.defense ? cardKey(entry.defense) : "",
  }));
}

function renderCardsHand(state) {
  if (!els.cardsHand) return;
  els.cardsHand.innerHTML = "";
  const myId = getUserId();
  const me = (state.players || []).find((player) => Number(player.user_id) === Number(myId));
  const hand = me?.hand || [];
  const currentIds = hand.map((card) => `${card.rank}${card.suit}`);
  const newIds = new Set(currentIds.filter((id) => !cardsState.prevHandIds.includes(id)));
  hand.forEach((card) => {
    const cardEl = buildCardElement(card);
    if (newIds.has(`${card.rank}${card.suit}`)) {
      cardEl.classList.add("deal");
    }
    cardEl.addEventListener("click", () => handleCardAction(state, card));
    els.cardsHand.appendChild(cardEl);
  });
  cardsState.prevHandIds = currentIds;
}

async function handleCardAction(state, card) {
  if (!cardsState.currentLobbyId) return;
  const myId = getUserId();
  if (!myId) return;
  if (state.status !== "active") return;
  const cardId = `${card.rank}${card.suit}`;
  if (state.phase === "attack" && Number(state.attacker_id) === Number(myId)) {
    await sendCardsAction("attack", { card_id: cardId });
    return;
  }
  if (state.phase === "defend" && Number(state.defender_id) === Number(myId)) {
    let targetIndex = cardsState.selectedAttackIndex;
    if (targetIndex === null || targetIndex === undefined) {
      targetIndex = (state.table || []).findIndex((entry) => !entry.defense);
    }
    await sendCardsAction("defend", { card_id: cardId, target_index: targetIndex });
    cardsState.selectedAttackIndex = null;
    return;
  }
  if (["throw", "throw_take"].includes(state.phase) && Number(state.defender_id) !== Number(myId)) {
    await sendCardsAction("throw", { card_id: cardId });
  }
}

async function sendCardsAction(action, extra = {}) {
  if (!cardsState.currentLobbyId) return;
  try {
    await api("/miniapp/api/cards/action", {
      method: "POST",
      body: JSON.stringify({
        lobby_id: cardsState.currentLobbyId,
        action,
        ...extra,
      }),
    });
    await loadCardsState();
  } catch (error) {
    showResult("Ошибка", "Ход не прошел.");
  }
}

function openStakePicker(minPrice, onSelect) {
  if (!els.cardsStakeModal || !els.cardsStakeList) return;
  cardsState.pendingLobbyId = null;
  els.cardsStakeList.innerHTML = "";
  els.cardsStakeModal.classList.remove("hidden");
  api(`/miniapp/api/cards/inventory?min_price=${minPrice}`)
    .then((response) => {
      if (!response.ok || !response.items?.length) {
        const message = `Нет сосисок от ${formatPrice(minPrice)}. Понизь ставку.`;
        els.cardsStakeList.innerHTML = `<div class="card-item">${message}</div>`;
        return;
      }
      response.items.forEach((item) => {
        const card = document.createElement("button");
        card.type = "button";
        card.className = "card-item";
        card.innerHTML = `
          <img class="card-thumb" src="${item.media_url}" alt="${item.name}" />
          <div class="card-meta">
            <div class="card-title">${item.name}</div>
            <div class="card-info">${item.rarity_label}</div>
            <div class="card-info">${formatPrice(item.price)}</div>
          </div>
        `;
        card.addEventListener("click", () => {
          closeStakePicker();
          onSelect(item);
        });
        els.cardsStakeList.appendChild(card);
      });
    })
    .catch(() => {
      els.cardsStakeList.innerHTML = "<div class=\"card-item\">Нет данных.</div>";
    });
}

function closeStakePicker() {
  if (els.cardsStakeModal) {
    els.cardsStakeModal.classList.add("hidden");
  }
}

function renderChessLobbies() {
  if (!els.chessLobbies) return;
  els.chessLobbies.innerHTML = "";
  if (!chessState.lobbies.length) {
    els.chessLobbies.innerHTML = "<div class=\"chess-lobby\">Пока нет лобби.</div>";
    return;
  }
  chessState.lobbies.forEach((lobby) => {
    const card = document.createElement("div");
    card.className = "chess-lobby";
    const meta = document.createElement("div");
    meta.className = "chess-lobby-row";
    meta.innerHTML = `
      <span>Ставка: ${lobby.bet_amount} (${lobby.bet_type === "sausage" ? "сосиска" : "балик"})</span>
      <span>Игроки: ${lobby.players}/2</span>
    `;
    const row = document.createElement("div");
    row.className = "chess-lobby-row";
    const owner = document.createElement("span");
    owner.textContent = `Хозяин: ${lobby.owner_id}`;
    const action = document.createElement("button");
    action.type = "button";
    action.className = "ghost-btn";
    if (lobby.joined) {
      action.textContent = "Открыть";
      action.addEventListener("click", () => openChessLobby(lobby.lobby_id));
    } else if (lobby.status === "open") {
      action.textContent = "Войти";
      action.addEventListener("click", () => joinChessLobby(lobby));
    } else {
      action.textContent = "Идет игра";
      action.disabled = true;
    }
    row.appendChild(owner);
    row.appendChild(action);
    card.appendChild(meta);
    card.appendChild(row);
    els.chessLobbies.appendChild(card);
  });
}

function startChessLobbyPolling() {
  stopChessPolling();
  loadChessLobbies();
  chessState.lobbyTimer = setInterval(loadChessLobbies, 4000);
}

function stopChessPolling() {
  if (chessState.lobbyTimer) {
    clearInterval(chessState.lobbyTimer);
    chessState.lobbyTimer = null;
  }
  if (chessState.gameTimer) {
    clearInterval(chessState.gameTimer);
    chessState.gameTimer = null;
  }
  if (chessState.turnTimer) {
    clearInterval(chessState.turnTimer);
    chessState.turnTimer = null;
  }
}

function chessErrorMessage(code, fallback) {
  const mapping = {
    funds: "Недостаточно балика для ставки.",
    item: "Выбери сосиску для ставки.",
    item_price: "Сосиска дешевле ставки. Понизь сумму.",
    create_failed: "Не удалось создать лобби.",
    join_failed: "Не удалось войти в лобби.",
    full: "Лобби уже заполнено.",
    closed: "Лобби уже запущено.",
    not_found: "Лобби не найдено.",
    not_turn: "Сейчас не твой ход.",
    invalid_move: "Этот ход невозможен.",
    game_closed: "Игра уже завершена.",
    coords: "Некорректные координаты.",
    action: "Действие не распознано.",
  };
  return mapping[code] || fallback;
}

async function createChessLobby() {
  const betType = els.chessBetType?.value || "balance";
  const betAmount = Number(els.chessBetAmount?.value || 0);
  if (!betAmount || betAmount <= 0) {
    showResult("Ошибка", "Укажи сумму ставки.");
    return;
  }
  if (betType === "sausage") {
    openStakePicker(betAmount, async (item) => {
      await submitChessCreate(betType, betAmount, item.id);
    });
    return;
  }
  await submitChessCreate(betType, betAmount, "");
}

async function submitChessCreate(betType, betAmount, itemId) {
  try {
    const response = await api("/miniapp/api/chess/create", {
      method: "POST",
      body: JSON.stringify({
        bet_type: betType,
        bet_amount: betAmount,
        item_id: itemId,
      }),
    });
    if (!response.ok) {
      showResult("Ошибка", chessErrorMessage(response.error, "Не удалось создать лобби."));
      return;
    }
    openChessLobby(response.lobby_id);
  } catch (error) {
    showResult("Ошибка", "Не удалось создать лобби.");
  }
}

async function joinChessLobby(lobby) {
  if (lobby.bet_type === "sausage") {
    openStakePicker(lobby.bet_amount, async (item) => {
      await submitChessJoin(lobby.lobby_id, item.id);
    });
    return;
  }
  await submitChessJoin(lobby.lobby_id, "");
}

async function submitChessJoin(lobbyId, itemId) {
  try {
    const response = await api("/miniapp/api/chess/join", {
      method: "POST",
      body: JSON.stringify({ lobby_id: lobbyId, item_id: itemId }),
    });
    if (!response.ok) {
      showResult("Ошибка", chessErrorMessage(response.error, "Не получилось войти."));
      return;
    }
    openChessLobby(lobbyId);
  } catch (error) {
    showResult("Ошибка", "Не получилось войти.");
  }
}

async function openChessLobby(lobbyId) {
  chessState.currentLobbyId = lobbyId;
  chessState.selectedCell = null;
  setChessMoves([]);
  chessState.finishedShown = false;
  setScreen(screens.chessGame);
  stopChessPolling();
  await loadChessState();
  chessState.gameTimer = setInterval(loadChessState, 2000);
}

async function loadChessLobbies() {
  try {
    const response = await api("/miniapp/api/chess/lobbies");
    if (!response.ok) {
      if (els.chessLobbies) {
        els.chessLobbies.innerHTML = "<div class=\"chess-lobby\">Лобби не загрузились.</div>";
      }
      return;
    }
    chessState.lobbies = response.lobbies || [];
    renderChessLobbies();
    if (response.current_lobby && !chessState.currentLobbyId) {
      openChessLobby(response.current_lobby);
    }
  } catch (error) {
    if (els.chessLobbies) {
      els.chessLobbies.innerHTML = "<div class=\"chess-lobby\">Лобби не загрузились.</div>";
    }
  }
}

async function loadChessState() {
  if (!chessState.currentLobbyId) return;
  try {
    const response = await api(
      `/miniapp/api/chess/state?lobby_id=${encodeURIComponent(chessState.currentLobbyId)}`
    );
    if (!response.ok) {
      showResult("Ошибка", chessErrorMessage(response.error, "Лобби не загрузилось."));
      return;
    }
    chessState.currentState = response.state;
    renderChessState(response.state);
  } catch (error) {
    showResult("Ошибка", "Лобби не загрузилось.");
  }
}

function renderChessState(state) {
  if (!state) return;
  syncChessSelection(state);
  if (els.chessLobbyMeta) {
    const statusText =
      state.status === "open"
        ? "Ожидание соперника"
        : state.status === "finished"
        ? "Игра завершена"
        : "Идет игра";
    els.chessLobbyMeta.textContent = statusText;
  }
  if (els.chessGameMeta) {
    if (state.status === "open") {
      els.chessGameMeta.innerHTML = "";
    } else {
      const turnLabel = state.turn === "black" ? "Черные" : "Белые";
      els.chessGameMeta.innerHTML = `
        <span>Ставка: ${state.bet_amount}</span>
        <span>Ход: ${turnLabel}</span>
      `;
    }
  }
  renderChessPlayers(state);
  renderChessBoard(state);
  startChessTurnTimer(state);

  const activeGame = state.status === "active";
  if (els.chessLeave) {
    els.chessLeave.style.display = activeGame ? "none" : "inline-flex";
  }
  if (els.backChessGame) {
    els.backChessGame.style.display = activeGame ? "none" : "inline-flex";
  }
  if (state.status === "finished") {
    if (!chessState.finishedShown) {
      const winner = (state.players || []).find(
        (player) => Number(player.user_id) === Number(state.winner_id)
      );
      const title = "Игра завершена";
      const body = winner
        ? `<div class="result-highlight">Победитель: ${winner.name || winner.user_id}</div>`
        : "Победитель не определён.";
      showModal(title, body);
      chessState.finishedShown = true;
    }
    stopChessPolling();
  }
}

function syncChessSelection(state) {
  const myId = getUserId();
  if (!myId || state.status !== "active" || Number(state.turn_owner_id) !== Number(myId)) {
    chessState.selectedCell = null;
    setChessMoves([]);
    return;
  }
  if (!chessState.selectedCell) {
    setChessMoves([]);
    return;
  }
  const board = state.board || [];
  const me = (state.players || []).find(
    (player) => Number(player.user_id) === Number(myId)
  );
  const myColor = me && me.color === "black" ? "b" : "w";
  const { row, col } = chessState.selectedCell;
  const piece = board[row] ? board[row][col] : null;
  if (!piece || chessPieceColor(piece) !== myColor) {
    chessState.selectedCell = null;
    setChessMoves([]);
    return;
  }
  setChessMoves(chessMovesFor(board, row, col, myColor));
}

function renderChessPlayers(state) {
  if (!els.chessPlayers) return;
  els.chessPlayers.innerHTML = "";
  (state.players || []).forEach((player) => {
    const item = document.createElement("div");
    item.className = "chess-player";
    if (player.color === "black") {
      item.classList.add("black");
    }
    if (Number(player.user_id) === Number(state.turn_owner_id)) {
      item.classList.add("turn");
    }
    const colorLabel = player.color
      ? player.color === "black"
        ? "Черные"
        : "Белые"
      : "Игрок";
    item.innerHTML = `
      <span class="player-chip"></span>
      <span>${player.name || player.user_id}</span>
      <span>${colorLabel}</span>
    `;
    els.chessPlayers.appendChild(item);
  });
}

function renderChessBoard(state) {
  if (!els.chessBoard) return;
  els.chessBoard.innerHTML = "";
  const board = state.board || [];
  const moveSet = chessState.availableMoveSet || new Set();
  for (let row = 0; row < 8; row += 1) {
    for (let col = 0; col < 8; col += 1) {
      const cell = document.createElement("button");
      cell.type = "button";
      cell.className = "chess-cell";
      cell.classList.add((row + col) % 2 === 0 ? "light" : "dark");
      if (
        chessState.selectedCell &&
        chessState.selectedCell.row === row &&
        chessState.selectedCell.col === col
      ) {
        cell.classList.add("selected");
      }
      if (moveSet.has(`${row},${col}`)) {
        cell.classList.add("move");
      }
      const piece = board[row] ? board[row][col] : null;
      if (piece) {
        cell.appendChild(buildChessPiece(piece));
      }
      cell.addEventListener("click", () => handleChessCellClick(row, col));
      els.chessBoard.appendChild(cell);
    }
  }
}

function buildChessPiece(piece) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 100 100");
  svg.classList.add("chess-piece");
  if (String(piece).startsWith("w")) {
    svg.classList.add("white");
  } else {
    svg.classList.add("black");
  }
  const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
  text.setAttribute("x", "50");
  text.setAttribute("y", "56");
  text.textContent = chessPieceSymbols[piece] || piece;
  svg.appendChild(text);
  return svg;
}

function handleChessCellClick(row, col) {
  const state = chessState.currentState;
  if (!state || state.status !== "active") return;
  const myId = getUserId();
  if (!myId || Number(state.turn_owner_id) !== Number(myId)) {
    return;
  }
  const me = (state.players || []).find(
    (player) => Number(player.user_id) === Number(myId)
  );
  const myColor = me && me.color === "black" ? "b" : "w";
  const board = state.board || [];
  const piece = board[row] ? board[row][col] : null;
  if (!chessState.selectedCell) {
    if (!piece || !String(piece).startsWith(myColor)) {
      return;
    }
    chessState.selectedCell = { row, col };
    setChessMoves(chessMovesFor(board, row, col, myColor));
    renderChessBoard(state);
    return;
  }
  if (chessState.selectedCell.row === row && chessState.selectedCell.col === col) {
    chessState.selectedCell = null;
    setChessMoves([]);
    renderChessBoard(state);
    return;
  }
  if (piece && String(piece).startsWith(myColor)) {
    chessState.selectedCell = { row, col };
    setChessMoves(chessMovesFor(board, row, col, myColor));
    renderChessBoard(state);
    return;
  }
  if (!chessState.availableMoveSet.has(`${row},${col}`)) {
    return;
  }
  sendChessAction("move", {
    from_row: chessState.selectedCell.row,
    from_col: chessState.selectedCell.col,
    to_row: row,
    to_col: col,
  });
  chessState.selectedCell = null;
  setChessMoves([]);
}

async function sendChessAction(action, extra = {}) {
  if (!chessState.currentLobbyId) return;
  try {
    const response = await api("/miniapp/api/chess/action", {
      method: "POST",
      body: JSON.stringify({
        lobby_id: chessState.currentLobbyId,
        action,
        ...extra,
      }),
    });
    if (!response.ok) {
      showResult("Ошибка", chessErrorMessage(response.error, "Ход не прошел."));
      return;
    }
    await loadChessState();
  } catch (error) {
    showResult("Ошибка", "Ход не прошел.");
  }
}

function updateChessTurnTimerDisplay() {
  if (!els.chessTurnTimer) return;
  if (!chessState.turnEndsAt) {
    els.chessTurnTimer.textContent = "";
    return;
  }
  const remaining = chessState.turnEndsAt - Date.now();
  if (remaining <= 0) {
    els.chessTurnTimer.textContent = `${chessState.turnPrefix}0:00`;
    return;
  }
  els.chessTurnTimer.textContent = `${chessState.turnPrefix}${formatTurnTimer(remaining)}`;
}

function startChessTurnTimer(state) {
  if (!els.chessTurnTimer) return;
  const started = Number(state.turn_started_at || 0);
  const timeout = Number(state.turn_timeout_sec || 0);
  if (!started || !timeout) {
    chessState.turnEndsAt = null;
    chessState.turnPrefix = "";
    updateChessTurnTimerDisplay();
    if (chessState.turnTimer) {
      clearInterval(chessState.turnTimer);
      chessState.turnTimer = null;
    }
    return;
  }
  const turnOwner = (state.players || []).find(
    (player) => Number(player.user_id) === Number(state.turn_owner_id)
  );
  const name = turnOwner ? turnOwner.name || turnOwner.user_id : "Игрок";
  chessState.turnPrefix = `Ход: ${name} · `;
  chessState.turnEndsAt = started * 1000 + timeout * 1000;
  updateChessTurnTimerDisplay();
  if (!chessState.turnTimer) {
    chessState.turnTimer = setInterval(updateChessTurnTimerDisplay, 1000);
  }
}

function randomDigit() {
  const digits = state.data?.state?.kazik?.digits;
  if (digits && digits.length) {
    return digits[Math.floor(Math.random() * digits.length)];
  }
  return Math.floor(Math.random() * 3) + 1;
}

function animateReels(targetDigits) {
  const reels = Array.from(document.querySelectorAll(".reel"));
  const durations = [600, 800, 1000];
  return new Promise((resolve) => {
    let finished = 0;
    reels.forEach((reel, index) => {
      reel.classList.add("spin");
      const interval = setInterval(() => {
        reel.textContent = randomDigit();
      }, 80);
      setTimeout(() => {
        clearInterval(interval);
        reel.textContent = targetDigits[index] || randomDigit();
        reel.classList.remove("spin");
        finished += 1;
        if (finished === reels.length) {
          resolve();
        }
      }, durations[index]);
    });
  });
}

async function loadState() {
  try {
    if (!getInitData()) {
      showResult(
        "Нет доступа",
        "Открой mini app через кнопку в боте и проверь Web App домен в BotFather."
      );
      return;
    }
    const data = await api("/miniapp/api/state");
    if (!data.ok) {
      showResult(
        "Нет доступа",
        "Telegram не передал initData. Проверь домен в BotFather и открывай через кнопку."
      );
      return;
    }
    updateState(data);
  } catch (error) {
    showResult("Ошибка", "Не удалось получить данные казика.");
  }
}

async function spin() {
  if (state.busy) return;
  setBusy(true);
  hideResult();
  els.spinHint.textContent = "Крутим...";
  let response;
  try {
    response = await api("/miniapp/api/spin", {
      method: "POST",
      body: JSON.stringify({}),
    });
  } catch (error) {
    showResult("Ошибка", "Не получилось запустить крутку.");
    setBusy(false);
    return;
  }

  if (!response.ok && response.error === "no_stars") {
    showResult("Нужны звезды", "Пополнить баланс звезд в боте.");
    openStarsMenu();
    setBusy(false);
    return;
  }
  if (!response.ok) {
    showResult("Ошибка", "Не получилось запустить крутку.");
    setBusy(false);
    return;
  }

  await animateReels(response.digits || [randomDigit(), randomDigit(), randomDigit()]);
  updateState(response);

  if (response.win && response.reward) {
    if (response.reward.status === "ok") {
      const mediaHtml = response.reward.media_type === "video"
        ? `<video src="${response.reward.media_url}" controls playsinline></video>`
        : `<img src="${response.reward.media_url}" alt="${response.reward.name}" />`;
      showModal(
        "Выигрыш!",
        `<div class="result-highlight">${response.reward.name}</div>
         <div>${response.reward.rarity_label}</div>
         ${mediaHtml}`
      );
    } else if (response.reward.status === "save_failed") {
      showModal("Выигрыш", "Не удалось сохранить карту, напиши /support.");
    } else {
      showModal("Выигрыш", "Карточек нет, но выпадение было.");
    }
  } else {
    showResult("Не повезло", "Попробуй ещё раз.");
  }

  setBusy(false);
}

async function buySpins(spins, cost) {
  if (state.busy) return;
  setBusy(true);
  hideResult();
  try {
    const response = await api("/miniapp/api/buy", {
      method: "POST",
      body: JSON.stringify({ spins, cost }),
    });
    if (!response.ok && response.error === "no_stars") {
      showResult("Нужны звезды", "Пополнить баланс звезд в боте.");
      openStarsMenu();
      return;
    }
    if (!response.ok) {
      showResult("Ошибка", "Не получилось купить спины.");
      return;
    }
    updateState(response);
    showResult("Покупка", response.message || "Спины добавлены.");
  } catch (error) {
    showResult("Ошибка", "Не получилось купить спины.");
  } finally {
    setBusy(false);
  }
}

function bindEvents() {
  document.getElementById("openKazik").addEventListener("click", () => {
    setScreen(screens.kazik);
  });
  document.getElementById("openUpgrade").addEventListener("click", () => {
    setScreen(screens.upgrade);
    loadUpgradeInventory();
  });
  if (els.openCards) {
    els.openCards.addEventListener("click", () => {
      setScreen(screens.cards);
      startCardsLobbyPolling();
    });
  }
  if (els.openChess) {
    els.openChess.addEventListener("click", () => {
      setScreen(screens.chess);
      startChessLobbyPolling();
    });
  }
  document.getElementById("backHome").addEventListener("click", () => {
    setScreen(screens.home);
  });
  document.getElementById("backUpgrade").addEventListener("click", () => {
    setScreen(screens.home);
  });
  document.getElementById("backCards").addEventListener("click", () => {
    stopCardsPolling();
    setScreen(screens.home);
  });
  if (els.backChess) {
    els.backChess.addEventListener("click", () => {
      stopChessPolling();
      setScreen(screens.home);
    });
  }
  document.getElementById("backCardsGame").addEventListener("click", () => {
    if (cardsState.currentState && cardsState.currentState.status === "active") {
      showResult("Игра", "Нельзя выйти во время матча.");
      return;
    }
    stopCardsPolling();
    setScreen(screens.cards);
    startCardsLobbyPolling();
  });
  if (els.backChessGame) {
    els.backChessGame.addEventListener("click", () => {
      if (chessState.currentState && chessState.currentState.status === "active") {
        showResult("Игра", "Нельзя выйти во время матча.");
        return;
      }
      stopChessPolling();
      setScreen(screens.chess);
      startChessLobbyPolling();
    });
  }
  document.getElementById("starsTopup").addEventListener("click", () => {
    showResult("Пополнение", "Открываю меню звёзд в боте.");
    openStarsMenu();
  });
  document.getElementById("closeResult").addEventListener("click", hideResult);
  els.spinBtn.addEventListener("click", spin);
  els.upgradeSpin.addEventListener("click", spinUpgrade);
  if (els.upgradeContinue) {
    els.upgradeContinue.addEventListener("click", continueUpgrade);
  }
  if (els.upgradeBackStep) {
    els.upgradeBackStep.addEventListener("click", backUpgradeStep);
  }
  if (els.cardsCreate) {
    els.cardsCreate.addEventListener("click", createCardsLobby);
  }
  if (els.chessCreate) {
    els.chessCreate.addEventListener("click", createChessLobby);
  }
  if (els.cardsStart) {
    els.cardsStart.addEventListener("click", () => {
      if (!cardsState.currentLobbyId) return;
      api("/miniapp/api/cards/start", {
        method: "POST",
        body: JSON.stringify({ lobby_id: cardsState.currentLobbyId }),
      }).then(loadCardsState);
    });
  }
  if (els.cardsTake) {
    els.cardsTake.addEventListener("click", () => sendCardsAction("take"));
  }
  if (els.cardsPass) {
    els.cardsPass.addEventListener("click", () => sendCardsAction("pass"));
  }
  if (els.cardsLeave) {
    els.cardsLeave.addEventListener("click", async () => {
      if (!cardsState.currentLobbyId) return;
      const response = await api("/miniapp/api/cards/leave", {
        method: "POST",
        body: JSON.stringify({ lobby_id: cardsState.currentLobbyId }),
      }).catch(() => ({ ok: false, error: "leave" }));
      if (!response.ok) {
        showResult("Игра", "Нельзя выйти во время матча.");
        return;
      }
      cardsState.currentLobbyId = null;
      cardsState.prevHandIds = [];
      cardsState.prevDeckCount = null;
      cardsState.prevTable = [];
      cardsState.finishedShown = false;
      stopCardsPolling();
      setScreen(screens.cards);
      startCardsLobbyPolling();
    });
  }
  if (els.chessLeave) {
    els.chessLeave.addEventListener("click", async () => {
      if (!chessState.currentLobbyId) return;
      const response = await api("/miniapp/api/chess/leave", {
        method: "POST",
        body: JSON.stringify({ lobby_id: chessState.currentLobbyId }),
      }).catch(() => ({ ok: false, error: "leave" }));
      if (!response.ok) {
        showResult("Игра", "Нельзя выйти во время матча.");
        return;
      }
      chessState.currentLobbyId = null;
      chessState.selectedCell = null;
      setChessMoves([]);
      chessState.currentState = null;
      chessState.finishedShown = false;
      stopChessPolling();
      setScreen(screens.chess);
      startChessLobbyPolling();
    });
  }
  if (els.cardsStakeClose) {
    els.cardsStakeClose.addEventListener("click", closeStakePicker);
  }
  document.querySelectorAll(".buy-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const spins = Number(btn.dataset.spins);
      const cost = Number(btn.dataset.cost);
      buySpins(spins, cost);
    });
  });
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const value = Number(btn.dataset.filter);
      if (!Number.isNaN(value)) {
        setUpgradeFilter(value);
      }
    });
  });
}

if (tg) {
  tg.ready();
  tg.expand();
}

bindEvents();
setUpgradeFilter(upgradeState.filter, true);
syncUpgradeSummary();
loadState();
