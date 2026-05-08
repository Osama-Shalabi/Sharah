const DEMO_CARDS = [
  {
    id: "1",
    platform: "facebook",
    duration: "00:25",
    title: "مشاهد الغروب",
    category: "سفر",
    date: "2024-05-24",
    thumb: "radial-gradient(420px 240px at 30% 30%, rgba(255, 255, 255, 0.35), rgba(255, 255, 255, 0) 55%), linear-gradient(135deg, #ffb703, #fb8500 45%, #0b1320)",
  },
  {
    id: "2",
    platform: "facebook",
    duration: "00:18",
    title: "تحدي الرقص 2024",
    category: "لياقة",
    date: "2024-05-24",
    thumb: "radial-gradient(360px 220px at 30% 40%, rgba(147, 51, 234, 0.55), rgba(147, 51, 234, 0) 60%), linear-gradient(135deg, #111827, #4f46e5 55%, #0ea5e9)",
  },
  {
    id: "3",
    platform: "facebook",
    duration: "00:12",
    title: "لحظات كلب مضحكة",
    category: "حيوانات",
    date: "2024-05-24",
    thumb: "radial-gradient(380px 240px at 35% 35%, rgba(245, 158, 11, 0.52), rgba(245, 158, 11, 0) 60%), linear-gradient(135deg, #1f2937, #b45309 55%, #f59e0b)",
  },
  {
    id: "4",
    platform: "facebook",
    duration: "00:30",
    title: "أفكار فطور صحي",
    category: "طعام",
    date: "2024-05-24",
    thumb: "radial-gradient(360px 260px at 35% 35%, rgba(34, 197, 94, 0.52), rgba(34, 197, 94, 0) 62%), linear-gradient(135deg, #0b1320, #16a34a 55%, #a3e635)",
  },
  {
    id: "5",
    platform: "facebook",
    duration: "00:15",
    title: "استكشاف الجبال",
    category: "سفر",
    date: "2024-05-24",
    thumb: "radial-gradient(400px 260px at 30% 35%, rgba(59, 130, 246, 0.5), rgba(59, 130, 246, 0) 62%), linear-gradient(135deg, #0b1320, #0284c7 55%, #22c55e)",
  },
  {
    id: "6",
    platform: "facebook",
    duration: "00:15",
    title: "ركوب الأمواج",
    category: "رياضة",
    date: "2024-05-23",
    thumb: "radial-gradient(420px 260px at 35% 35%, rgba(14, 165, 233, 0.5), rgba(14, 165, 233, 0) 60%), linear-gradient(135deg, #0b1320, #0ea5e9 55%, #38bdf8)",
  },
  {
    id: "7",
    platform: "facebook",
    duration: "00:35",
    title: "وصفة باستا سريعة",
    category: "طعام",
    date: "2024-05-22",
    thumb: "radial-gradient(380px 240px at 35% 30%, rgba(251, 146, 60, 0.52), rgba(251, 146, 60, 0) 60%), linear-gradient(135deg, #0b1320, #ea580c 55%, #fde68a)",
  },
  {
    id: "8",
    platform: "facebook",
    duration: "00:28",
    title: "عطلة في المدينة",
    category: "سفر",
    date: "2024-05-21",
    thumb: "radial-gradient(420px 240px at 32% 28%, rgba(244, 114, 182, 0.5), rgba(244, 114, 182, 0) 62%), linear-gradient(135deg, #0b1320, #db2777 55%, #f59e0b)",
  },
  {
    id: "9",
    platform: "facebook",
    duration: "00:18",
    title: "جولة في إعداد تقني",
    category: "تقنية",
    date: "2024-05-20",
    thumb: "radial-gradient(420px 260px at 30% 35%, rgba(139, 92, 246, 0.5), rgba(139, 92, 246, 0) 62%), linear-gradient(135deg, #0b1320, #7c3aed 55%, #06b6d4)",
  },
  {
    id: "10",
    platform: "facebook",
    duration: "00:45",
    title: "تمرين رياضي",
    category: "رياضة",
    date: "2024-05-19",
    thumb: "radial-gradient(420px 260px at 35% 35%, rgba(156, 163, 175, 0.5), rgba(156, 163, 175, 0) 62%), linear-gradient(135deg, #0b1320, #374151 55%, #111827)",
  },
];

const DESKTOP_PAGE_SIZE = 20;
const MOBILE_PAGE_SIZE = 10;
const MOBILE_PAGE_QUERY = "(max-width: 720px)";
const MAX_REELS_TO_LOAD = 10000;

let CARDS = [...DEMO_CARDS];

const state = {
  draft: { q: "", platform: "all", sort: "new" },
  applied: { q: "", platform: "all", sort: "new" },
  page: 1,
};

function $(id) {
  return document.getElementById(id);
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function normalize(s) {
  return (s || "").toString().trim().toLowerCase();
}

function pageSize() {
  return window.matchMedia(MOBILE_PAGE_QUERY).matches ? MOBILE_PAGE_SIZE : DESKTOP_PAGE_SIZE;
}

function formatArabicDate(iso) {
  const d = new Date(`${iso}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "";
  return new Intl.DateTimeFormat("ar", { year: "numeric", month: "long", day: "2-digit" }).format(d);
}

function iconSvg(platform) {
  if (platform === "tiktok") {
    return `
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M14 3v10.2a4.2 4.2 0 1 1-3-4V6.5" stroke="#111827" stroke-width="2" stroke-linecap="round" />
        <path d="M14 6c1.1 2.2 3 3.4 5.5 3.6" stroke="#111827" stroke-width="2" stroke-linecap="round" />
      </svg>`;
  }
  if (platform === "instagram") {
    return `
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <rect x="5" y="5" width="14" height="14" rx="4" stroke="#111827" stroke-width="2" />
        <path d="M9.5 12a2.5 2.5 0 1 0 5 0 2.5 2.5 0 0 0-5 0Z" stroke="#111827" stroke-width="2" />
        <path d="M16.8 7.5h.01" stroke="#111827" stroke-width="3" stroke-linecap="round" />
      </svg>`;
  }
  return `
    <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M15.35 8.05h-1.65c-.48 0-.8.34-.8.86v1.45h2.32l-.35 2.38H12.9V19h-2.58v-6.26H8.35v-2.38h1.97V8.7c0-2.05 1.22-3.2 3.1-3.2.9 0 1.66.07 1.93.1v2.45Z"
        fill="#1877F2"
      />
    </svg>`;
}

function matchesFilters(card, filters) {
  if (filters.platform !== "all" && card.platform !== filters.platform) return false;
  if (card.searchRanked) return true;
  const q = normalize(filters.q);
  if (!q) return true;
  const hay = normalize(`${card.title} ${card.category}`);
  return hay.includes(q);
}

function sortCards(cards, sortKey) {
  const cloned = [...cards];
  if (sortKey === "old") {
    cloned.sort((a, b) => a.date.localeCompare(b.date));
    return cloned;
  }
  if (sortKey === "title") {
    cloned.sort((a, b) => a.title.localeCompare(b.title, "ar"));
    return cloned;
  }
  cloned.sort((a, b) => b.date.localeCompare(a.date));
  return cloned;
}

function filteredCards() {
  const cards = CARDS.filter((c) => matchesFilters(c, state.applied));
  if (state.applied.q.trim() && cards.some((c) => c.searchRanked)) return cards;
  return sortCards(cards, state.applied.sort);
}

function escapeHtml(s) {
  return (s || "")
    .toString()
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderGrid() {
  const grid = $("grid");
  const countEl = $("count");
  const cards = filteredCards();
  countEl.textContent = `عرض ${cards.length} فيديو`;

  const size = pageSize();
  const totalPages = Math.max(1, Math.ceil(cards.length / size));
  state.page = clamp(state.page, 1, totalPages);
  const start = (state.page - 1) * size;
  const pageItems = cards.slice(start, start + size);

  grid.innerHTML = "";
  for (const c of pageItems) {
    const a = document.createElement("a");
    a.className = "card";
    a.href = c.facebookReelUrl || "#";
    a.setAttribute("aria-label", c.title);
    if (c.facebookReelUrl) {
      a.target = "_blank";
      a.rel = "noopener noreferrer";
    } else {
      a.addEventListener("click", (e) => e.preventDefault());
    }

    const thumb = document.createElement("div");
    thumb.className = "thumb";
    thumb.style.backgroundImage = c.thumb;

    const shade = document.createElement("div");
    shade.className = "shade";

    const duration = document.createElement("div");
    duration.className = "badge";
    duration.textContent = c.duration;

    const platform = document.createElement("div");
    platform.className = "platform";
    platform.innerHTML = iconSvg(c.platform);

    const info = document.createElement("div");
    info.className = "info";
    info.innerHTML = `
      <div class="title">${escapeHtml(c.title)}</div>
    `;

    a.appendChild(thumb);
    a.appendChild(shade);
    a.appendChild(duration);
    a.appendChild(platform);
    a.appendChild(info);
    grid.appendChild(a);
  }

  renderPager(totalPages);
}

function renderPager(totalPages) {
  const pages = $("pages");
  const prev = $("prevPage");
  const next = $("nextPage");
  prev.disabled = state.page <= 1;
  next.disabled = state.page >= totalPages;

  pages.innerHTML = "";
  const visible = [];
  const start = Math.max(1, state.page - 1);
  const end = Math.min(totalPages, state.page + 1);
  for (let i = start; i <= end; i++) visible.push(i);

  if (start > 1) visible.unshift(1);
  if (end < totalPages) visible.push(totalPages);

  const uniq = [...new Set(visible)];
  for (const p of uniq) {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "pageBtn";
    b.textContent = `${p}`;
    if (p === state.page) b.setAttribute("aria-current", "page");
    b.addEventListener("click", () => {
      state.page = p;
      renderGrid();
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
    pages.appendChild(b);
  }
}

function setupMenu(buttonId, menuId, onSelect) {
  const btn = $(buttonId);
  const menu = $(menuId);

  function close() {
    btn.setAttribute("aria-expanded", "false");
    menu.classList.remove("open");
  }

  btn.addEventListener("click", () => {
    const isOpen = menu.classList.contains("open");
    document.querySelectorAll(".menu.open").forEach((m) => m.classList.remove("open"));
    document.querySelectorAll('[aria-expanded="true"]').forEach((b) => b.setAttribute("aria-expanded", "false"));
    if (!isOpen) {
      btn.setAttribute("aria-expanded", "true");
      menu.classList.add("open");
    }
  });

  menu.addEventListener("click", (e) => {
    const t = e.target;
    if (!(t instanceof HTMLElement)) return;
    if (!t.matches(".menuItem")) return;
    onSelect(t.dataset);
    close();
  });

  document.addEventListener("click", (e) => {
    const t = e.target;
    if (!(t instanceof Node)) return;
    if (btn.contains(t) || menu.contains(t)) return;
    close();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") close();
  });
}

async function applyFilters() {
  state.applied = { ...state.draft };
  state.page = 1;
  if (state.applied.q.trim()) {
    await loadHybridSearchResults(state.applied.q);
  }
  renderGrid();
}

async function resetFilters() {
  state.draft = { q: "", platform: "all", sort: "new" };
  state.applied = { q: "", platform: "all", sort: "new" };
  state.page = 1;
  $("q").value = "";
  $("platformLabel").textContent = "كل المنصات";
  $("sortLabel").textContent = "الأحدث";
  await loadReels();
  renderGrid();
}

function init() {
  $("q").addEventListener("input", (e) => {
    state.draft.q = e.target.value || "";
  });
  $("q").addEventListener("keydown", (e) => {
    if (e.key === "Enter") applyFilters();
  });

  setupMenu("platformBtn", "platformMenu", ({ platform }) => {
    state.draft.platform = platform || "all";
    const label =
      platform === "tiktok"
        ? "TikTok"
        : platform === "instagram"
          ? "Instagram"
          : platform === "facebook"
            ? "Facebook"
            : "كل المنصات";
    $("platformLabel").textContent = label;
  });

  setupMenu("sortBtn", "sortMenu", ({ sort }) => {
    state.draft.sort = sort || "new";
    const label = sort === "old" ? "الأقدم" : sort === "title" ? "العنوان" : "الأحدث";
    $("sortLabel").textContent = label;
  });

  $("applyBtn").addEventListener("click", applyFilters);
  $("resetBtn").addEventListener("click", resetFilters);

  $("prevPage").addEventListener("click", () => {
    state.page = Math.max(1, state.page - 1);
    renderGrid();
  });
  $("nextPage").addEventListener("click", () => {
    state.page = state.page + 1;
    renderGrid();
  });

  window.matchMedia(MOBILE_PAGE_QUERY).addEventListener("change", () => {
    state.page = 1;
    renderGrid();
  });

  applyFilters();
}

init();

function reelToCard(r, idx, { searchRanked = false } = {}) {
  const title = r.title || "";
  const category = r.topic || "عام";
  const date = "";
  const thumb = r.thumbnail
    ? `linear-gradient(180deg, rgba(0,0,0,0.05), rgba(0,0,0,0.35)), url('${`${r.thumbnail}`.replaceAll("'", "%27")}')`
    : "radial-gradient(420px 260px at 35% 35%, rgba(122, 90, 59, 0.22), rgba(122, 90, 59, 0) 62%), linear-gradient(135deg, #0b1320, #6a4a30 55%, #efe7de)";

  return {
    id: r.id || `${idx + 1}`,
    platform: "facebook",
    duration: r.duration || "",
    title,
    category,
    date,
    thumb,
    facebookReelUrl: r.facebookReelUrl,
    searchRanked,
  };
}

async function loadReels() {
  try {
    const resp = await fetch(`/api/sharah/reels?limit=${MAX_REELS_TO_LOAD}&_ts=${Date.now()}`, { cache: "no-store" });
    if (!resp.ok) throw new Error(`status=${resp.status}`);
    const data = await resp.json();
    const reels = Array.isArray(data) ? data : [];
    if (reels.length === 0) throw new Error("empty");

    const fetchedCards = reels.filter((r) => r?.facebookReelUrl).map((r, idx) => reelToCard(r, idx));
    CARDS = fetchedCards;
    renderGrid();
  } catch {
    console.warn("Could not fetch Facebook reels");
  }
}

async function loadHybridSearchResults(query) {
  try {
    const url = `/api/sharah/reels/search?q=${encodeURIComponent(query)}&limit=${MAX_REELS_TO_LOAD}&_ts=${Date.now()}`;
    const resp = await fetch(url, { cache: "no-store" });
    if (!resp.ok) throw new Error(`status=${resp.status}`);
    const data = await resp.json();
    const reels = Array.isArray(data) ? data : [];
    CARDS = reels.filter((r) => r?.facebookReelUrl).map((r, idx) => reelToCard(r, idx, { searchRanked: true }));
  } catch {
    console.warn("Could not fetch hybrid search results");
  }
}

loadReels();
