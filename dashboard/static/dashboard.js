/**
 * dashboard/static/dashboard.js
 * ──────────────────────────────
 * WebSocket client + Chart.js live chart for the AQI dashboard.
 */

// ── State ─────────────────────────────────────────────────────────────────────
const cityData      = {};   // city → latest payload
let   selectedCity  = null;
let   updateCount   = 0;
let   chart         = null;

// ── Chart colours per city (cycles if more than 8 cities) ────────────────────
const CHART_COLORS = [
  "#3b82f6","#22c55e","#f97316","#a855f7",
  "#ef4444","#eab308","#06b6d4","#ec4899",
];
const cityColorMap = {};
let colorIndex = 0;
function getCityColor(city) {
  if (!cityColorMap[city]) {
    cityColorMap[city] = CHART_COLORS[colorIndex++ % CHART_COLORS.length];
  }
  return cityColorMap[city];
}

// ── AQI CSS class helper ──────────────────────────────────────────────────────
function cssClass(cssFromServer) {
  // Server sends css_class like "very-unhealthy" — pass through directly
  return cssFromServer || "good";
}

// ── Smooth number counter animation ──────────────────────────────────────────
function animateNumber(el, from, to, duration = 500) {
  const start = performance.now();
  function step(now) {
    const t    = Math.min((now - start) / duration, 1);
    const ease = t < 0.5 ? 2*t*t : -1+(4-2*t)*t;
    el.textContent = Math.round(from + (to - from) * ease);
    if (t < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

// ── Build / update a city card ────────────────────────────────────────────────
function upsertCard(d) {
  const prevAqi = cityData[d.city]?.aqi ?? d.aqi;
  cityData[d.city] = d;

  const cat = cssClass(d.css_class);
  let card  = document.getElementById(`card-${d.city.replace(/\s+/g, "-")}`);

  if (!card) {
    // ── Create card DOM ────────────────────────────────────────────────────
    card = document.createElement("div");
    card.className = `city-card ${cat}`;
    card.id        = `card-${d.city.replace(/\s+/g, "-")}`;
    const sourceLabel = d.source || "Simulation";
    card.innerHTML = `
      <div class="card-header">
        <span class="city-name">${d.city} <small style="font-size: 0.6em; opacity: 0.6;">(${sourceLabel})</small></span>
        <span class="trend-badge" id="trend-${d.city}"></span>
      </div>
      <div class="aqi-value ${cat}" id="aqi-${d.city}">${d.aqi.toFixed(1)}</div>
      <div class="category-badge ${cat}" id="cat-${d.city}">${d.category}</div>
      <div class="pollutants">
        <div class="chip">PM2.5 <span id="pm25-${d.city}">${d.pm25}</span></div>
        <div class="chip">PM10 <span id="pm10-${d.city}">${d.pm10}</span></div>
        <div class="chip">NO₂ <span id="no2-${d.city}">${d.no2}</span></div>
        <div class="chip">O₃ <span id="o3-${d.city}">${d.o3}</span></div>
      </div>
      <div class="prediction-row">
        <span class="pred-label">⏱ Next Hour</span>
        <div>
          <span class="pred-value ${cssClass(d.next_hour_css)}" id="pred-${d.city}">${d.next_hour_aqi}</span>
          <span class="pred-cat" id="predcat-${d.city}">${d.next_hour_label}</span>
        </div>
      </div>`;

    document.getElementById("loader")?.remove();
    document.getElementById("cardsContainer").appendChild(card);
    addCityButton(d.city);

  } else {
    // ── Update existing card ───────────────────────────────────────────────
    card.className = `city-card ${cat} updated`;
    setTimeout(() => card.classList.remove("updated"), 600);

    const aqiEl = document.getElementById(`aqi-${d.city}`);
    aqiEl.className = `aqi-value ${cat}`;
    animateNumber(aqiEl, prevAqi, d.aqi);

    document.getElementById(`cat-${d.city}`).className = `category-badge ${cat}`;
    document.getElementById(`cat-${d.city}`).textContent = d.category;

    document.getElementById(`pm25-${d.city}`).textContent = d.pm25;
    document.getElementById(`pm10-${d.city}`).textContent = d.pm10;
    document.getElementById(`no2-${d.city}`).textContent  = d.no2;
    document.getElementById(`o3-${d.city}`).textContent   = d.o3;

    document.getElementById(`pred-${d.city}`).textContent    = d.next_hour_aqi;
    document.getElementById(`pred-${d.city}`).className      = `pred-value ${cssClass(d.next_hour_css)}`;
    document.getElementById(`predcat-${d.city}`).textContent = d.next_hour_label;
  }

  // ── Trend arrow ────────────────────────────────────────────────────────────
  const trendEl    = document.getElementById(`trend-${d.city}`);
  trendEl.textContent = d.trend;
  trendEl.className = "trend-badge " +
    (d.trend === "↑" ? "trend-up" : d.trend === "↓" ? "trend-down" : "trend-flat");
}

// ── Summary bar ───────────────────────────────────────────────────────────────
function updateSummary(cities) {
  if (!cities.length) return;
  const sorted = [...cities].sort((a, b) => a.aqi - b.aqi);
  document.getElementById("bestCity").textContent  = `${sorted[0].city} (${Math.round(sorted[0].aqi)})`;
  document.getElementById("worstCity").textContent = `${sorted[sorted.length-1].city} (${Math.round(sorted[sorted.length-1].aqi)})`;
  document.getElementById("cityCount").textContent  = cities.length;
  document.getElementById("updateCount").textContent = ++updateCount;
  document.getElementById("lastUpdated").textContent =
    "Updated " + new Date().toLocaleTimeString();
}

// ── Chart ─────────────────────────────────────────────────────────────────────
function initChart() {
  const ctx  = document.getElementById("aqiChart").getContext("2d");
  chart = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 400 },
      scales: {
        x: {
          ticks: { color: "#4a5a7a", maxTicksLimit: 8, font: { size: 11 } },
          grid:  { color: "rgba(255,255,255,0.04)" },
        },
        y: {
          ticks: { color: "#4a5a7a", font: { size: 11 } },
          grid:  { color: "rgba(255,255,255,0.06)" },
          suggestedMin: 0, suggestedMax: 200,
        },
      },
      plugins: {
        legend: { labels: { color: "#8899bb", font: { size: 12 } } },
        tooltip: {
          backgroundColor: "#0d1420",
          borderColor: "rgba(255,255,255,0.12)", borderWidth: 1,
          titleColor: "#f0f4ff", bodyColor: "#8899bb",
        },
      },
    },
  });
}

function updateChart(citiesPayload) {
  if (!chart) return;

  const visibleCities = selectedCity ? [selectedCity] : Object.keys(cityData);
  let maxLen = 0;

  // Ensure a dataset exists per visible city
  visibleCities.forEach(city => {
    let ds = chart.data.datasets.find(d => d.label === city);
    if (!ds) {
      ds = {
        label: city,
        data: [],
        borderColor: getCityColor(city),
        backgroundColor: getCityColor(city) + "22",
        pointRadius: 3,
        tension: 0.35,
        fill: true,
        borderWidth: 2,
      };
      chart.data.datasets.push(ds);
    }
    const payload = citiesPayload.find(c => c.city === city);
    if (payload && payload.history_aqi) {
      ds.data = [...payload.history_aqi];
    } else if (payload) {
      ds.data.push(payload.aqi);
      if (ds.data.length > 40) ds.data.shift();
    }
    if (ds.data.length > maxLen) maxLen = ds.data.length;
  });

  // Sync labels array to max length
  if (chart.data.labels.length === 0 && maxLen > 0) {
      // First load: backfill dummy labels
      const now = new Date();
      for (let i = maxLen - 1; i >= 0; i--) {
          chart.data.labels.push(new Date(now.getTime() - i * 5000).toLocaleTimeString());
      }
  } else {
      // Normal update: shift labels if needed
      if (maxLen > chart.data.labels.length) {
          chart.data.labels.push(new Date().toLocaleTimeString());
      } else if (maxLen > 0) {
          chart.data.labels.shift();
          chart.data.labels.push(new Date().toLocaleTimeString());
      }
      if (chart.data.labels.length > 40) {
          chart.data.labels.shift();
      }
  }

  // Remove datasets for deselected cities
  chart.data.datasets = chart.data.datasets.filter(d =>
    visibleCities.includes(d.label)
  );

  chart.update("none");
}

// ── City selector buttons ─────────────────────────────────────────────────────
function addCityButton(city) {
  const sel = document.getElementById("citySelector");
  if (document.getElementById(`btn-${city}`)) return;

  const btn = document.createElement("button");
  btn.className = "city-btn";
  btn.id        = `btn-${city}`;
  btn.textContent = city;
  btn.addEventListener("click", () => {
    if (selectedCity === city) {
      selectedCity = null;
      btn.classList.remove("active");
    } else {
      document.querySelectorAll(".city-btn").forEach(b => b.classList.remove("active"));
      selectedCity = city;
      btn.classList.add("active");
      // Reset chart to show only this city
      chart.data.datasets = [];
      chart.data.labels   = [];
    }
  });
  sel.appendChild(btn);
}

// ── Socket.IO connection ──────────────────────────────────────────────────────
function connect() {
  const socket = io({ transports: ["websocket", "polling"] });

  socket.on("connect", () => {
    document.getElementById("liveBadge").style.opacity = "1";
    console.log("[WS] Connected");
  });

  socket.on("disconnect", () => {
    document.getElementById("liveBadge").style.opacity = "0.4";
    document.getElementById("lastUpdated").textContent = "Reconnecting…";
  });

  socket.on("city_update", (cities) => {
    cities.forEach(upsertCard);
    updateSummary(cities);
    updateChart(cities);
  });
}

// ── Demo Mode Toggle ──────────────────────────────────────────────────────────
function initDemoToggle() {
  const btn = document.getElementById("demoModeToggle");
  if (!btn) return;
  let isDemo = false;
  
  btn.addEventListener("click", () => {
    isDemo = !isDemo;
    btn.textContent = `Demo Mode: ${isDemo ? "ON" : "OFF"}`;
    btn.style.background = isDemo ? "rgba(239, 68, 68, 0.2)" : "rgba(255,255,255,0.1)";
    btn.style.borderColor = isDemo ? "rgba(239, 68, 68, 0.5)" : "rgba(255,255,255,0.2)";
    
    fetch("/api/mode", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ demo: isDemo })
    }).catch(e => console.error("Mode toggle failed", e));
  });
}

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  initDemoToggle();
  initChart();
  connect();

  // Fetch initial snapshot so cards appear immediately without waiting for push
  fetch("/api/current")
    .then(r => r.json())
    .then(cities => {
      if (cities.length) {
        cities.forEach(upsertCard);
        updateSummary(cities);
        updateChart(cities);
      }
    })
    .catch(() => {/* dashboard not ready yet */});
});
