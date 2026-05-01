const socket = io();

// ── State ───────────────────────────────────────────────────────────────────
let allCitiesData = {};
let selectedCity = null;
let aqiChart = null;
let _dropdownPopulated = false;   // guard against duplicate options

// ── Toast Notifications ─────────────────────────────────────────────────────
function showToast(message, type = "warning", duration = 3500) {
  const container = document.getElementById("toastContainer");
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.classList.add("hiding");
    toast.addEventListener("animationend", () => toast.remove());
  }, duration);
}

// ── Connection Status Indicator ─────────────────────────────────────────────
const connDot = document.getElementById("connStatus");

socket.on("connect", () => {
  connDot.className = "conn-dot connected";
  connDot.title = "Connected — receiving live data";
});
socket.on("disconnect", () => {
  connDot.className = "conn-dot disconnected";
  connDot.title = "Disconnected — attempting to reconnect…";
});

// ── Chart Initialization ────────────────────────────────────────────────────
function initChart() {
  const ctx = document.getElementById("aqiChart").getContext("2d");
  Chart.defaults.color = "#94a3b8";
  Chart.defaults.font.family = "'Inter', sans-serif";
  
  aqiChart = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400, easing: 'easeOutQuart' },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(15,22,38,0.9)",
          titleFont: { size: 13 },
          bodyFont: { size: 14, weight: 'bold' },
          padding: 12,
          cornerRadius: 8,
          displayColors: false
        }
      },
      scales: {
        x: { 
          title: { display: true, text: "Time (Recent Readings)", color: "rgba(255,255,255,0.5)", font: { size: 11, weight: '500' } },
          grid: { color: "rgba(255,255,255,0.04)" } 
        },
        y: { 
          title: { display: true, text: "Air Quality Index (AQI)", color: "rgba(255,255,255,0.5)", font: { size: 11, weight: '500' } },
          grid: { color: "rgba(255,255,255,0.04)" },
          beginAtZero: true,
          suggestedMax: 200
        }
      }
    }
  });
}

// ── Render Logic ────────────────────────────────────────────────────────────

// Safe accessor — returns "—" for null/undefined values
function safeVal(val, suffix) {
  if (val == null || val === "" || val === undefined) return "—";
  return suffix ? `${val}${suffix}` : val;
}

// 1. Update the Main Dashboard Grid (Tab 1)
function updateDashboardCards(cities) {
  const container = document.getElementById("cityCardsContainer");
  if(container.querySelector(".loader")) container.innerHTML = "";
  
  // Build a set of current city names for stale-card cleanup
  const activeCityNames = new Set(cities.map(c => c.city));

  // Remove stale cards (cities that are no longer in the active set)
  container.querySelectorAll(".city-card").forEach(card => {
    const cardCity = card.id.replace("card-", "");
    if (!activeCityNames.has(cardCity)) {
      card.remove();
    }
  });

  // Sort cities by priority score (worst-first)
  const sorted = [...cities].sort((a, b) => (b.priority?.score || 0) - (a.priority?.score || 0));

  sorted.forEach(city => {
    let card = document.getElementById(`card-${city.city}`);
    
    // Create card if missing
    if(!card) {
      card = document.createElement("div");
      card.id = `card-${city.city}`;
      card.className = "city-card";
      
      // Clicking a card jumps to Detailed Insights page
      card.onclick = () => {
        selectedCity = city.city;
        document.getElementById("globalCitySelector").value = city.city;
        switchTab("insightsTab");
        refreshInsightsView();
      };
      
      container.appendChild(card);
    }
    
    // Update contents
    const pri = city.priority || {};
    const priBadge = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}[pri.priority] || "⚪";
    card.className = `city-card ${city.css_class || ""}`;
    card.innerHTML = `
      <div class="card-header">
        <span class="city-name">${city.city}</span>
        <span class="priority-badge" title="Priority: ${pri.priority || 'unknown'}">${priBadge}</span>
      </div>
      <div class="aqi-value ${city.css_class || ""}">${safeVal(city.aqi)}</div>
      <div class="category-badge ${city.css_class || ""}">${safeVal(city.category)}</div>
      <div class="pollutants">
        <span class="chip">PM2.5 <b>${safeVal(city.pm25)}</b></span>
        <span class="chip">PM10 <b>${safeVal(city.pm10)}</b></span>
      </div>
    `;
  });
}

// 2. Update the Insights view (Hero Card + Intel Grid) (Tab 2)
function updateHeroAndIntel(cityData) {
  // --- Hero Section ---
  const aqiEl = document.getElementById("heroAqiValue");
  const catEl = document.getElementById("heroCategory");
  const timeEl = document.getElementById("heroTime");
  const trendEl = document.getElementById("heroTrend");
  
  aqiEl.textContent = safeVal(cityData.aqi);
  aqiEl.className = `aqi-num ${cityData.css_class || ""}`;
  catEl.textContent = safeVal(cityData.category);
  catEl.className = `category-badge ${cityData.css_class || ""}`;
  trendEl.textContent = cityData.trend || "→";
  
  if (cityData.trend === "↑") trendEl.className = "trend-icon trend-up";
  else if (cityData.trend === "↓") trendEl.className = "trend-icon trend-down";
  else trendEl.className = "trend-icon trend-flat";
  
  if (cityData.timestamp) {
    const d = new Date(cityData.timestamp);
    timeEl.textContent = `Updated: ${d.toLocaleTimeString()}`;
  } else {
    timeEl.textContent = "--:--";
  }

  // --- Intel Grid (Structured Messages) ---
  const msg = cityData.message || {};

  // AI Insight (now uses structured message)
  document.getElementById("insightText").textContent = msg.summary || cityData.insight || "Waiting for data…";
  
  // Prediction (uses structured prediction_note)
  document.getElementById("predValue").textContent = safeVal(cityData.next_hour_aqi);
  document.getElementById("predValue").className = `focus-text ${cityData.next_hour_css || ""}`;
  document.getElementById("predLabel").textContent = msg.prediction_note || cityData.next_hour_label || "";
  
  // Traffic
  const t = cityData.traffic || "Medium";
  const trafficText = document.getElementById("trafficText");
  const trafficIndicator = document.getElementById("trafficIndicator");
  trafficText.textContent = t;
  trafficText.className = `focus-text traffic-${t.toLowerCase()}`;
  trafficIndicator.className = `traffic-dot traffic-${t.toLowerCase()}-dot`;
  
  // Alerts (uses structured title for alert-level severity)
  const alertCard = document.getElementById("cardAlerts");
  const alertEl = document.getElementById("alertText");
  const severity = msg.severity || "good";
  if (severity === "unhealthy" || severity === "very-unhealthy" || severity === "hazardous") {
    alertEl.textContent = msg.title || cityData.alert || "Alert active";
    alertCard.classList.add("has-alert");
  } else {
    alertEl.textContent = "No active alerts";
    alertCard.classList.remove("has-alert");
  }
  
  // Health Advisory (uses structured advice)
  document.getElementById("healthText").textContent = msg.advice || cityData.health_advisory || "—";
}

// 3. Update the line chart in Insights view
function updateChart(cityData) {
  if (!aqiChart) return;
  const history = cityData.history_aqi || [];
  
  aqiChart.data.labels = history.map((_, i) => {
    const past = history.length - 1 - i;
    return past === 0 ? "Now" : `-${past}`;
  });
  
  // Create gradient
  const ctx = aqiChart.canvas.getContext("2d");
  const grad = ctx.createLinearGradient(0, 0, 0, 280);
  
  // Get CSS variable color based on category class
  const dummyEl = document.createElement("div");
  dummyEl.className = cityData.css_class || "";
  document.body.appendChild(dummyEl);
  let color = getComputedStyle(dummyEl).color;
  document.body.removeChild(dummyEl);
  if(!color || color === 'rgba(0, 0, 0, 0)') color = "#3b82f6";
  
  // Convert standard hex/rgb to rgba for gradient
  grad.addColorStop(0, color.replace('rgb', 'rgba').replace(')', ', 0.2)'));
  grad.addColorStop(1, "rgba(23, 29, 43, 0)");

  aqiChart.data.datasets = [{
    label: "AQI",
    data: history,
    borderColor: color,
    backgroundColor: grad,
    borderWidth: 2.5,
    pointBackgroundColor: '#171d2b',
    pointBorderColor: color,
    pointRadius: 0,
    pointHoverRadius: 5,
    fill: true,
    tension: 0.4
  }];
  aqiChart.update();
}

function refreshInsightsView() {
  if (selectedCity && allCitiesData[selectedCity]) {
    updateHeroAndIntel(allCitiesData[selectedCity]);
    updateChart(allCitiesData[selectedCity]);
  }
}

// ── Populate / Rebuild Dropdown ─────────────────────────────────────────────
function rebuildDropdown(cities) {
  const sel = document.getElementById("globalCitySelector");
  sel.innerHTML = "";
  cities.forEach(c => {
    const opt = document.createElement("option");
    opt.value = c.city;
    opt.textContent = c.city;
    sel.appendChild(opt);
  });
  _dropdownPopulated = true;
}

// ── WebSockets ──────────────────────────────────────────────────────────────
socket.on("city_update", (cities) => {
  if (!cities || cities.length === 0) return;

  // Stash data
  cities.forEach(c => { allCitiesData[c.city] = c; });
  
  // Populate dropdown once (or if it was cleared after a city change)
  const sel = document.getElementById("globalCitySelector");
  if (!_dropdownPopulated || sel.options.length === 0) {
    rebuildDropdown(cities);
    // Set initial selection to worst-AQI city
    selectedCity = [...cities].sort((a,b) => (b.aqi || 0) - (a.aqi || 0))[0].city;
    sel.value = selectedCity;
  }
  
  updateDashboardCards(cities);
  
  if (document.getElementById("insightsTab").classList.contains("active")) {
    refreshInsightsView();
  }
});


// ── Event Listeners ─────────────────────────────────────────────────────────

// Dropdown (only visible/used on Insights tab)
document.getElementById("globalCitySelector").addEventListener("change", (e) => {
  selectedCity = e.target.value;
  refreshInsightsView();
});

// Demo Mode Toggle
document.getElementById("demoToggle").addEventListener("change", (e) => {
  fetch("/api/mode", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ demo: e.target.checked })
  });
});

// Tab Switcher
document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", (e) => {
    switchTab(e.target.dataset.target);
  });
});

function switchTab(targetId) {
  // Update buttons
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelector(`[data-target="${targetId}"]`).classList.add("active");
  
  // Update content
  document.querySelectorAll(".tab-content").forEach(c => c.classList.remove("active"));
  document.getElementById(targetId).classList.add("active");
  
  // Toggle UI elements
  const sel = document.getElementById("globalCitySelector");
  if (targetId === "insightsTab") {
    sel.style.display = "block";  // Show dropdown in top bar

    // Auto-select worst city if nothing is selected yet
    if (!selectedCity || !allCitiesData[selectedCity]) {
      const available = Object.values(allCitiesData);
      if (available.length > 0) {
        selectedCity = available.sort((a,b) => (b.aqi || 0) - (a.aqi || 0))[0].city;
        sel.value = selectedCity;
      }
    }

    refreshInsightsView();
    if(aqiChart) setTimeout(() => aqiChart.resize(), 50); // Resize fix
  } else {
    sel.style.display = "none";   // Hide dropdown on dashboard home
  }
}

// ── City Picker Modal Logic ─────────────────────────────────────────────────
const modal = document.getElementById("cityPickerModal");
const btnOpen = document.getElementById("openCityPicker");
const btnCancel = document.getElementById("cityCancel");
const btnApply = document.getElementById("cityApply");
const btnSelectAll = document.getElementById("citySelectAll");
const btnClearAll = document.getElementById("cityClearAll");
const searchInput = document.getElementById("citySearch");
const listContainer = document.getElementById("cityListContainer");
const counterEl = document.getElementById("cityCounter");

// Custom Coord elements
const customLatInput = document.getElementById("customLat");
const customLonInput = document.getElementById("customLon");
const btnAddCustom = document.getElementById("addCustomCityBtn");
const customMsg = document.getElementById("customCoordMsg");

let catalogData = [];
let maxCities = 8;
let tempSelected = new Set(); // holds active selection during modal open

// 1. Fetch Catalog on load
async function fetchCatalog() {
  try {
    const res = await fetch("/api/catalog");
    const data = await res.json();
    catalogData = data.cities;
    maxCities = data.max;
    
    // Check localStorage first
    const saved = localStorage.getItem("selectedCities");
    if (saved) {
      const parsed = JSON.parse(saved);
      // Ensure we don't exceed max if user tampered with localStorage
      const valid = parsed.slice(0, maxCities);
      syncBackendCities(valid); // Tell backend what we want
      catalogData.forEach(c => c.active = valid.includes(c.name));
    }
  } catch (err) {
    console.error("Failed to load catalog:", err);
  }
}

// 2. Open Modal
btnOpen.addEventListener("click", () => {
  tempSelected = new Set(catalogData.filter(c => c.active).map(c => c.name));
  renderCatalogList(catalogData);
  modal.style.display = "flex";
  searchInput.value = "";
  searchInput.focus();
});

// 3. Close Modal
function closeModal() {
  modal.style.display = "none";
}
btnCancel.addEventListener("click", closeModal);

// Close on Escape key
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && modal.style.display !== "none") {
    closeModal();
  }
});

// Close on backdrop click (click on overlay, not modal-card)
modal.addEventListener("click", (e) => {
  if (e.target === modal) {
    closeModal();
  }
});

// 4. Render the List
function renderCatalogList(citiesToRender) {
  listContainer.innerHTML = "";
  
  // Group by region
  const grouped = {};
  citiesToRender.forEach(c => {
    grouped[c.region] = grouped[c.region] || [];
    grouped[c.region].push(c);
  });
  
  for (const [region, cities] of Object.entries(grouped)) {
    if (cities.length === 0) continue;
    
    const regionHeading = document.createElement("div");
    regionHeading.style.cssText = "font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin: 10px 0 4px; font-weight: 700; letter-spacing: 1px;";
    regionHeading.textContent = region || "Global";
    listContainer.appendChild(regionHeading);
    
    cities.forEach(city => {
      const isSelected = tempSelected.has(city.name);
      
      const opt = document.createElement("div");
      opt.className = `city-option ${isSelected ? "selected" : ""}`;
      opt.innerHTML = `
        <span class="city-name">${city.name}</span>
        <span class="city-region">${isSelected ? "✓" : "+"}</span>
      `;
      
      opt.addEventListener("click", () => toggleCity(city.name, opt));
      listContainer.appendChild(opt);
    });
  }
  updateCounter();
}

function toggleCity(name, el) {
  if (tempSelected.has(name)) {
    tempSelected.delete(name);
    el.classList.remove("selected");
    el.querySelector(".city-region").textContent = "+";
  } else {
    if (tempSelected.size >= maxCities) {
      showToast(`Maximum ${maxCities} cities allowed. Deselect one first.`, "warning");
      return;
    }
    tempSelected.add(name);
    el.classList.add("selected");
    el.querySelector(".city-region").textContent = "✓";
  }
  updateCounter();
}

function updateCounter() {
  counterEl.textContent = `${tempSelected.size}/${maxCities}`;
  counterEl.style.color = tempSelected.size === maxCities ? "var(--unhealthy)" : "var(--accent)";
  btnApply.disabled = tempSelected.size === 0;
  btnApply.style.opacity = tempSelected.size === 0 ? "0.5" : "1";
}

// 5. Search filtering
searchInput.addEventListener("input", (e) => {
  const q = e.target.value.toLowerCase();
  const filtered = catalogData.filter(c => c.name.toLowerCase().includes(q));
  renderCatalogList(filtered);
});

// 6. Bulk actions
btnSelectAll.addEventListener("click", () => {
  // Only select up to maxCities from the currently visible filtered list
  const q = searchInput.value.toLowerCase();
  const visible = catalogData.filter(c => c.name.toLowerCase().includes(q));
  
  for (const c of visible) {
    if (tempSelected.size >= maxCities) break;
    tempSelected.add(c.name);
  }
  renderCatalogList(visible);
});

btnClearAll.addEventListener("click", () => {
  tempSelected.clear();
  const q = searchInput.value.toLowerCase();
  const visible = catalogData.filter(c => c.name.toLowerCase().includes(q));
  renderCatalogList(visible);
});

// 7. Apply Changes
btnApply.addEventListener("click", async () => {
  if (tempSelected.size === 0) return;
  
  const selectedArr = Array.from(tempSelected);
  
  // 1. Update internal state
  catalogData.forEach(c => c.active = tempSelected.has(c.name));
  
  // 2. Persist to localStorage
  localStorage.setItem("selectedCities", JSON.stringify(selectedArr));
  
  // 3. Clear UI waiting for new data
  document.getElementById("cityCardsContainer").innerHTML = `
    <div class="loader">
      <div class="spinner"></div>
      <p style="margin-top: 1rem; color: var(--text-secondary);">Switching cities…</p>
    </div>
  `;
  
  // 4. Reset dropdown so it rebuilds on next data push
  document.getElementById("globalCitySelector").innerHTML = "";
  _dropdownPopulated = false;
  
  // 5. Clear stale allCitiesData
  allCitiesData = {};
  selectedCity = null;
  
  // 6. Tell Backend
  await syncBackendCities(selectedArr);
  
  showToast(`Now monitoring ${selectedArr.length} cities`, "success", 2500);
  closeModal();
});

async function syncBackendCities(citiesList) {
  try {
    await fetch("/api/cities", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cities: citiesList })
    });
  } catch (err) {
    console.error("Failed to sync cities with backend:", err);
  }
}

// ── Custom Coordinate API Logic ───────────────────────────────────────────────
btnAddCustom.addEventListener("click", async () => {
  const lat = parseFloat(customLatInput.value);
  const lon = parseFloat(customLonInput.value);
  
  if (isNaN(lat) || isNaN(lon)) {
    showCustomMsg("Please enter valid numeric coordinates", true);
    return;
  }
  
  if (tempSelected.size >= maxCities) {
    showCustomMsg(`Maximum ${maxCities} cities. Deselect one first.`, true);
    return;
  }

  // Loading state
  btnAddCustom.disabled = true;
  btnAddCustom.textContent = "…";
  showCustomMsg("Locating…", false);
  
  try {
    const res = await fetch("/api/custom_city", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lat, lon })
    });
    
    const data = await res.json();
    
    if (!res.ok) {
      showCustomMsg(data.error || "Failed to locate coordinates", true);
    } else {
      // Success! Add to local catalog if not already there
      const exists = catalogData.find(c => c.name === data.name);
      if (!exists) {
        catalogData.push({
          name: data.name,
          region: data.region || "Custom",
          active: true
        });
      } else {
        exists.active = true;
      }
      
      // Auto-select it
      tempSelected.add(data.name);
      
      // Re-render
      searchInput.value = "";
      renderCatalogList(catalogData);
      
      // Feedback
      showCustomMsg(`Added ${data.name}!`, false);
      customLatInput.value = "";
      customLonInput.value = "";
    }
  } catch (err) {
    showCustomMsg("Network error connecting to backend", true);
  } finally {
    btnAddCustom.disabled = false;
    btnAddCustom.textContent = "Add";
  }
});

function showCustomMsg(text, isError) {
  customMsg.textContent = text;
  customMsg.className = `custom-coord-msg ${isError ? 'error' : ''}`;
  customMsg.style.display = 'block';
  if (!isError) setTimeout(() => customMsg.style.display = 'none', 3000);
}

// ── Boot ────────────────────────────────────────────────────────────────────
initChart();
fetchCatalog();

// ── REST Polling Fallback ───────────────────────────────────────────────────
// Fetches /api/current as a backup in case the WebSocket is slow or broken.
// Auto-disables once WebSocket delivers data.
let _wsReceivedData = false;
let _restPollTimer = null;

socket.on("city_update", () => { _wsReceivedData = true; });

async function _restPoll() {
  if (_wsReceivedData) {
    // WebSocket is working — stop REST polling
    if (_restPollTimer) { clearInterval(_restPollTimer); _restPollTimer = null; }
    return;
  }
  try {
    const res = await fetch("/api/current");
    const cities = await res.json();
    if (cities && cities.length > 0) {
      cities.forEach(c => { allCitiesData[c.city] = c; });

      const sel = document.getElementById("globalCitySelector");
      if (!_dropdownPopulated || sel.options.length === 0) {
        rebuildDropdown(cities);
        selectedCity = [...cities].sort((a,b) => (b.aqi || 0) - (a.aqi || 0))[0].city;
        sel.value = selectedCity;
      }

      updateDashboardCards(cities);
      if (document.getElementById("insightsTab").classList.contains("active")) {
        refreshInsightsView();
      }
    }
  } catch (err) {
    console.log("[REST fallback] Fetch error:", err);
  }
}

// Initial fetch on page load (get any cached data immediately)
_restPoll();

// Poll every 5s as fallback until WebSocket delivers
_restPollTimer = setInterval(_restPoll, 5000);
