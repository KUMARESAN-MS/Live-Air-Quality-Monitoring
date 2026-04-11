const socket = io();

// ── State ───────────────────────────────────────────────────────────────────
let allCitiesData = {};
let selectedCity = null;
let aqiChart = null;

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
        x: { grid: { color: "rgba(255,255,255,0.05)" } },
        y: { 
          grid: { color: "rgba(255,255,255,0.05)" },
          beginAtZero: true,
          suggestedMax: 200
        }
      }
    }
  });
}

// ── Render Logic ────────────────────────────────────────────────────────────

// 1. Update the Main Dashboard Grid (Tab 1)
function updateDashboardCards(cities) {
  const container = document.getElementById("cityCardsContainer");
  if(container.querySelector(".loader")) container.innerHTML = "";
  
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
    card.className = `city-card ${city.css_class}`;
    card.innerHTML = `
      <div class="card-header">
        <span class="city-name">${city.city}</span>
        <span class="priority-badge" title="Priority: ${pri.priority || 'unknown'}">${priBadge}</span>
      </div>
      <div class="aqi-value ${city.css_class}">${city.aqi}</div>
      <div class="category-badge ${city.css_class}">${city.category}</div>
      <div class="pollutants">
        <span class="chip">PM2.5 <b>${city.pm25}</b></span>
        <span class="chip">PM10 <b>${city.pm10}</b></span>
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
  
  aqiEl.textContent = cityData.aqi;
  aqiEl.className = `aqi-num ${cityData.css_class}`;
  catEl.textContent = cityData.category;
  catEl.className = `category-badge ${cityData.css_class}`;
  trendEl.textContent = cityData.trend;
  
  if (cityData.trend === "↑") trendEl.className = "trend-icon trend-up";
  else if (cityData.trend === "↓") trendEl.className = "trend-icon trend-down";
  else trendEl.className = "trend-icon trend-flat";
  
  const d = new Date(cityData.timestamp);
  timeEl.textContent = `Updated: ${d.toLocaleTimeString()}`;

  // --- Intel Grid (Structured Messages) ---
  const msg = cityData.message || {};

  // AI Insight (now uses structured message)
  document.getElementById("insightText").textContent = msg.summary || cityData.insight || "Waiting for data...";
  
  // Prediction (uses structured prediction_note)
  document.getElementById("predValue").textContent = cityData.next_hour_aqi;
  document.getElementById("predValue").className = `focus-text ${cityData.next_hour_css}`;
  document.getElementById("predLabel").textContent = msg.prediction_note || cityData.next_hour_label;
  
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
  
  aqiChart.data.labels = history.map((_, i) => `-${history.length - i}`);
  
  // Create gradient
  const ctx = aqiChart.canvas.getContext("2d");
  const grad = ctx.createLinearGradient(0, 0, 0, 300);
  
  // Get CSS variable color based on category class
  const dummyEl = document.createElement("div");
  dummyEl.className = cityData.css_class;
  document.body.appendChild(dummyEl);
  let color = getComputedStyle(dummyEl).color;
  document.body.removeChild(dummyEl);
  if(!color || color === 'rgba(0, 0, 0, 0)') color = "#3b82f6";
  
  // Convert standard hex/rgb to rgba for gradient
  grad.addColorStop(0, color.replace('rgb', 'rgba').replace(')', ', 0.25)'));
  grad.addColorStop(1, "rgba(21, 27, 41, 0)");

  aqiChart.data.datasets = [{
    label: "AQI",
    data: history,
    borderColor: color,
    backgroundColor: grad,
    borderWidth: 3,
    pointBackgroundColor: '#151b29',
    pointBorderColor: color,
    pointRadius: 0,
    pointHoverRadius: 6,
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

// ── WebSockets ──────────────────────────────────────────────────────────────
socket.on("city_update", (cities) => {
  if (!cities || cities.length === 0) return;

  // Stash data
  cities.forEach(c => { allCitiesData[c.city] = c; });
  
  // Populate dropdown once
  const sel = document.getElementById("globalCitySelector");
  if (sel.options.length === 0) {
    cities.forEach(c => {
      const opt = document.createElement("option");
      opt.value = c.city;
      opt.textContent = c.city;
      sel.appendChild(opt);
    });
    // Set initial selection
    selectedCity = cities.sort((a,b) => b.aqi - a.aqi)[0].city;
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
    regionHeading.style.cssText = "font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase; margin: 10px 0 4px; font-weight: 700; letter-spacing: 1px;";
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
      alert(`You can only track up to ${maxCities} cities at a time.`);
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
      <p style="margin-top: 1rem; color: var(--text-secondary);">Switching cities...</p>
    </div>
  `;
  
  // 4. Clean old global dropdown
  document.getElementById("globalCitySelector").innerHTML = "";
  
  // 5. Tell Backend
  await syncBackendCities(selectedArr);
  
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
    showCustomMsg(`You can only track up to ${maxCities} cities at a time. Deselect one first.`, true);
    return;
  }

  // Loading state
  btnAddCustom.disabled = true;
  btnAddCustom.textContent = "...";
  showCustomMsg("Locating...", false);
  
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
      if (sel.options.length === 0) {
        cities.forEach(c => {
          const opt = document.createElement("option");
          opt.value = c.city;
          opt.textContent = c.city;
          sel.appendChild(opt);
        });
        selectedCity = cities.sort((a,b) => b.aqi - a.aqi)[0].city;
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
