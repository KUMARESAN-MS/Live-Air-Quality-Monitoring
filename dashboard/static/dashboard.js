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
  
  // Sort cities by severity automatically for overview
  const sorted = [...cities].sort((a, b) => b.aqi - a.aqi);

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
    card.className = `city-card ${city.css_class}`;
    card.innerHTML = `
      <div class="card-header">
        <span class="city-name">${city.city}</span>
        <span class="trend">${city.trend}</span>
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

  // --- Intel Grid ---
  // AI Insight
  document.getElementById("insightText").textContent = cityData.insight || "Waiting for data...";
  
  // Prediction
  document.getElementById("predValue").textContent = cityData.next_hour_aqi;
  document.getElementById("predValue").className = `focus-text ${cityData.next_hour_css}`;
  document.getElementById("predLabel").textContent = " " + cityData.next_hour_label;
  
  // Traffic
  const t = cityData.traffic || "Medium";
  const trafficText = document.getElementById("trafficText");
  const trafficIndicator = document.getElementById("trafficIndicator");
  trafficText.textContent = t;
  trafficText.className = `focus-text traffic-${t.toLowerCase()}`;
  trafficIndicator.className = `traffic-dot traffic-${t.toLowerCase()}-dot`;
  
  // Alerts
  const alertCard = document.getElementById("cardAlerts");
  const alertEl = document.getElementById("alertText");
  if(cityData.alert) {
    alertEl.textContent = cityData.alert;
    alertCard.classList.add("has-alert");
  } else {
    alertEl.textContent = "No active alerts";
    alertCard.classList.remove("has-alert");
  }
  
  // Health Advisory
  document.getElementById("healthText").textContent = cityData.health_advisory || "—";
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
  fetch("/toggle_demo", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ demo_mode: e.target.checked })
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

// Boot
initChart();
