const API_BASE = "/api";

let volumeChart;
let globalAnalyticsData = [];

// Initialize Chart
function initChart() {
  const ctx = document.getElementById("volumeChart").getContext("2d");
  volumeChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: Array.from({ length: 24 }, (_, i) => `${i}:00`),
      datasets: [],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: "Vehicle Volume" },
        },
        x: { title: { display: true, text: "Hour of Day" } },
      },
      plugins: {
        legend: { position: "bottom" },
      },
    },
  });

  document
    .getElementById("chart-junction-filter")
    .addEventListener("change", (e) => {
      updateChartDisplay(e.target.value);
    });
}

const colorMap = {
  J1_KOLLUPITIYA: "rgba(54, 162, 235, 0.7)",
  J2_BAMBALAPITIYA: "rgba(255, 99, 132, 0.7)",
  J3_BORELLA: "rgba(255, 159, 64, 0.7)",
  J4_NUGEGODA: "rgba(75, 192, 192, 0.7)",
};

function updateChartDisplay(filter) {
  if (!volumeChart) return;

  let junctions = [...new Set(globalAnalyticsData.map((d) => d.sensor_id))];
  if (filter !== "ALL") {
    junctions = [filter];
  }

  const datasets = junctions.map((j) => {
    const jData = globalAnalyticsData.filter((d) => d.sensor_id === j);
    const dataMap = new Map(
      jData.map((d) => [parseInt(d.hour, 10), Number(d.total_vehicles)]),
    );

    return {
      label: j,
      data: Array.from({ length: 24 }, (_, i) => dataMap.get(i) || 0),
      backgroundColor: colorMap[j] || "rgba(153, 102, 255, 0.7)",
      borderWidth: 1,
    };
  });

  volumeChart.data.datasets = datasets;
  volumeChart.update();
}

async function fetchLiveTraffic() {
  try {
    const res = await fetch(`${API_BASE}/live-traffic`);
    const data = await res.json();

    const container = document.getElementById("live-traffic-container");
    container.innerHTML = "";

    data.forEach((item) => {
      const isSlow = item.avg_speed < 15;
      container.innerHTML += `
                <div class="flex justify-between items-center p-3 bg-gray-50 border rounded ${isSlow ? "border-orange-300" : "border-gray-200"}">
                    <div>
                        <div class="font-bold text-gray-800">${item.sensor_id.replace("J", "Junction ")}</div>
                        <div class="text-xs text-gray-500">${new Date(item.event_time).toLocaleTimeString()}</div>
                    </div>
                    <div class="text-right">
                        <div class="font-semibold">${item.vehicle_count} <span class="text-xs font-normal">vehs</span></div>
                        <div class="${isSlow ? "text-orange-600 font-bold" : "text-green-600"} text-sm">${Number(item.avg_speed).toFixed(1)} km/h</div>
                    </div>
                </div>
            `;
    });
  } catch (e) {
    console.error(e);
  }
}

async function fetchAlerts() {
  try {
    const res = await fetch(`${API_BASE}/alerts`);
    const data = await res.json();

    const container = document.getElementById("alerts-container");
    if (data.length === 0) {
      container.innerHTML = '<li class="text-gray-500">No alerts yet...</li>';
      return;
    }

    container.innerHTML = data
      .map(
        (item) => `
            <li class="p-2 bg-red-50 border border-red-100 rounded text-red-800">
                <div class="flex justify-between items-start">
                    <span class="font-bold">${item.sensor_id}</span>
                </div>
                <div class="text-xs mt-1">
                    Spd: <b>${Number(item.avg_speed).toFixed(1)} kmh</b> | Vol: ${item.vehicle_count}
                </div>
                <div class="text-[10px] text-red-400 mt-1">${new Date(item.event_time).toLocaleString()}</div>
            </li>
        `,
      )
      .join("");
  } catch (e) {
    console.error(e);
  }
}

async function fetchAnalytics() {
  try {
    const res = await fetch(`${API_BASE}/analytics`);
    const data = await res.json();
    globalAnalyticsData = data;

    // Populate filter dropdown
    const select = document.getElementById("chart-junction-filter");
    const currentVal = select.value;
    const junctions = [...new Set(data.map((d) => d.sensor_id))];

    if (select.options.length === 1) {
      junctions.forEach((j) => {
        select.innerHTML += `<option value="${j}">${j}</option>`;
      });
    }

    updateChartDisplay(document.getElementById("chart-junction-filter").value);
  } catch (e) {
    console.error(e);
  }
}

async function fetchDeployment() {
  try {
    const res = await fetch(`${API_BASE}/police-deployment`);
    const data = await res.json();

    const tbody = document.getElementById("deployment-tbody");
    tbody.innerHTML = data
      .map((item) => {
        const isDeploy = item.deployment_score >= 6.0;
        return `
                <tr class="border-b hover:bg-gray-50">
                    <td class="py-2 px-3 font-medium">${item.junction}</td>
                    <td class="py-2 px-3 text-gray-600">${item.peak_hour}</td>
                    <td class="py-2 px-3">${item.peak_vehicle_count}</td>
                    <td class="py-2 px-3">${item.peak_avg_speed_kmh}</td>
                    <td class="py-2 px-3 text-red-500 font-semibold">${item.critical_alert_count}</td>
                    <td class="py-2 px-3 font-bold">${item.deployment_score}</td>
                    <td class="py-2 px-3">
                        <span class="px-2 py-1 rounded text-xs font-bold ${isDeploy ? "bg-red-100 text-red-700 blink" : "bg-green-100 text-green-700"}">
                            ${isDeploy ? "DEPLOY POLICE" : "MONITOR"}
                        </span>
                    </td>
                </tr>
            `;
      })
      .join("");
  } catch (e) {
    console.error(e);
  }
}

// Polling intervals
function startDashboard() {
  initChart();

  // Initial fetch
  fetchLiveTraffic();
  fetchAlerts();
  fetchAnalytics();
  fetchDeployment();

  // Refresh live and alerts every 3 seconds for "real-time" feel
  setInterval(() => {
    fetchLiveTraffic();
    fetchAlerts();
  }, 3000);

  // Refresh heavy batch outputs every 15 seconds
  setInterval(() => {
    fetchAnalytics();
    fetchDeployment();
  }, 15000);
}

document.addEventListener("DOMContentLoaded", startDashboard);
