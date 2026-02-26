const state = {
  chart: null,
  latestResponse: null,
  selectedGcid: "",
  selectedDays: 14,
};

const form = document.getElementById("search-form");
const statusEl = document.getElementById("status");
const tableEl = document.getElementById("daily-table");
const detailsEl = document.getElementById("experiment-details");
const chartCanvas = document.getElementById("daily-chart");

const apiBase = `${window.location.origin}/api`;
const FUNNY_LOADING_WORDS = [
  "Combobulating",
  "Knowledge-seeking",
  "Experiment-whispering",
  "Variant-vibing",
  "Timeline-tunneling",
  "Signal-sniffing",
  "Data-sherlocking",
  "Hypothesis-hunting",
  "Insight-foraging",
  "Pattern-puzzling",
];

let loadingTimer = null;

function setStatus(message, isError = false) {
  statusEl.classList.remove("status-loading");
  statusEl.innerHTML = "";
  const text = document.createElement("span");
  text.textContent = message;
  statusEl.appendChild(text);
  statusEl.style.color = isError ? "#b91c1c" : "#111827";
}

function setLoading(message) {
  if (loadingTimer) {
    clearInterval(loadingTimer);
    loadingTimer = null;
  }
  statusEl.classList.add("status-loading");
  statusEl.style.color = "#111827";
  statusEl.innerHTML = "";
  const spinner = document.createElement("span");
  spinner.className = "spinner";
  spinner.setAttribute("aria-hidden", "true");
  const text = document.createElement("span");
  text.textContent = message;
  statusEl.append(spinner, text);
}

function startFunnySearchLoading() {
  if (loadingTimer) {
    clearInterval(loadingTimer);
    loadingTimer = null;
  }
  const words = [...FUNNY_LOADING_WORDS].sort(() => Math.random() - 0.5);
  let idx = 0;
  const nextMessage = () => {
    const word = words[idx % words.length];
    setLoading(`${word}...`);
    idx += 1;
  };
  nextMessage();
  loadingTimer = setInterval(nextMessage, 900);
}

function stopFunnySearchLoading() {
  if (loadingTimer) {
    clearInterval(loadingTimer);
    loadingTimer = null;
  }
}

function renderTable(data) {
  if (!data.daily.length) {
    tableEl.innerHTML = "<p class='muted'>No participation data found for this window.</p>";
    return;
  }

  const rows = data.daily
    .map((dayEntry) => {
      const experiments = dayEntry.experiments
        .map(
          (exp) =>
            `<div class="experiment-row">
              <button
                type="button"
                class="experiment-chip"
                data-exp-open="${exp.experiment_id}"
                title="Show experiment details"
                aria-label="Show experiment details for ${exp.experiment_id}"
              >
                <span class="experiment-corner">i</span>
                <span class="experiment-label">${exp.experiment_id}</span>
              </button>
            </div>`
        )
        .join("");
      return `<tr>
        <td>${dayEntry.day}</td>
        <td>${dayEntry.count}</td>
        <td>${experiments}</td>
      </tr>`;
    })
    .join("");

  tableEl.innerHTML = `<table>
    <thead><tr><th>Day</th><th>Count</th><th>Experiments</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

tableEl.addEventListener("click", async (event) => {
  const trigger = event.target.closest("[data-exp-open]");
  if (!trigger) return;
  const experimentId = trigger.getAttribute("data-exp-open");
  if (!experimentId) return;
  await loadExperimentDetails(experimentId);
});

function renderChart(data) {
  const labels = data.daily.map((entry) => entry.day);
  const counts = data.daily.map((entry) => entry.count);

  if (state.chart) {
    state.chart.destroy();
  }

  state.chart = new window.Chart(chartCanvas, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Experiments per day",
          data: counts,
          backgroundColor: "#60a5fa",
        },
      ],
    },
    options: {
      onClick: async (_, elements) => {
        if (!elements.length || !state.latestResponse) return;
        const index = elements[0].index;
        const dayEntry = state.latestResponse.daily[index];
        const firstExperiment = dayEntry?.experiments?.[0];
        if (firstExperiment) {
          await loadExperimentDetails(firstExperiment.experiment_id);
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          precision: 0,
        },
      },
    },
  });
}

async function loadExperimentDetails(experimentId) {
  if (!state.selectedGcid) return;

  setLoading(`Loading details for experiment ${experimentId}...`);
  detailsEl.classList.remove("muted");

  try {
    const params = new URLSearchParams({
      gcid: state.selectedGcid,
      days: String(state.selectedDays),
    });
    const response = await fetch(`${apiBase}/experiments/${encodeURIComponent(experimentId)}?${params}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Could not load experiment details");
    }

    detailsEl.innerHTML = `
      <p><strong>ID:</strong> ${payload.experiment_id}</p>
      <p><strong>Name:</strong> ${payload.experiment_name}</p>
      <p><strong>Start date:</strong> ${payload.start_date ?? "N/A"}</p>
      <p><strong>End date:</strong> ${payload.end_date ?? "N/A"}</p>
      <p><strong>Running days:</strong> ${payload.running_days}</p>
      <p><strong>Overlapping experiments:</strong> ${payload.overlap_experiment_count}</p>
      <p><strong>Variants:</strong> ${payload.variants.join(", ") || "None"}</p>
    `;
    setStatus("Experiment details loaded.");
  } catch (error) {
    detailsEl.textContent = error.message;
    setStatus(error.message, true);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const gcid = document.getElementById("gcid").value.trim();
  const days = Number(document.getElementById("days").value || 14);
  if (!gcid) {
    setStatus("GCID is required.", true);
    return;
  }

  startFunnySearchLoading();
  detailsEl.textContent = "Click a bar or experiment ID from the table to load details.";
  detailsEl.classList.add("muted");

  try {
    const response = await fetch(`${apiBase}/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ gcid, days }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Search failed");
    }

    state.latestResponse = payload;
    state.selectedGcid = gcid;
    state.selectedDays = days;
    renderChart(payload);
    renderTable(payload);
    stopFunnySearchLoading();
    setStatus(`Found ${payload.daily.length} active day(s).`);
  } catch (error) {
    stopFunnySearchLoading();
    if (state.chart) {
      state.chart.destroy();
      state.chart = null;
    }
    tableEl.innerHTML = "";
    setStatus(error.message, true);
  }
});
