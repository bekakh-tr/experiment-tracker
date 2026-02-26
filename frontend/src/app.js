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
const chartLoadingOverlay = document.getElementById("chart-loading-overlay");
const experimentFilterInput = document.getElementById("experiment-filter");
const searchButton = document.getElementById("search-button");
const searchButtonLabel = document.getElementById("search-button-label");
const searchButtonSpinner = document.getElementById("search-button-spinner");

const apiBase = `${window.location.origin}/api`;
const SEARCH_LABEL = "Search";
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
  "Query-juggling",
  "Metric-munching",
  "Data-divining",
  "Cluster-cuddling",
  "Dashboard-doodling",
  "Pixel-polishing",
  "Schema-surfing",
  "Signal-summoning",
  "Event-entangling",
  "Fact-fishing",
  "Pattern-peeking",
  "Filter-finessing",
  "Null-banishing",
  "Result-rousing",
  "Timeline-tickling",
  "Insight-igniting",
  "Graph-grooming",
  "Latency-lassoing",
  "Payload-polishing",
  "Variant-vaulting",
];

let loadingTimer = null;

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#b91c1c" : "#111827";
}

function setChartLoading(isLoading) {
  chartLoadingOverlay.classList.toggle("hidden", !isLoading);
}

function shuffleWords(words) {
  const copy = [...words];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

function setSearchButtonLoading(isLoading, message = SEARCH_LABEL) {
  searchButton.disabled = isLoading;
  searchButton.classList.toggle("button-loading", isLoading);
  searchButtonSpinner.classList.toggle("hidden", !isLoading);
  searchButtonLabel.textContent = message;
}

function startFunnySearchLoading() {
  if (loadingTimer) {
    clearInterval(loadingTimer);
    loadingTimer = null;
  }
  const words = shuffleWords(FUNNY_LOADING_WORDS);
  let idx = 0;
  const nextMessage = () => {
    const word = words[idx % words.length];
    setSearchButtonLoading(true, `${word}...`);
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
  setSearchButtonLoading(false, SEARCH_LABEL);
}

function applyExperimentHighlight() {
  const query = (experimentFilterInput.value || "").trim().toLowerCase();
  tableEl.querySelectorAll(".experiment-chip").forEach((chip) => {
    const expId = chip.getAttribute("data-exp-open") || "";
    const isMatch = query && expId.toLowerCase().includes(query);
    chip.classList.toggle("experiment-chip-highlight", Boolean(isMatch));
  });
}

function renderTable(data) {
  if (!data.daily.length) {
    tableEl.innerHTML = "<p class='muted'>No participation data found for this window.</p>";
    return;
  }

  const dayBlocks = data.daily
    .map((dayEntry) => {
      const experiments = [...dayEntry.experiments]
        .sort((a, b) => a.experiment_id.localeCompare(b.experiment_id))
        .map((exp) => {
          const variantsTitle = Array.isArray(exp.variants) && exp.variants.length
            ? `Variants: ${exp.variants.join(", ")}`
            : "Variants: not available yet";
          return `<button
              type="button"
              class="experiment-chip"
              data-exp-open="${exp.experiment_id}"
              title="${variantsTitle}"
              aria-label="Show experiment details for ${exp.experiment_id}"
            >
              <span class="experiment-corner">i</span>
              <span class="experiment-label">${exp.experiment_id}</span>
            </button>`;
        })
        .join("");

      return `<article class="day-card">
        <header class="day-card-header">
          <span class="day-card-date">${dayEntry.day}</span>
          <span class="day-card-count">${dayEntry.count} experiment(s)</span>
        </header>
        <div class="day-card-experiments">${experiments}</div>
      </article>`;
    })
    .join("");

  tableEl.innerHTML = `<div class="day-card-list">${dayBlocks}</div>`;
  applyExperimentHighlight();
}

tableEl.addEventListener("click", async (event) => {
  const trigger = event.target.closest("[data-exp-open]");
  if (!trigger) return;
  const experimentId = trigger.getAttribute("data-exp-open");
  if (!experimentId) return;
  await loadExperimentDetails(experimentId, trigger);
});

experimentFilterInput.addEventListener("input", applyExperimentHighlight);

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
          const triggerEl = tableEl.querySelector(`[data-exp-open="${firstExperiment.experiment_id}"]`);
          await loadExperimentDetails(firstExperiment.experiment_id, triggerEl);
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

async function loadExperimentDetails(experimentId, triggerEl = null) {
  if (!state.selectedGcid) return;

  setStatus(`Loading details for experiment ${experimentId}...`);
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

    if (triggerEl) {
      const updatedTitle = payload.variants.length
        ? `Variants: ${payload.variants.join(", ")}`
        : "Variants: none found";
      triggerEl.setAttribute("title", updatedTitle);
    }
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
  setChartLoading(true);
  setStatus("");
  detailsEl.textContent = "Click an experiment chip or chart bar to load details.";
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
    setStatus(`Found ${payload.daily.length} active day(s).`);
  } catch (error) {
    if (state.chart) {
      state.chart.destroy();
      state.chart = null;
    }
    tableEl.innerHTML = "";
    setStatus(error.message, true);
  } finally {
    stopFunnySearchLoading();
    setChartLoading(false);
  }
});
