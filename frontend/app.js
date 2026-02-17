const API_BASE = "http://127.0.0.1:8000";

const el = {
  apiStatus: document.getElementById("apiStatus"),
  fileInput: document.getElementById("fileInput"),
  dropzone: document.getElementById("dropzone"),
  uploadBtn: document.getElementById("uploadBtn"),
  datasetUrlInput: document.getElementById("datasetUrlInput"),
  uploadUrlBtn: document.getElementById("uploadUrlBtn"),
  uploadResult: document.getElementById("uploadResult"),
  totalJobs: document.getElementById("totalJobs"),
  rolesCount: document.getElementById("rolesCount"),
  countriesCount: document.getElementById("countriesCount"),
  roleSelect: document.getElementById("roleSelect"),
  countrySelect: document.getElementById("countrySelect"),
  skillSelect: document.getElementById("skillSelect"),
  kInput: document.getElementById("kInput"),
  loadTopBtn: document.getElementById("loadTopBtn"),
  loadRisingBtn: document.getElementById("loadRisingBtn"),
  loadTrendBtn: document.getElementById("loadTrendBtn"),
  topList: document.getElementById("topList"),
  risingList: document.getElementById("risingList"),
  trendCanvas: document.getElementById("trendCanvas")
};

async function api(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

function fillSelect(selectEl, items, placeholder = "Select") {
  selectEl.innerHTML = "";
  if (!items.length) {
    const option = new Option(`No ${placeholder.toLowerCase()} available`, "");
    selectEl.add(option);
    return;
  }
  for (const item of items) {
    selectEl.add(new Option(item, item));
  }
}

function renderSimpleList(listEl, rows, type) {
  listEl.innerHTML = "";
  if (!rows.length) {
    listEl.innerHTML = `<li><span>No data found</span><strong>0</strong></li>`;
    return;
  }

  for (const row of rows) {
    const li = document.createElement("li");
    if (type === "top") {
      li.innerHTML = `<span>${row.skill}</span><strong>${row.count}</strong>`;
    } else {
      li.innerHTML = `<span>${row.skill}</span><strong>+${row.growth}</strong>`;
    }
    listEl.appendChild(li);
  }
}

function drawTrend(trendRows) {
  const canvas = el.trendCanvas;
  const ctx = canvas.getContext("2d");
  const W = canvas.width;
  const H = canvas.height;

  ctx.clearRect(0, 0, W, H);

  ctx.strokeStyle = "rgba(168, 189, 219, 0.4)";
  ctx.beginPath();
  ctx.moveTo(60, 30);
  ctx.lineTo(60, H - 45);
  ctx.lineTo(W - 24, H - 45);
  ctx.stroke();

  if (!trendRows.length) {
    ctx.fillStyle = "#a8bddb";
    ctx.font = "600 24px Manrope";
    ctx.fillText("No trend data", W / 2 - 72, H / 2);
    return;
  }

  const max = Math.max(...trendRows.map((r) => r.count), 1);
  const stepX = (W - 100) / Math.max(trendRows.length - 1, 1);

  ctx.strokeStyle = "#2de2e6";
  ctx.lineWidth = 3;
  ctx.beginPath();

  trendRows.forEach((row, idx) => {
    const x = 60 + stepX * idx;
    const y = H - 45 - (row.count / max) * (H - 90);
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  trendRows.forEach((row, idx) => {
    const x = 60 + stepX * idx;
    const y = H - 45 - (row.count / max) * (H - 90);

    ctx.fillStyle = "#ffb84d";
    ctx.beginPath();
    ctx.arc(x, y, 5, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = "#cfe3ff";
    ctx.font = "600 12px Manrope";
    ctx.fillText(row.month, x - 22, H - 20);
    ctx.fillText(String(row.count), x - 6, y - 10);
  });
}

async function refreshDatasetInfo() {
  const info = await api("/dataset/info");
  el.totalJobs.textContent = info.total_jobs;
  el.rolesCount.textContent = info.available_roles.length;
  el.countriesCount.textContent = info.available_countries.length;

  fillSelect(el.roleSelect, info.available_roles, "roles");
  fillSelect(el.countrySelect, info.available_countries, "countries");

  await refreshSkills();
}

async function refreshSkills() {
  const role = el.roleSelect.value;
  const country = el.countrySelect.value;
  if (!role || !country) {
    fillSelect(el.skillSelect, [], "skills");
    return;
  }

  const params = new URLSearchParams({ role, country });
  const data = await api(`/dropdown/skills?${params.toString()}`);
  fillSelect(el.skillSelect, data.skills, "skills");
}

async function checkApi() {
  try {
    const health = await api("/health");
    el.apiStatus.textContent = `${health.service} is online`;
    el.apiStatus.style.borderColor = "rgba(66, 240, 178, 0.6)";
    el.apiStatus.style.background = "rgba(66, 240, 178, 0.18)";
  } catch {
    el.apiStatus.textContent = "API offline at http://127.0.0.1:8000";
    el.apiStatus.style.borderColor = "rgba(255, 77, 109, 0.6)";
    el.apiStatus.style.background = "rgba(255, 77, 109, 0.18)";
  }
}

async function uploadDataset() {
  const file = el.fileInput.files[0];
  if (!file) {
    el.uploadResult.textContent = "Please choose a CSV file first.";
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  el.uploadBtn.disabled = true;
  el.uploadBtn.textContent = "Uploading...";

  try {
    const res = await fetch(`${API_BASE}/jobs/upload`, {
      method: "POST",
      body: formData
    });
    const data = await res.json();
    el.uploadResult.textContent = JSON.stringify(data, null, 2);

    await refreshDatasetInfo();
    await runTopSkills();
    await runRisingSkills();
  } catch (err) {
    el.uploadResult.textContent = `Upload failed: ${err.message}`;
  } finally {
    el.uploadBtn.disabled = false;
    el.uploadBtn.textContent = "Upload & Replace Dataset";
  }
}

async function uploadDatasetFromUrl() {
  const url = (el.datasetUrlInput.value || "").trim();
  if (!url) {
    el.uploadResult.textContent = "Please paste a dataset URL first.";
    return;
  }

  el.uploadUrlBtn.disabled = true;
  el.uploadUrlBtn.textContent = "Loading...";

  try {
    const res = await fetch(`${API_BASE}/jobs/upload-from-url`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url })
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || `HTTP ${res.status}`);
    }
    el.uploadResult.textContent = JSON.stringify(data, null, 2);

    await refreshDatasetInfo();
    await runTopSkills();
    await runRisingSkills();
  } catch (err) {
    el.uploadResult.textContent = `URL upload failed: ${err.message}`;
  } finally {
    el.uploadUrlBtn.disabled = false;
    el.uploadUrlBtn.textContent = "Load From URL";
  }
}

async function runTopSkills() {
  const role = el.roleSelect.value;
  const country = el.countrySelect.value;
  const k = el.kInput.value || "10";
  const params = new URLSearchParams({ role, country, k });
  const data = await api(`/skills/top?${params.toString()}`);
  renderSimpleList(el.topList, data.top_skills || [], "top");
}

async function runRisingSkills() {
  const role = el.roleSelect.value;
  const country = el.countrySelect.value;
  const k = el.kInput.value || "10";
  const params = new URLSearchParams({ role, country, k });
  const data = await api(`/skills/rising?${params.toString()}`);
  renderSimpleList(el.risingList, data.rising_skills || [], "rising");
}

async function runTrend() {
  const skill = el.skillSelect.value;
  const role = el.roleSelect.value;
  const country = el.countrySelect.value;

  if (!skill) {
    drawTrend([]);
    return;
  }

  const params = new URLSearchParams({ skill, role, country });
  const data = await api(`/trends/skill?${params.toString()}`);
  drawTrend(data.trend || []);
}

function wireDnD() {
  ["dragenter", "dragover"].forEach((evtName) => {
    el.dropzone.addEventListener(evtName, (evt) => {
      evt.preventDefault();
      el.dropzone.classList.add("drag-over");
    });
  });

  ["dragleave", "drop"].forEach((evtName) => {
    el.dropzone.addEventListener(evtName, (evt) => {
      evt.preventDefault();
      el.dropzone.classList.remove("drag-over");
    });
  });

  el.dropzone.addEventListener("drop", (evt) => {
    if (evt.dataTransfer.files.length) {
      el.fileInput.files = evt.dataTransfer.files;
    }
  });
}

async function init() {
  wireDnD();
  await checkApi();
  await refreshDatasetInfo();
  await runTopSkills();
  await runRisingSkills();
  await runTrend();

  el.uploadBtn.addEventListener("click", uploadDataset);
  el.uploadUrlBtn.addEventListener("click", uploadDatasetFromUrl);
  el.roleSelect.addEventListener("change", async () => { await refreshSkills(); await runTopSkills(); await runRisingSkills(); await runTrend(); });
  el.countrySelect.addEventListener("change", async () => { await refreshSkills(); await runTopSkills(); await runRisingSkills(); await runTrend(); });
  el.skillSelect.addEventListener("change", runTrend);
  el.kInput.addEventListener("change", async () => { await runTopSkills(); await runRisingSkills(); });
  el.loadTopBtn.addEventListener("click", runTopSkills);
  el.loadRisingBtn.addEventListener("click", runRisingSkills);
  el.loadTrendBtn.addEventListener("click", runTrend);
}

init().catch((err) => {
  el.apiStatus.textContent = `Startup failed: ${err.message}`;
});
