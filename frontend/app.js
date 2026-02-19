const summaryBox = document.getElementById('summaryBox');
const agentBox = document.getElementById('agentBox');
const runsBox = document.getElementById('runsBox');

function show(target, data) {
  target.textContent = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
}

async function uploadDataset() {
  const fileInput = document.getElementById('fileInput');
  const file = fileInput.files?.[0];
  if (!file) {
    show(summaryBox, 'Please choose a dataset file first.');
    return;
  }

  const formData = new FormData();
  formData.append('file', file);

  show(summaryBox, 'Uploading and profiling dataset...');
  try {
    const response = await fetch('/api/upload', { method: 'POST', body: formData });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || 'Upload failed');
    show(summaryBox, data);
  } catch (error) {
    show(summaryBox, `Error: ${error.message}`);
  }
}

async function runAgent() {
  const query = document.getElementById('queryInput').value.trim();
  if (!query) {
    show(agentBox, 'Enter a natural-language query first.');
    return;
  }

  show(agentBox, 'Running agent workflow...');
  try {
    const response = await fetch('/api/agent/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || 'Agent execution failed');
    show(agentBox, data);
  } catch (error) {
    show(agentBox, `Error: ${error.message}`);
  }
}

async function loadRuns() {
  show(runsBox, 'Loading recent runs...');
  try {
    const response = await fetch('/api/agent/runs');
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || 'Could not load runs');
    show(runsBox, data);
  } catch (error) {
    show(runsBox, `Error: ${error.message}`);
  }
}

document.getElementById('uploadBtn').addEventListener('click', uploadDataset);
document.getElementById('runBtn').addEventListener('click', runAgent);
document.getElementById('refreshRunsBtn').addEventListener('click', loadRuns);
loadRuns();
