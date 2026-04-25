async function postJson(url, body) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error(data?.detail || `Request failed: ${resp.status}`);
  }
  return data;
}

function renderJob(jobData) {
  const summary = document.getElementById("summary");
  const urlsEl = document.getElementById("urls");
  const logEl = document.getElementById("log");

  const job = jobData.job;
  const stats = job.stats || {};
  summary.textContent = `Job ${job.id} • ${job.type} • ${job.status} • found=${stats.discovered ?? 0} downloaded=${stats.downloaded ?? 0} uploaded=${stats.uploaded ?? 0} upload_failed=${stats.upload_failed ?? 0} skipped=${stats.skipped ?? 0} failed=${stats.failed ?? 0} filtered=${stats.filtered ?? 0}${job.error ? " • error=" + job.error : ""}`;

  urlsEl.innerHTML = "";
  for (const it of jobData.items || []) {
    const li = document.createElement("li");
    li.textContent = `${it.status} — ${it.url}`;
    urlsEl.appendChild(li);
  }

  const lines = (jobData.logs || []).map((l) => `[${new Date(l.ts * 1000).toLocaleTimeString()}] ${l.level.toUpperCase()}: ${l.message}`);
  logEl.textContent = lines.join("\n");
}

async function pollJob(jobId) {
  while (true) {
    const resp = await fetch(`/jobs/${jobId}`);
    if (!resp.ok) throw new Error(`Job fetch failed: ${resp.status}`);
    const jobData = await resp.json();
    renderJob(jobData);

    if (["completed", "failed"].includes(jobData.job.status)) return;
    await new Promise((r) => setTimeout(r, 1500));
  }
}

document.getElementById("jobForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const startBtn = document.getElementById("startBtn");
  const watchBtn = document.getElementById("watchBtn");
  startBtn.disabled = true;
  watchBtn.disabled = true;

  try {
    const page_url = document.getElementById("page_url").value;
    const max_videos = parseInt(document.getElementById("max_videos").value || "50", 10);
    const quality = document.getElementById("quality").value || "best";
    const output = document.getElementById("output").value || "downloads";
    const concurrency = parseInt(document.getElementById("concurrency").value || "2", 10);
    const interval_s = parseInt(document.getElementById("interval_s").value || "600", 10);
    const headless = document.getElementById("headless").checked;
    const dry_run = document.getElementById("dry_run").checked;
    const upload_to_drive = document.getElementById("upload_to_drive").checked;
    const gdrive_folder_id = document.getElementById("gdrive_folder_id").value || null;

    const { job_id } = await postJson("/fetch-and-download", {
      page_url,
      fetch: { output, max_videos, headless },
      download: { output, quality, dry_run, concurrency, upload_to_drive, gdrive_folder_id },
    });

    await pollJob(job_id);
  } catch (err) {
    document.getElementById("summary").textContent = `Error: ${err.message}`;
  } finally {
    startBtn.disabled = false;
    watchBtn.disabled = false;
  }
});

document.getElementById("watchBtn").addEventListener("click", async () => {
  const startBtn = document.getElementById("startBtn");
  const watchBtn = document.getElementById("watchBtn");
  startBtn.disabled = true;
  watchBtn.disabled = true;

  try {
    const page_url = document.getElementById("page_url").value;
    const max_videos = parseInt(document.getElementById("max_videos").value || "50", 10);
    const quality = document.getElementById("quality").value || "best";
    const output = document.getElementById("output").value || "downloads";
    const concurrency = parseInt(document.getElementById("concurrency").value || "2", 10);
    const interval_s = parseInt(document.getElementById("interval_s").value || "600", 10);
    const headless = document.getElementById("headless").checked;
    const dry_run = document.getElementById("dry_run").checked;
    const upload_to_drive = document.getElementById("upload_to_drive").checked;
    const gdrive_folder_id = document.getElementById("gdrive_folder_id").value || null;

    const { job_id } = await postJson("/watch", {
      page_url,
      fetch: { output, max_videos, headless },
      download: { output, quality, dry_run, concurrency, upload_to_drive, gdrive_folder_id },
      watch: { interval_s },
    });

    await pollJob(job_id);
  } catch (err) {
    document.getElementById("summary").textContent = `Error: ${err.message}`;
  } finally {
    startBtn.disabled = false;
    watchBtn.disabled = false;
  }
});
