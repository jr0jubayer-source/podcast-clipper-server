const express = require("express");
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "";
const TMP_ROOT = process.env.TMP_DIR || "/tmp/clipper-jobs";
const MIN_SOURCE_SECONDS = Number(process.env.MIN_SOURCE_SECONDS || 600);
const CLIP_MAX_SECONDS = Number(process.env.CLIP_MAX_SECONDS || 90);
const CLIP_MIN_SECONDS = Number(process.env.CLIP_MIN_SECONDS || 30);
const MIN_CLIPS = Number(process.env.MIN_CLIPS || 4);
const MAX_CLIPS = Number(process.env.MAX_CLIPS || 5);
const JOB_TTL_MS = Number(process.env.JOB_TTL_MS || 1000 * 60 * 60);

fs.mkdirSync(TMP_ROOT, { recursive: true });
const jobs = new Map();

function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { ...opts });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (d) => (stdout += d.toString()));
    child.stderr?.on("data", (d) => (stderr += d.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${cmd} exited ${code}: ${stderr.slice(-2000)}`));
    });
  });
}

function requireApiKey(req, res, next) {
  if (!API_KEY) return next();
  const provided = req.get("x-api-key");
  if (provided !== API_KEY) return res.status(401).json({ error: "unauthorized" });
  next();
}

async function downloadVideo(url, destDir) {
  const outputTemplate = path.join(destDir, "source.%(ext)s");
  await run("yt-dlp", [
    "--no-playlist",
    "-f", "mp4/bestvideo[ext=mp4]+bestaudio[ext=m4a]/best",
    "--merge-output-format", "mp4",
    "-o", outputTemplate,
    url,
  ]);
  const files = fs.readdirSync(destDir).filter((f) => f.startsWith("source."));
  if (!files.length) throw new Error("yt-dlp did not produce an output file");
  return path.join(destDir, files[0]);
}

async function getDurationSeconds(filePath) {
  const { stdout } = await run("ffprobe", [
    "-v", "error",
    "-show_entries", "format=duration",
    "-of", "default=noprint_wrappers=1:nokey=1",
    filePath,
  ]);
  const seconds = parseFloat(stdout.trim());
  if (!Number.isFinite(seconds)) throw new Error("Could not read video duration");
  return seconds;
}

async function measureMeanVolume(filePath, startSec, durationSec) {
  const { stderr } = await run("ffmpeg", [
    "-ss", String(startSec),
    "-t", String(durationSec),
    "-i", filePath,
    "-af", "volumedetect",
    "-vn", "-f", "null", "-",
  ]);
  const match = stderr.match(/mean_volume:\s*(-?\d+(\.\d+)?)\s*dB/);
  return match ? parseFloat(match[1]) : -Infinity;
}

async function findHookWindows(filePath, totalDuration) {
  const windowSize = CLIP_MAX_SECONDS;
  const stride = Math.max(15, Math.floor(windowSize / 2));
  const candidates = [];

  for (let start = 0; start + CLIP_MIN_SECONDS <= totalDuration; start += stride) {
    const duration = Math.min(windowSize, totalDuration - start);
    if (duration < CLIP_MIN_SECONDS) continue;
    const meanVolume = await measureMeanVolume(filePath, start, duration);
    candidates.push({ start, duration, meanVolume });
  }

  candidates.sort((a, b) => b.meanVolume - a.meanVolume);

  const selected = [];
  const minGap = Math.max(60, windowSize);
  for (const c of candidates) {
    const overlaps = selected.some((s) => Math.abs(s.start - c.start) < minGap);
    if (!overlaps) selected.push(c);
    if (selected.length >= MAX_CLIPS) break;
  }

  if (selected.length < MIN_CLIPS) {
    for (const c of candidates) {
      if (selected.length >= MIN_CLIPS) break;
      if (!selected.includes(c)) selected.push(c);
    }
  }

  selected.sort((a, b) => a.start - b.start);
  return selected.slice(0, MAX_CLIPS);
}

async function extractClip(filePath, start, duration, outPath) {
  await run("ffmpeg", [
    "-ss", String(start),
    "-i", filePath,
    "-t", String(duration),
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-c:a", "aac",
    "-movflags", "+faststart",
    "-y", outPath,
  ]);
}

setInterval(() => {
  const now = Date.now();
  for (const [jobId, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) {
      fs.rm(job.dir, { recursive: true, force: true }, () => {});
      jobs.delete(jobId);
    }
  }
}, 5 * 60 * 1000).unref();

app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/jobs/:jobId", requireApiKey, (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "job not found" });
  const { dir, ...safe } = job;
  res.json({ jobId: req.params.jobId, ...safe });
});

app.get("/clips/:jobId/:filename", requireApiKey, (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "job not found" });
  const filePath = path.join(job.dir, req.params.filename);
  if (!filePath.startsWith(job.dir) || !fs.existsSync(filePath)) {
    return res.status(404).json({ error: "clip not found" });
  }
  res.sendFile(filePath);
});

app.post("/process", requireApiKey, async (req, res) => {
  const { url } = req.body || {};
  if (!url || typeof url !== "string") {
    return res.status(400).json({ error: "body must include a video 'url'" });
  }

  const jobId = crypto.randomBytes(8).toString("hex");
  const jobDir = path.join(TMP_ROOT, jobId);
  fs.mkdirSync(jobDir, { recursive: true });

  const job = { status: "processing", error: null, clips: [], createdAt: Date.now(), dir: jobDir };
  jobs.set(jobId, job);

  res.status(202).json({ jobId, status: "processing" });

  try {
    const sourcePath = await downloadVideo(url, jobDir);
    const totalDuration = await getDurationSeconds(sourcePath);

    if (totalDuration < MIN_SOURCE_SECONDS) {
      job.status = "skipped";
      job.error = `Source is ${Math.round(totalDuration)}s, below the ${MIN_SOURCE_SECONDS}s minimum`;
      return;
    }

    const windows = await findHookWindows(sourcePath, totalDuration);
    const clips = [];
    for (let i = 0; i < windows.length; i++) {
      const { start, duration, meanVolume } = windows[i];
      const filename = `clip_${String(i + 1).padStart(2, "0")}.mp4`;
      const outPath = path.join(jobDir, filename);
      await extractClip(sourcePath, start, duration, outPath);
      clips.push({
        index: i + 1,
        filename,
        startSeconds: Math.round(start),
        durationSeconds: Math.round(duration),
        loudnessScoreDb: Number.isFinite(meanVolume) ? Number(meanVolume.toFixed(1)) : null,
        url: `/clips/${jobId}/${filename}`,
      });
    }

    fs.rm(sourcePath, { force: true }, () => {});

    job.status = "done";
    job.clips = clips;
    job.sourceDurationSeconds = Math.round(totalDuration);
  } catch (err) {
    job.status = "error";
    job.error = String(err.message || err);
  }
});

app.listen(PORT, () => {
  console.log(`Podcast clipper server listening on :${PORT}`);
});
