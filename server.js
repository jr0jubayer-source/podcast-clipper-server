/**
 * Podcast Clipper Server
 * ----------------------
 * Runs on Render.com. Receives a video URL from the Cloudflare Worker,
 * downloads it with yt-dlp, picks 4-5 high-energy "hook" windows using
 * an audio-loudness heuristic, cuts them with ffmpeg, and exposes the
 * resulting clips over HTTP so the Worker can hand them to Gemini and
 * Telegram.
 *
 * This process does NOT do semantic "hook strength" analysis - that is
 * intentionally left to the Gemini step in the pipeline. This server's
 * job is fast, cheap, deterministic segment selection + cutting.
 */

const express = require("express");
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const app = express();
app.use(express.json());

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || ""; // shared secret with the Worker
const TMP_ROOT = process.env.TMP_DIR || "/tmp/clipper-jobs";
const MIN_SOURCE_SECONDS = Number(process.env.MIN_SOURCE_SECONDS || 15); // 15 sec floor - filters out near-nothing clips, allows normal Reels through
const CLIP_MAX_SECONDS = Number(process.env.CLIP_MAX_SECONDS || 90);
const CLIP_MIN_SECONDS = Number(process.env.CLIP_MIN_SECONDS || 30);
const MIN_CLIPS = Number(process.env.MIN_CLIPS || 4);
const MAX_CLIPS = Number(process.env.MAX_CLIPS || 5);
const LONG_FORM_SECONDS = Number(process.env.LONG_FORM_SECONDS || 600); // MIN_CLIPS is only enforced at/above this duration
const POSTER_THUMBNAIL_COUNT = 5; // always generated, spread across the full source, independent of clip count
const JOB_TTL_MS = Number(process.env.JOB_TTL_MS || 1000 * 60 * 60); // 1 hour

fs.mkdirSync(TMP_ROOT, { recursive: true });

// In-memory job registry. Render's disk is ephemeral, so jobs are
// meant to be consumed (downloaded by the Worker) quickly, then swept.
const jobs = new Map(); // jobId -> { status, error, clips, createdAt, dir }

// ---------------------------------------------------------------------
// Small process helper
// ---------------------------------------------------------------------
function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { ...opts });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let timer = null;
    if (opts.timeoutMs) {
      timer = setTimeout(() => {
        timedOut = true;
        child.kill("SIGKILL");
      }, opts.timeoutMs);
    }
    child.stdout?.on("data", (d) => (stdout += d.toString()));
    child.stderr?.on("data", (d) => (stderr += d.toString()));
    child.on("error", (err) => {
      if (timer) clearTimeout(timer);
      reject(err);
    });
    child.on("close", (code) => {
      if (timer) clearTimeout(timer);
      if (timedOut) return reject(new Error(`${cmd} timed out after ${opts.timeoutMs}ms`));
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${cmd} exited ${code}: ${stderr.slice(-2000)}`));
    });
  });
}

// ---------------------------------------------------------------------
// Auth middleware - simple shared-secret header from the Worker
// ---------------------------------------------------------------------
function requireApiKey(req, res, next) {
  if (!API_KEY) return next(); // no key configured -> open (dev only)
  const provided = req.get("x-api-key");
  if (provided !== API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
}

// ---------------------------------------------------------------------
// Step 1: download source video with yt-dlp
// ---------------------------------------------------------------------
async function downloadVideo(url, destDir) {
  const outputTemplate = path.join(destDir, "source.%(ext)s");
  await run("yt-dlp", [
    "--no-playlist",
    "-f",
    "mp4/bestvideo[ext=mp4]+bestaudio[ext=m4a]/best",
    "--merge-output-format",
    "mp4",
    "-o",
    outputTemplate,
    url,
  ]);
  const files = fs.readdirSync(destDir).filter((f) => f.startsWith("source."));
  if (!files.length) throw new Error("yt-dlp did not produce an output file");
  return path.join(destDir, files[0]);
}

// ---------------------------------------------------------------------
// Step 2: probe duration
// ---------------------------------------------------------------------
async function getDurationSeconds(filePath) {
  const { stdout } = await run("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ]);
  const seconds = parseFloat(stdout.trim());
  if (!Number.isFinite(seconds)) throw new Error("Could not read video duration");
  return seconds;
}

// ---------------------------------------------------------------------
// Step 3: loudness-based hook window detection
//
// Heuristic: slide a window of CLIP_MAX_SECONDS across the source,
// measure mean audio volume (dB) per window via ffmpeg's volumedetect,
// then take the loudest non-overlapping windows as candidate "hooks".
// Louder/denser audio energy tends to correlate with high-engagement
// moments (reactions, punchlines, raised voices) in podcast content.
// ---------------------------------------------------------------------
async function measureMeanVolume(filePath, startSec, durationSec) {
  const { stderr } = await run("ffmpeg", [
    "-ss",
    String(startSec),
    "-t",
    String(durationSec),
    "-i",
    filePath,
    "-af",
    "volumedetect",
    "-vn",
    "-f",
    "null",
    "-",
  ]);
  const match = stderr.match(/mean_volume:\s*(-?\d+(\.\d+)?)\s*dB/);
  return match ? parseFloat(match[1]) : -Infinity;
}

// Merges overlapping or near-adjacent windows into single windows before
// selection, so we don't waste extraction work on near-duplicate clips
// that share most of their content. Capped at CLIP_MAX_SECONDS so merges
// can't produce an oversized clip.
function mergeWindows(windows, gapThresholdSec = 5) {
  if (windows.length === 0) return [];
  const sorted = [...windows]
    .filter((w) => w.duration > 0)
    .sort((a, b) => a.start - b.start);
  if (sorted.length === 0) return [];

  const merged = [{ ...sorted[0] }];

  for (let i = 1; i < sorted.length; i++) {
    const prev = merged[merged.length - 1];
    const curr = sorted[i];
    const prevEnd = prev.start + prev.duration;
    const gap = curr.start - prevEnd;

    if (gap < gapThresholdSec) {
      // Overlapping or adjacent - always merge, then cap the result to
      // CLIP_MAX_SECONDS (trimming the tail) rather than skipping the
      // merge entirely. Since raw scan windows are already
      // CLIP_MAX_SECONDS wide, refusing to merge anything over that
      // length would make merging a no-op for the exact case it exists
      // to handle.
      const mergedEnd = Math.max(prevEnd, curr.start + curr.duration);
      prev.duration = Math.min(mergedEnd - prev.start, CLIP_MAX_SECONDS);
      prev.meanVolume = Math.max(
        prev.meanVolume ?? -Infinity,
        curr.meanVolume ?? -Infinity
      );
    } else {
      merged.push({ ...curr });
    }
  }
  return merged;
}

async function findHookWindows(filePath, totalDuration) {
  // Short source (typical for Reels/short clips): just use the whole
  // thing as one clip instead of skipping it or hunting for sub-windows.
  if (totalDuration <= CLIP_MAX_SECONDS) {
    return [{ start: 0, duration: totalDuration, meanVolume: null }];
  }

  const windowSize = CLIP_MAX_SECONDS;
  const stride = Math.max(15, Math.floor(windowSize / 2)); // 50% overlap scan
  const rawCandidates = [];

  for (let start = 0; start + CLIP_MIN_SECONDS <= totalDuration; start += stride) {
    const duration = Math.min(windowSize, totalDuration - start);
    if (duration < CLIP_MIN_SECONDS) continue;
    const meanVolume = await measureMeanVolume(filePath, start, duration);
    rawCandidates.push({ start, duration, meanVolume });
  }

  // Merge overlapping/adjacent windows first so selection works on
  // distinct, non-redundant candidates instead of near-duplicates.
  const candidates = mergeWindows(rawCandidates);

  // Sort loudest first
  candidates.sort((a, b) => b.meanVolume - a.meanVolume);

  // Greedily select non-overlapping windows, spaced out across the video
  const selected = [];
  const minGap = Math.max(60, windowSize); // avoid picking near-duplicate moments
  for (const c of candidates) {
    const overlaps = selected.some(
      (s) => Math.abs(s.start - c.start) < minGap
    );
    if (!overlaps) selected.push(c);
    if (selected.length >= MAX_CLIPS) break;
  }

  // If we came up short, only force extra (possibly overlapping) clips
  // for genuinely long-form content. For shorter videos, honest natural
  // clip count (even just 1-3) is correct - forcing 4 clips out of a
  // ~3-minute video just produces near-duplicate overlapping content.
  if (totalDuration >= LONG_FORM_SECONDS && selected.length < MIN_CLIPS) {
    for (const c of candidates) {
      if (selected.length >= MIN_CLIPS) break;
      if (!selected.includes(c)) selected.push(c);
    }
  }

  selected.sort((a, b) => a.start - b.start);
  return selected.slice(0, MAX_CLIPS);
}

// ---------------------------------------------------------------------
// Step 4: cut clips with ffmpeg (re-encode for frame-accurate cuts)
// ---------------------------------------------------------------------
// Tries a fast, lossless stream copy first (works when the seek point
// lands cleanly on a keyframe - near-instant, no re-encode). Falls back
// to a full re-encode with the fastest usable preset if that fails or
// produces a suspiciously small/invalid file.
async function extractClip(filePath, start, duration, outPath) {
  try {
    await run(
      "ffmpeg",
      [
        "-ss",
        String(start),
        "-i",
        filePath,
        "-t",
        String(duration),
        "-c",
        "copy",
        "-avoid_negative_ts",
        "make_zero",
        "-y",
        outPath,
      ],
      { timeoutMs: 15000 }
    );
    const stat = fs.existsSync(outPath) ? fs.statSync(outPath) : null;
    if (stat && stat.size > 1024) return; // copy succeeded, done
  } catch (_) {
    // keyframe misalignment or other failure - fall through to re-encode
  }

  await run("ffmpeg", [
    "-ss",
    String(start),
    "-i",
    filePath,
    "-t",
    String(duration),
    "-c:v",
    "libx264",
    "-preset",
    "ultrafast",
    "-crf",
    "23",
    "-c:a",
    "aac",
    "-movflags",
    "+faststart",
    "-y",
    outPath,
  ]);
}

// A single mid-clip JPEG frame, used as a fallback input for image-only
// AI analysis (e.g. a free OpenRouter vision model) when full video
// analysis via Gemini is unavailable.
async function extractThumbnail(clipPath, clipDuration, outPath) {
  const midpoint = Math.max(0, clipDuration / 2);
  await run("ffmpeg", [
    "-ss",
    String(midpoint),
    "-i",
    clipPath,
    "-frames:v",
    "1",
    "-q:v",
    "2",
    "-y",
    outPath,
  ]);
}

// Grabs a single frame from the ORIGINAL source at an absolute timestamp.
async function extractFrameAt(sourcePath, atSeconds, outPath) {
  await run("ffmpeg", [
    "-ss",
    String(atSeconds),
    "-i",
    sourcePath,
    "-frames:v",
    "1",
    "-q:v",
    "2",
    "-y",
    outPath,
  ]);
}

// Always produces POSTER_THUMBNAIL_COUNT (5) thumbnail options spread
// evenly across the full source video, independent of how many actual
// video clips were extracted. This gives a real choice of thumbnail
// images for posting even when a short video only yields 1-2 clips.
async function generatePosterThumbnails(sourcePath, totalDuration, jobDir, jobId) {
  const fractions = [0.1, 0.3, 0.5, 0.7, 0.9].slice(0, POSTER_THUMBNAIL_COUNT);
  const thumbnails = [];
  for (let i = 0; i < fractions.length; i++) {
    const atSeconds = Math.min(
      Math.max(0, totalDuration - 0.5),
      totalDuration * fractions[i]
    );
    const filename = `poster_${i + 1}.jpg`;
    const outPath = path.join(jobDir, filename);
    await extractFrameAt(sourcePath, atSeconds, outPath);
    thumbnails.push({
      index: i + 1,
      timestampSeconds: Math.round(atSeconds),
      filename,
      url: `/clips/${jobId}/${filename}`,
    });
  }
  return thumbnails;
}

// ---------------------------------------------------------------------
// Cleanup old jobs periodically (Render disk is ephemeral but finite)
// ---------------------------------------------------------------------
setInterval(() => {
  const now = Date.now();
  for (const [jobId, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) {
      fs.rm(job.dir, { recursive: true, force: true }, () => {});
      jobs.delete(jobId);
    }
  }
}, 5 * 60 * 1000).unref();

// ---------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------
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

  // Respond immediately with the job id; processing continues async.
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

      const thumbFilename = `clip_${String(i + 1).padStart(2, "0")}_thumb.jpg`;
      const thumbPath = path.join(jobDir, thumbFilename);
      await extractThumbnail(outPath, duration, thumbPath);

      clips.push({
        index: i + 1,
        filename,
        thumbnailFilename: thumbFilename,
        startSeconds: Math.round(start),
        durationSeconds: Math.round(duration),
        loudnessScoreDb: Number.isFinite(meanVolume) ? Number(meanVolume.toFixed(1)) : null,
        url: `/clips/${jobId}/${filename}`,
        thumbnailUrl: `/clips/${jobId}/${thumbFilename}`,
      });
    }

    // Always generate 5 poster thumbnail options from the full source,
    // independent of clip count - even a 2-clip short video gets a real
    // choice of thumbnail images for posting.
    const posterThumbnails = await generatePosterThumbnails(
      sourcePath,
      totalDuration,
      jobDir,
      jobId
    );

    // Free up space: the full source is no longer needed once cut
    fs.rm(sourcePath, { force: true }, () => {});

    job.status = "done";
    job.clips = clips;
    job.posterThumbnails = posterThumbnails;
    job.sourceDurationSeconds = Math.round(totalDuration);
  } catch (err) {
    job.status = "error";
    job.error = String(err.message || err);
  }
});

// =====================================================================
// FILE MODE: process a raw video upload with NO yt-dlp and NO Facebook
// contact. The phone (residential IP) downloads the reel, then the
// Worker streams the bytes here. Everything downstream is identical.
// =====================================================================
app.post("/process-file", requireApiKey, express.raw({ type: "*/*", limit: "300mb" }), async (req, res) => {
  if (!Buffer.isBuffer(req.body) || req.body.length < 1024) {
    return res.status(400).json({ error: "body must be the raw video bytes" });
  }

  const jobId = crypto.randomBytes(8).toString("hex");
  const jobDir = path.join(TMP_ROOT, jobId);
  fs.mkdirSync(jobDir, { recursive: true });

  const sourcePath = path.join(jobDir, "source.mp4");
  fs.writeFileSync(sourcePath, req.body);

  const job = { status: "processing", error: null, clips: [], createdAt: Date.now(), dir: jobDir };
  jobs.set(jobId, job);

  res.status(202).json({ jobId, status: "processing" });

  try {
    const totalDuration = await getDurationSeconds(sourcePath);

    if (totalDuration < MIN_SOURCE_SECONDS) {
      job.status = "skipped";
      job.error = "Source is " + Math.round(totalDuration) + "s, below the " + MIN_SOURCE_SECONDS + "s minimum";
      return;
    }

    const windows = await findHookWindows(sourcePath, totalDuration);

    const clips = [];
    for (let i = 0; i < windows.length; i++) {
      const w = windows[i];
      const filename = "clip_" + String(i + 1).padStart(2, "0") + ".mp4";
      const outPath = path.join(jobDir, filename);
      await extractClip(sourcePath, w.start, w.duration, outPath);

      const thumbFilename = "clip_" + String(i + 1).padStart(2, "0") + "_thumb.jpg";
      const thumbPath = path.join(jobDir, thumbFilename);
      await extractThumbnail(outPath, w.duration, thumbPath);

      clips.push({
        index: i + 1,
        filename: filename,
        thumbnailFilename: thumbFilename,
        startSeconds: Math.round(w.start),
        durationSeconds: Math.round(w.duration),
        loudnessScoreDb: Number.isFinite(w.meanVolume) ? Number(w.meanVolume.toFixed(1)) : null,
        url: "/clips/" + jobId + "/" + filename,
        thumbnailUrl: "/clips/" + jobId + "/" + thumbFilename,
      });
    }

    const posterThumbnails = await generatePosterThumbnails(
      sourcePath,
      totalDuration,
      jobDir,
      jobId
    );

    fs.rm(sourcePath, { force: true }, () => {});

    job.status = "done";
    job.clips = clips;
    job.posterThumbnails = posterThumbnails;
    job.sourceDurationSeconds = Math.round(totalDuration);
  } catch (err) {
    job.status = "error";
    job.error = String(err.message || err);
  }
});

app.listen(PORT, () => {
  console.log(`Podcast clipper server listening on :${PORT}`);
});
