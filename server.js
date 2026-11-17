import express from 'express';
import { spawn } from 'node:child_process';
import { createWriteStream, promises as fs } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { randomUUID } from 'node:crypto';

const PORT = Number(process.env.PORT || 8080);
const WORKER_TOKEN = String(process.env.WORKER_TOKEN || '').trim();
const UPLOADER_URL = 'https://uploader.pandavideo.com/files/';
const API_BASE = 'https://api-v2.pandavideo.com.br';

const app = express();
app.use(express.json({ limit: '2mb' }));

let busy = false;
let currentJobId = null;

function auth(req, res, next) {
  const header = String(req.headers.authorization || '');
  const token = header.startsWith('Bearer ') ? header.slice(7).trim() : '';
  if (!WORKER_TOKEN || token !== WORKER_TOKEN) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  return next();
}

async function callback(url, token, payload) {
  if (!url) return;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token || ''}`,
      },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      console.error('callback HTTP', res.status, payload?.stage || '', text.slice(0, 200));
    } else {
      console.log('callback ok', payload?.status, payload?.stage, payload?.progress);
    }
  } catch (err) {
    console.error('callback failed', err?.message || err);
  }
}

async function pandaGet(apiKey, path) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { Authorization: apiKey, Accept: 'application/json' },
  });
  const text = await res.text();
  let data = null;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!res.ok) {
    throw new Error(`Panda GET ${path} HTTP ${res.status}: ${text.slice(0, 300)}`);
  }
  return data;
}

async function findVideoByExternalId(apiKey, externalId) {
  const id = String(externalId || '').toLowerCase();
  try {
    const direct = await pandaGet(apiKey, `/videos/${encodeURIComponent(id)}`);
    if (direct && (String(direct.video_external_id || '').toLowerCase() === id || String(direct.id || '').toLowerCase() === id)) {
      return normalizeVideo(direct);
    }
  } catch {
    // continue listing
  }

  for (let page = 1; page <= 50; page++) {
    const data = await pandaGet(apiKey, `/videos?page=${page}&limit=100`);
    const videos = Array.isArray(data?.videos) ? data.videos : [];
    for (const video of videos) {
      const ext = String(video.video_external_id || '').toLowerCase();
      if (ext === id) return normalizeVideo(video);
    }
    if (videos.length < 100) break;
  }
  return null;
}

function normalizeVideo(video) {
  return {
    id: String(video.id || ''),
    video_external_id: String(video.video_external_id || video.id || '').toLowerCase(),
    status: String(video.status || ''),
    library_id: String(video.library_id || ''),
    title: String(video.title || ''),
  };
}

async function downloadVideo(apiKey, downloadHost, internalId, destPath) {
  const url = `${downloadHost.replace(/\/$/, '')}/videos/${encodeURIComponent(internalId)}/auto/download`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: apiKey, Accept: '*/*' },
  });
  if (!res.ok || !res.body) {
    const body = await res.text().catch(() => '');
    throw new Error(`Download HTTP ${res.status}: ${body.slice(0, 300)}`);
  }
  await pipeline(res.body, createWriteStream(destPath));
  const st = await fs.stat(destPath);
  if (st.size < 1024) throw new Error('Download result too small');
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 8000) stderr = stderr.slice(-8000);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exit ${code}: ${stderr.slice(-500)}`));
    });
  });
}

async function uploadTus(apiKey, filePath, filename, folderId, videoId) {
  const buf = await fs.readFile(filePath);
  const meta = [
    `authorization ${Buffer.from(apiKey).toString('base64')}`,
    `filename ${Buffer.from(filename).toString('base64')}`,
    `video_id ${Buffer.from(videoId).toString('base64')}`,
  ];
  if (folderId) {
    meta.push(`folder_id ${Buffer.from(folderId).toString('base64')}`);
  }

  const res = await fetch(UPLOADER_URL, {
    method: 'POST',
    headers: {
      'Tus-Resumable': '1.0.0',
      'Upload-Length': String(buf.length),
      'Content-Type': 'application/offset+octet-stream',
      'Upload-Metadata': meta.join(', '),
      'Content-Length': String(buf.length),
    },
    body: buf,
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Upload HTTP ${res.status}: ${text.slice(-400)}`);
  }

  return { video_id: videoId, location: res.headers.get('location') || '' };
}

async function waitConverted(apiKey, videoId, onTick) {
  for (let i = 0; i < 60; i++) {
    if (onTick) onTick(i, 60);
    const data = await pandaGet(apiKey, `/videos/${encodeURIComponent(videoId)}`);
    const video = normalizeVideo(data);
    const status = video.status.toUpperCase();
    if (status === 'CONVERTED' && video.video_external_id) return video;
    if (['FAILED', 'BLOCKED', 'DELETING'].includes(status)) {
      throw new Error(`Conversão falhou: ${status}`);
    }
    await new Promise((r) => setTimeout(r, 5000));
  }
  throw new Error('Timeout aguardando conversão na Panda');
}

async function processJob(job) {
  const workDir = join(tmpdir(), `abiud-tease-${job.job_id || randomUUID()}`);
  await fs.mkdir(workDir, { recursive: true });
  const src = join(workDir, 'source.mp4');
  const clip = join(workDir, 'tease.mp4');
  const notify = (stage, progress, message = '') =>
    callback(job.callback_url, job.callback_token, {
      // job_id omitido nos updates intermediários: evita mismatch se o WP
      // reenfileirou e trocou o meta enquanto este job ainda roda.
      post_id: job.post_id,
      status: 'processing',
      stage,
      progress,
      message,
    });

  try {
    await notify('resolve', 5, 'Resolvendo vídeo');
    const video = await findVideoByExternalId(job.panda_api_key, job.source_external_id);
    if (!video?.id) throw new Error(`Vídeo não encontrado: ${job.source_external_id}`);

    await notify('download', 15, 'Baixando');
    await downloadVideo(job.panda_api_key, job.download_host, video.id, src);
    await notify('download', 40, 'Download concluído');

    const start = Math.max(0, Number(job.clip_start) || 0);
    const end = Math.max(start + 10, Number(job.clip_end) || start + 30);
    await notify('cut', 45, `Recortando ${start}s–${end}s`);
    await runFfmpeg([
      '-y',
      '-i', src,
      '-ss', String(start),
      '-to', String(end),
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-crf', '23',
      '-c:a', 'aac',
      '-b:a', '128k',
      '-movflags', '+faststart',
      clip,
    ]);
    await notify('cut', 60, 'Recorte concluído');

    const newId = randomUUID();
    const filename = `tease-${job.post_id}-${job.source_external_id}.mp4`;
    await notify('upload', 65, 'Enviando para Panda');
    await uploadTus(job.panda_api_key, clip, filename, job.folder_id || '', newId);
    await notify('upload', 80, 'Upload concluído');

    await notify('convert', 82, 'Convertendo na Panda');
    const converted = await waitConverted(job.panda_api_key, newId, (attempt, max) => {
      const pct = 82 + Math.floor((attempt / max) * 15);
      notify('convert', Math.min(96, pct), `Convertendo (${attempt + 1}/${max})`);
    });

    await callback(job.callback_url, job.callback_token, {
      job_id: job.job_id,
      post_id: job.post_id,
      status: 'ready',
      stage: 'ready',
      progress: 100,
      clip_external_id: converted.video_external_id,
      clip_internal_id: converted.id,
      clip_lib: job.library_id || '',
      clip_start: start,
      clip_end: end,
      clip_seconds: end - start,
      message: 'Clip gerado',
    });
  } catch (err) {
    await callback(job.callback_url, job.callback_token, {
      job_id: job.job_id,
      post_id: job.post_id,
      status: 'error',
      stage: 'error',
      progress: 0,
      message: err?.message || String(err),
    });
    throw err;
  } finally {
    await fs.rm(workDir, { recursive: true, force: true }).catch(() => {});
  }
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    busy,
    current_job_id: currentJobId,
    ffmpeg: true,
  });
});

app.post('/v1/jobs', auth, async (req, res) => {
  if (busy) {
    return res.status(409).json({
      ok: false,
      error: 'busy',
      message: 'Worker ocupado (concurrency=1).',
      current_job_id: currentJobId,
    });
  }

  const body = req.body || {};
  const required = ['post_id', 'source_external_id', 'panda_api_key', 'callback_url', 'download_host'];
  for (const key of required) {
    if (!body[key]) {
      return res.status(400).json({ ok: false, error: `missing_${key}` });
    }
  }

  const job = {
    job_id: String(body.job_id || randomUUID()),
    post_id: Number(body.post_id),
    source_external_id: String(body.source_external_id),
    clip_start: Number(body.clip_start ?? 130),
    clip_end: Number(body.clip_end ?? 160),
    folder_id: String(body.folder_id || ''),
    library_id: String(body.library_id || ''),
    callback_url: String(body.callback_url),
    callback_token: String(body.callback_token || ''),
    panda_api_key: String(body.panda_api_key),
    download_host: String(body.download_host),
  };

  busy = true;
  currentJobId = job.job_id;
  res.status(202).json({ ok: true, accepted: true, job_id: job.job_id });

  setImmediate(async () => {
    try {
      await processJob(job);
    } catch (err) {
      console.error('job failed', job.job_id, err?.message || err);
    } finally {
      busy = false;
      currentJobId = null;
    }
  });
});

app.listen(PORT, () => {
  console.log(`abiud-tease-clip-worker on :${PORT}`);
});
