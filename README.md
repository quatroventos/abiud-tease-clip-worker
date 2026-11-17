# Abiud Tease Clip Worker

Worker Node + FFmpeg para gerar clips de tease da Panda Video (concurrency=1).

## Endpoints

- `GET /health`
- `POST /v1/jobs` (Bearer `WORKER_TOKEN`)

## Env

```
PORT=8080
WORKER_TOKEN=<secret>
```
