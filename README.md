# MiniMedium API (Import/Export Module)

Production-grade Node.js (TypeScript + Express) system backed by PostgreSQL and BullMQ workers for high-volume import/export workflows.

The codebase uses [the RealWorld Node + Express starter](https://github.com/gothinkster/node-express-realworld-example-app) as its boilerplate/template.

This README focuses on the import/export system: API surface, runtime architecture, configuration, local/dev/prod workflows, and expected behavior.

## Quick Navigation

- [Architecture](#architecture)
- [Feature Summary](#feature-summary)
- [Data Model (Import/Export)](#data-model-importexport)
- [Job Status Semantics](#job-status-semantics)
- [API Surface](#api-surface)
- [Configuration](#configuration)
- [Local Development](#local-development)
- [Scripts](#scripts)
- [Testing](#testing)
- [To Be Implemented](#to-be-implemented)

## Architecture

```mermaid
flowchart LR
  C["Client"]
  API["API (Node/Express)"]
  Q(["Queue (Redis + BullMQ)"])
  W{{"Worker (Node + BullMQ)"}}
  DB[("DB (PostgreSQL)")]
  S[("Storage (Local Filesystem)")]


  C -->|"Import/Export request"| API
  API -->|"Import/Export response"| C
  API -->|"Enqueue job"| Q
  Q -->|"Queue job push"| W
  W -->|"Read/Persist job status"| DB
  W -->|"Read/Persist records"| S
  API -->|"Read/Persist job status"| DB
```

## Feature Summary

- Async import & export jobs for `users`, `articles`, `comments`
- Async import & export jobs to & from `json`/`ndjson`
- Streaming export endpoint with cursor pagination (`limit` capped to 1000 per request)
- Idempotency on import/export job creation via `Idempotency-Key`
- Handles up to 1,000,000 records per job, with O(1) memory (streaming + batching)
- Processes over 40k rows/second for ndjson exports (M1 MacBook Pro 2022, local PostgreSQL, 1M test records)
- Per-record import validation and error reporting, with persistence
- Full import error report download endpoint
- For async exports, download endpoint with artifact metadata (`expiresAt`, `fileSize` etc.)
- Structured lifecycle logging and metrics (`durationMs`, `rowsPerSecond`, `errorRate` etc.)

## Data Model (Import/Export)

Prisma models in `src/prisma/schema.prisma`:

- `ImportJob`
  - status/counters: `status`, `processedRecords`, `successCount`, `errorCount`
  - source metadata: `sourceType`, `sourceLocation`, `fileName`, `fileSize`
  - idempotency: unique `(createdById, idempotencyKey, resource)`
  - report metadata: `errorSummary` JSON
- `ImportError`
  - per-record error details for import failures
- `ExportJob`
  - status/counters: `status`, `processedRecords`
  - output metadata: `outputLocation`, `downloadUrl`, `fileSize`, `expiresAt`
  - idempotency: unique `(createdById, idempotencyKey, resource)`

## Job Status Semantics

All job states:

- `queued`: request accepted, job row created, and job is waiting in BullMQ.
- `running`: worker has claimed the job and processing is in progress.
- `succeeded`: processing finished successfully.
  - Import: `errorCount === 0`
  - Export: output artifact generated and available for download.
- `partial`: import only, used when `successCount > 0 && errorCount > 0`.
- `failed`: terminal failure.
  - Import: `successCount === 0 && errorCount > 0` OR fatal pipeline failure.
  - Export: fatal pipeline failure (artifact not available).
- `cancelled`: cancellation was requested and the worker stopped at a cancellation checkpoint.

## API Surface

Base path prefix: `/api`

### Imports

POST `/v1/imports` - Creates imports

Params:
- Path: none
- Query: none
- Headers:
  - Required: `Authorization` (`Bearer <jwt>`), `Idempotency-Key` (non-empty string)
- Body:
  - Required: `resource` (`users|articles|comments`)
  - Required source: one of `url` (http/https URL) OR multipart `file`
  - Optional: `format` (`json|ndjson`, inferred from filename if omitted)

```bash
curl -X POST http://localhost:3000/api/v1/imports \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: import-articles-001" \
  -d '{"resource":"articles","url":"https://example.com/articles.ndjson"}'
```

Sample response (`202`):

```json
{
  "importJob": {
    "id": "b2b31afc-11f4-4c0a-adf8-5d9f5f2d0e90",
    "status": "queued",
    "entityType": "articles",
    "format": "ndjson",
    "processedRecords": 0,
    "successCount": 0,
    "errorCount": 0,
    "idempotencyKey": "import-articles-001"
  }
}
```

GET `/v1/imports/:jobId` - Get import job status and error report URL (if applicable)


Params:
- Path:
  - Required: `jobId` (import job UUID)
- Headers:
  - Required: `Authorization` (`Bearer <jwt>`)

```bash
curl "http://localhost:3000/api/v1/imports/<jobId>" \
  -H "Authorization: Bearer $JWT"
```

Sample response:

```json
{
  "importJob": {
    "id": "b2b31afc-11f4-4c0a-adf8-5d9f5f2d0e90",
    "status": "partial",
    "entityType": "articles",
    "format": "ndjson",
    "totalRecords": 10000,
    "processedRecords": 10000,
    "successCount": 9800,
    "errorCount": 200
  },
  "errorReportUrl": "/api/v1/imports/b2b31afc-11f4-4c0a-adf8-5d9f5f2d0e90/errors/download",
  "errorReportStatus": "ready"
}
```

GET `/v1/imports/:jobId/errors/download` - Download full import error report (NDJSON with one error object per line)


Params:
- Path:
  - Required: `jobId` (import job UUID)
- Headers:
  - Required: `Authorization` (`Bearer <jwt>`)

```bash
curl -L "http://localhost:3000/api/v1/imports/<jobId>/errors/download" \
  -H "Authorization: Bearer $JWT" \
  -o import-errors.ndjson
```

Sample download response:

```ndjson
{
  "recordIndex": 4197,
  "recordId": "bulk-article-20260208-04198",
  "errorCode": 1007,
  "errorName": "DUPLICATE_VALUE",
  "message": "Slug is already in use",
  "field": "slug",
  "value": "bulk-article-20260208-04198",
  "details": null,
  "createdAt": "2026-02-09T14:12:23.344Z"
}
{
  "recordIndex": 8871,
  "recordId": "bulk-article-20260208-08872",
  "errorCode": 1007,
  "errorName": "DUPLICATE_VALUE",
  "message": "Slug is already in use",
  "field": "slug",
  "value": "bulk-article-20260208-08872",
  "details": null,
  "createdAt": "2026-02-09T14:12:27.974Z"
}
...
```

### Exports

GET `/v1/exports` - Stream exports with cursor pagination 

Params:
- Query:
  - Required: `resource` (`users|articles|comments`)
  - Optional: `format` (`json|ndjson`, default `ndjson`)
  - Optional: `limit` (`1..1000`, default `1000`)
  - Optional: `cursor` (positive integer, use prior `nextCursor`)
  - Optional: `filters` (JSON object with filter conditions, e.g. `{"authorId": 123}`)
  - Optional: `fields` (comma-separated list of field names to include, e.g. `id, slug`)
- Headers:
  - Required: `Authorization` (`Bearer <jwt>`)

```bash
curl "http://localhost:3000/api/v1/exports?resource=articles&format=json&limit=3" \
  -H "Authorization: Bearer $JWT"
```

Sample response (`format=ndjson`):

```ndjson
{ "id": 1, "title": "A" },
{ "id": 3, "title": "B" },
{ "id": 5, "title": "C" }
{"_type": "cursor", "nextCursor": 1012}
```

POST `/v1/exports` - Create async export job with filters and fields


Params:
- Headers:
  - Required: `Authorization` (`Bearer <jwt>` or `Token <jwt>`)
  - Optional: `Idempotency-Key` (non-empty string)
- Body:
  - Required: `resource` (`users|articles|comments`)
  - Optional: `format` (`json|ndjson`, default `ndjson`)
  - Optional: `filters` (JSON object/array)
  - Optional: `fields` (JSON object/array)

```bash
curl -X POST http://localhost:3000/api/v1/exports \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: export-articles-001" \
  -d '{"resource":"articles","format":"ndjson","filters":{"authorId":123},"fields":["id","title"]}'
```

Sample response (`202`):

```json
{
  "exportJob": {
    "id": "8b12e4c7-66e6-4ed0-a637-b9ab5ba8b9de",
    "status": "queued",
    "entityType": "articles",
    "format": "ndjson",
    "processedRecords": 0,
    "idempotencyKey": "export-articles-001"
  }
}
```

GET `/v1/exports/:jobId` - Get async export job status and download URL (if applicable)

Params:
- Path:
  - Required: `jobId` (export job UUID)
- Headers:
  - Required: `Authorization` (`Bearer <jwt>`)

```bash
curl "http://localhost:3000/api/v1/exports/<jobId>" \
  -H "Authorization: Bearer $JWT"
```

Sample response:

```json
{
  "exportJob": {
    "id": "8b12e4c7-66e6-4ed0-a637-b9ab5ba8b9de",
    "status": "succeeded",
    "entityType": "articles",
    "format": "ndjson",
    "totalRecords": 1000001,
    "processedRecords": 1000000,
    "downloadUrl": "/api/v1/exports/8b12e4c7-66e6-4ed0-a637-b9ab5ba8b9de/download",
    "fileSize": 21844253,
    "truncated": true,
    "recordLimit": 1000000,
    "reason": "max_records_reached"
  }
}
```

GET `/v1/exports/:jobId/download`


Params:
- Path:
  - Required: `jobId` (export job UUID)
- Headers:
  - Required: `Authorization` (`Bearer <jwt>`)

```bash
curl -L "http://localhost:3000/api/v1/exports/<jobId>/download" \
  -H "Authorization: Bearer $JWT" \
  -o export.ndjson
```

Sample response (download):

```http
HTTP/1.1 200 OK
Content-Type: application/x-ndjson
Content-Disposition: attachment; filename="<jobId>.ndjson"
```

### NB: All routes require authentication via `Authorization` header (`Bearer <jwt>`). Get a JWT by creating a user as shown below
#### Register a user and get JWT token

POST `/users`

Params:
- Body (`user`): `username` (non-empty string), `email` (valid email), `password` (non-empty string)

```bash
curl -X POST http://localhost:3000/api/users \
  -H "Content-Type: application/json" \
  -d '{"user":{"username":"importadmin","email":"importadmin@example.com","password":"SomePassword123"}}'
```

Sample response:

```json
{
  "user": {
    "email": "somenewuser@example.com",
    "username": "somenewuser",
    "bio": null,
    "image": null,
    "token": "<jwt>"
  }
}
```


## Configuration

### Core

- `DATABASE_URL`
- `REDIS_URL`
- `JWT_SECRET`
- `PORT` (default `3000`)
- `JOB_WORKER_CONCURRENCY` (default `4`)

### Import

- `IMPORT_BATCH_SIZE` (default `1000`)
- `IMPORT_MAX_FILE_SIZE` (default `1073741824` = 1GB)
- `IMPORT_MAX_RECORDS` (default `1000000`)
- `IMPORT_ALLOWED_HOSTS` (comma-separated allowlist; empty allows all)
- `IMPORT_STORAGE_PATH` (default `./imports`)
- `ERROR_REPORT_STORAGE_PATH` (default `./import-errors`)
- `IMPORT_ERROR_REPORT_DOWNLOAD_BASE_URL` (optional absolute base URL)

### Export

- `EXPORT_BATCH_SIZE` (default `1000`)
- `EXPORT_MAX_RECORDS` (default `1000000`, async export cap)
- `EXPORT_STREAM_MAX_LIMIT` (default `1000`)
- `EXPORT_STORAGE_PATH` (default `./exports`)
- `EXPORT_FILE_RETENTION_HOURS` (default `24`)
- `EXPORT_DOWNLOAD_BASE_URL` (optional absolute base URL)

### Logging

- `LOG_LEVEL` (default `info`)
- `LOG_PRETTY`
  - `true`: pretty logs (dev-friendly)
  - `false`: raw JSON logs (production/aggregation friendly)

## Local Development

### Prerequisites

- Node.js 22+
- Docker + Docker Compose

### Environment

Create `.env` in project root from `.env.sample` and update values as needed like so:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/minimedium?schema=public
REDIS_URL=redis://localhost:6379
JWT_SECRET=some-jwt-secret
# The remaining values have configured defaults, but you can override them here if needed
```

### Install dependencies

```bash
npm install
```

### Run with Docker Compose

Dev stack with live-reload and logs in the foreground:

```bash
npm run dev:up
```

Base stack (Production-like, no live reload, detached):

```bash
npm run prod:up
```

Stop:

```bash
npm run prod:down # if using base stack
npm run dev:down # if using dev stack
```

## Scripts

- `npm run dev:api` - run API with nodemon
- `npm run dev:worker` - run worker with nodemon
- `npm run run:api` - run API without watcher
- `npm run run:worker` - run worker without watcher
- `npm run dev:up` - run Docker Compose base + dev override
- `npm run dev:down` - stop dev Docker Compose stack and delete containers
- `npm run prod:up` - run Docker compose base stack (detached)
- `npm run prod:down` - stop Docker compose base stack and delete containers
- `npm test` - all tests

## Testing

Run import/export + jobs suites:

```bash
npx jest src/tests/routes/imports src/tests/routes/exports src/tests/jobs --runInBand
```

## To Be Implemented

- **Imported user auth**: add imported-user credential onboarding (password setup/reset flow + login gating for imported accounts).
- **Job cancellation endpoint**: add import/export job cancellation endpoints that mark jobs as `cancelled` in DB and let worker lifecycle checks stop them.
- **AuthZ * rate limiting**: implement security controls for import/export (admin-only authorization, rate limiting etc).
- **Import, export and error report retention cleanup**: add a scheduled cron job to delete rows from DB and files from Storage after configured download deadline.
