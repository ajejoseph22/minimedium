import { createReadStream } from 'fs';
import { once } from 'events';
import { NextFunction, Request, Response, Router } from 'express';
import type { Prisma } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import HttpException from '../../models/http-exception.model';
import { enqueueExportJob, enqueueImportJob } from '../../jobs/import-export.queue';
import auth from '../auth/auth';
import { loadConfig } from './config';
import { streamExportRecords } from './export.service';
import {
  createImportUploadMiddleware,
  fetchRemoteImport,
  mapUploadedFileToImportIntakeResult,
  UploadedFile,
  validateUploadedFile,
} from './intake.service';
import { EntityType, FileFormat } from './types';

const router = Router();
const importUploadMiddleware = createImportUploadMiddleware('file');

interface AuthenticatedRequest extends Request {
  auth?: { user?: { id?: number } };
  file?: UploadedFile;
}

interface ImportCreatePayload {
  resource?: string;
  format?: string;
  url?: string;
}

interface ExportCreatePayload {
  resource?: string;
  format?: string;
  filters?: Prisma.InputJsonValue;
  fields?: Prisma.InputJsonValue;
}

router.post(
  '/v1/imports',
  auth.required,
  importUploadMiddleware,
  async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
    try {
      const createdById = requireUserId(req);
      const payload = getImportPayload(req);
      const resource = parseEntityType(payload.resource);
      const headerIdempotencyKey = getIdempotencyKey(req);
      const idempotencyKey = headerIdempotencyKey || null;

      if (idempotencyKey) {
        const existing = await prismaClient.importJob.findFirst({
          where: { createdById, idempotencyKey, resource },
        });
        if (existing) {
          res.status(200).json({ importJob: serializeImportJob(existing) });
          return;
        }
      }

      const intake = await resolveImportIntake(req.file, payload);
      const format = resolveFormat(payload.format, intake.fileName);

      const created = await prismaClient.importJob.create({
        data: {
          status: 'queued',
          resource,
          format,
          sourceType: intake.sourceType,
          sourceLocation: intake.location,
          fileName: intake.fileName,
          fileSize: intake.bytes,
          idempotencyKey,
          createdById,
          requestHash: null,
        },
      });

      await enqueueImportJob({
        jobId: created.id,
        resource: created.resource,
        format: created.format,
      });

      res.status(202).json({ importJob: serializeImportJob(created) });
    } catch (error) {
      next(error);
    }
  },
);

router.get('/v1/imports/:jobId', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const job = await prismaClient.importJob.findFirst({
      where: { id: req.params.jobId, createdById },
    });

    if (!job) {
      throw new HttpException(404, { errors: { job: ['import job not found'] } });
    }

    const errorLimit = parsePositiveInteger(getQueryParamValue(req.query.errorLimit), 100, 500, 'errorLimit');
    const errors = await prismaClient.importError.findMany({
      where: { jobId: job.id },
      orderBy: { recordIndex: 'asc' },
      take: errorLimit,
    });

    const errorSummary = toJsonObject(job.errorSummary);
    const errorReportUrl =
      typeof errorSummary?.reportLocation === 'string' ? (errorSummary.reportLocation as string) : undefined;

    res.status(200).json({
      importJob: serializeImportJob(job),
      errors: errors.map(serializeImportError),
      errorReportUrl,
    });
  } catch (error) {
    next(error);
  }
});

router.get('/v1/exports/:jobId/download', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const job = await prismaClient.exportJob.findFirst({
      where: { id: req.params.jobId, createdById },
    });

    if (!job) {
      throw new HttpException(404, { errors: { job: ['export job not found'] } });
    }

    if (job.status !== 'succeeded' || !job.outputLocation) {
      throw new HttpException(409, { errors: { job: ['export is not ready for download'] } });
    }

    if (job.expiresAt && job.expiresAt.getTime() < Date.now()) {
      throw new HttpException(410, { errors: { job: ['download URL has expired'] } });
    }

    const format = normalizeFormat(job.format);
    res.setHeader('Content-Type', format === 'json' ? 'application/json' : 'application/x-ndjson');
    res.setHeader('Content-Disposition', `attachment; filename="${job.id}.${format}"`);

    const stream = createReadStream(job.outputLocation);
    stream.on('error', () => {
      next(new HttpException(404, { errors: { job: ['export artifact not found'] } }));
    });
    stream.pipe(res);
  } catch (error) {
    next(error);
  }
});

router.post('/v1/exports', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const payload = getExportPayload(req);
    const resource = parseEntityType(payload.resource);
    const format = parseFormat(payload.format);
    const idempotencyKey = getIdempotencyKey(req) || null;

    if (idempotencyKey) {
      const existing = await prismaClient.exportJob.findFirst({
        where: { createdById, idempotencyKey, resource },
      });
      if (existing) {
        res.status(200).json({ exportJob: serializeExportJob(existing) });
        return;
      }
    }

    const created = await prismaClient.exportJob.create({
      data: {
        status: 'queued',
        resource,
        format,
        filters: normalizeJsonValue(payload.filters),
        fields: normalizeJsonValue(payload.fields),
        idempotencyKey,
        createdById,
        requestHash: null,
      },
    });

    await enqueueExportJob({
      jobId: created.id,
      resource: created.resource,
      format: created.format,
    });

    res.status(202).json({ exportJob: serializeExportJob(created) });
  } catch (error) {
    next(error);
  }
});

router.get('/v1/exports/:jobId', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const job = await prismaClient.exportJob.findFirst({
      where: { id: req.params.jobId, createdById },
    });

    if (!job) {
      throw new HttpException(404, { errors: { job: ['export job not found'] } });
    }

    res.status(200).json({ exportJob: serializeExportJob(job) });
  } catch (error) {
    next(error);
  }
});

/**
 * Stream exports for users/articles/comments.
 * @auth required
 * @route {GET} /api/v1/exports
 */
router.get('/v1/exports', auth.required, async (req: Request, res: Response, next: NextFunction) => {
  let responseFormat: FileFormat | null = null;
  let limit = 0;
  let count = 0;
  let lastId: number | null = null;
  let jsonStarted = false;
  let jsonClosed = false;

  try {
    const parsed = parseExportQuery(req);
    const { entityType, format, cursor } = parsed;
    responseFormat = format;
    limit = parsed.limit;

    res.status(200);
    res.setHeader('Content-Type', format === 'ndjson' ? 'application/x-ndjson' : 'application/json');
    res.setHeader('Cache-Control', 'no-store');

    const abortController = new AbortController();
    req.on('close', () => abortController.abort());

    const writeChunk = async (chunk: string): Promise<void> => {
      if (!res.write(chunk)) {
        await once(res, 'drain');
      }
    };

    let first = true;
    if (format === 'json') {
      await writeChunk('{"data":[');
      jsonStarted = true;
    }

    for await (const record of streamExportRecords({
      entityType,
      limit,
      cursor,
      signal: abortController.signal,
    })) {
      const payload = JSON.stringify(record);
      if (format === 'json') {
        await writeChunk(first ? payload : `,${payload}`);
      } else {
        await writeChunk(`${payload}\n`);
      }
      count += 1;
      lastId = record.id;
      first = false;
    }

    if (format === 'json') {
      const nextCursor = count === limit ? lastId : null;
      await writeChunk(`],"nextCursor":${nextCursor ?? 'null'}}`);
      jsonClosed = true;
    } else {
      const nextCursor = count === limit ? lastId : null;
      await writeChunk(`${JSON.stringify({ _type: 'cursor', nextCursor })}\n`);
    }

    res.end();
  } catch (error) {
    if (res.headersSent) {
      if (responseFormat === 'json' && jsonStarted && !jsonClosed) {
        try {
          const fallbackCursor = count === limit ? lastId : null;
          res.write(`],"nextCursor":${fallbackCursor ?? 'null'}}`);
        } catch {
          // Best effort only; response is ending anyway.
        }
      }
      res.end();
      return;
    }
    next(error);
  }
});

export default router;

function requireUserId(req: AuthenticatedRequest): number {
  const userId = req.auth?.user?.id;
  if (typeof userId !== 'number') {
    throw new HttpException(401, { errors: { auth: ['missing authorization credentials'] } });
  }
  return userId;
}

function getIdempotencyKey(req: Request): string | undefined {
  const key = req.get('Idempotency-Key')?.trim();
  return key && key.length > 0 ? key : undefined;
}

function getImportPayload(req: Request): ImportCreatePayload {
  const body = req.body?.import ?? req.body;
  if (!isObject(body)) {
    return {};
  }
  return body as ImportCreatePayload;
}

function getExportPayload(req: Request): ExportCreatePayload {
  const body = req.body?.export ?? req.body;
  if (!isObject(body)) {
    return {};
  }
  return body as ExportCreatePayload;
}

async function resolveImportIntake(file: UploadedFile | undefined, payload: ImportCreatePayload) {
  if (file) {
    await validateUploadedFile(file);
    return mapUploadedFileToImportIntakeResult(file);
  }

  if (typeof payload.url === 'string' && payload.url.trim().length > 0) {
    return fetchRemoteImport({ url: payload.url.trim() });
  }

  throw new HttpException(422, { errors: { source: ['file upload or url is required'] } });
}

function resolveFormat(rawFormat: string | undefined, fileName: string): FileFormat {
  if (rawFormat) {
    return parseFormat(rawFormat);
  }
  const ext = fileName.toLowerCase().split('.').pop();
  return ext === 'json' ? 'json' : 'ndjson';
}

function parseExportQuery(req: Request): {
  entityType: EntityType;
  format: FileFormat;
  limit: number;
  cursor: number | null;
} {
  const config = loadConfig();
  const resource = getQueryParamValue(req.query.resource);
  const formatValue = getQueryParamValue(req.query.format);
  const limitValue = getQueryParamValue(req.query.limit);
  const cursorValue = getQueryParamValue(req.query.cursor);

  const entityType = parseEntityType(resource);
  const format = parseFormat(formatValue);
  const limit = parseLimit(limitValue, config.exportStreamMaxLimit);
  const cursor = parseCursor(cursorValue);

  return { entityType, format, limit, cursor };
}

function getQueryParamValue(value: unknown): string | undefined {
  if (typeof value === 'string') {
    return value;
  }
  if (Array.isArray(value) && value.length > 0) {
    return String(value[0]);
  }
  return undefined;
}

function parseEntityType(value: string | undefined): EntityType {
  if (!value) {
    throw new HttpException(422, { errors: { resource: ['resource is required'] } });
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === 'users' || normalized === 'articles' || normalized === 'comments') {
    return normalized as EntityType;
  }
  throw new HttpException(422, {
    errors: { resource: ['resource must be one of users, articles, comments'] },
  });
}

function parseFormat(value?: string): FileFormat {
  if (!value) {
    return 'ndjson';
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === 'ndjson' || normalized === 'json') {
    return normalized as FileFormat;
  }
  throw new HttpException(422, {
    errors: { format: ['format must be json or ndjson'] },
  });
}

function normalizeFormat(value: string | null): FileFormat {
  if (value === 'ndjson' || value === 'json') {
    return value;
  }
  return 'ndjson';
}

function parseLimit(value: string | undefined, maxRecords: number): number {
  if (!value) {
    return maxRecords;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new HttpException(422, { errors: { limit: ['limit must be a positive integer'] } });
  }
  if (parsed > maxRecords) {
    throw new HttpException(422, {
      errors: { limit: [`limit must be <= ${maxRecords}`] },
    });
  }
  return parsed;
}

function parseCursor(value?: string): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new HttpException(422, { errors: { cursor: ['cursor must be a positive integer'] } });
  }
  return parsed;
}

function parsePositiveInteger(
  value: string | undefined,
  defaultValue: number,
  maxValue: number,
  fieldName: string,
): number {
  if (!value) {
    return defaultValue;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0 || parsed > maxValue) {
    throw new HttpException(422, {
      errors: { [fieldName]: [`${fieldName} must be a positive integer <= ${maxValue}`] },
    });
  }
  return parsed;
}

function serializeImportJob(job: {
  id: string;
  status: string;
  resource: string;
  format: string;
  totalRecords: number | null;
  processedRecords: number;
  successCount: number;
  errorCount: number;
  createdAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  fileName: string | null;
  fileSize: number | null;
  sourceLocation: string | null;
  idempotencyKey: string | null;
  errorSummary?: Prisma.JsonValue | null;
}) {
  return {
    id: job.id,
    status: job.status,
    entityType: job.resource,
    format: job.format,
    totalRecords: job.totalRecords,
    processedRecords: job.processedRecords,
    successCount: job.successCount,
    errorCount: job.errorCount,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    completedAt: job.finishedAt,
    createdBy: undefined,
    idempotencyKey: job.idempotencyKey,
    fileName: job.fileName,
    fileSize: job.fileSize,
    sourceUrl: job.sourceLocation,
    errorSummary: toJsonObject(job.errorSummary),
  };
}

function serializeExportJob(job: {
  id: string;
  status: string;
  resource: string;
  format: string;
  totalRecords: number | null;
  processedRecords: number;
  createdAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  expiresAt: Date | null;
  idempotencyKey: string | null;
  outputLocation: string | null;
  downloadUrl: string | null;
  fileSize: number | null;
}) {
  return {
    id: job.id,
    status: job.status,
    entityType: job.resource,
    format: job.format,
    totalRecords: job.totalRecords,
    processedRecords: job.processedRecords,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    completedAt: job.finishedAt,
    expiresAt: job.expiresAt,
    createdBy: undefined,
    idempotencyKey: job.idempotencyKey,
    outputPath: job.outputLocation,
    downloadUrl: job.downloadUrl,
    fileSize: job.fileSize,
  };
}

function serializeImportError(error: {
  id: string;
  recordIndex: number;
  recordId: string | null;
  errorCode: number;
  errorName: string;
  message: string;
  field: string | null;
  value: Prisma.JsonValue | null;
  details: Prisma.JsonValue | null;
  createdAt: Date;
}) {
  return {
    id: error.id,
    recordIndex: error.recordIndex,
    recordId: error.recordId,
    errorCode: error.errorCode,
    errorName: error.errorName,
    message: error.message,
    field: error.field,
    value: error.value,
    details: error.details,
    createdAt: error.createdAt,
  };
}

function toJsonObject(value: Prisma.JsonValue | null | undefined): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function normalizeJsonValue(value: unknown): Prisma.InputJsonValue | null {
  if (!isObject(value) && !Array.isArray(value)) {
    return null;
  }
  return value as Prisma.InputJsonValue;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
