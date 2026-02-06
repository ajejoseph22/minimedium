import { createReadStream } from 'fs';
import { once } from 'events';
import { NextFunction, Request, Response, Router } from 'express';
import type { Prisma } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import HttpException from '../../models/http-exception.model';
import { enqueueExportJob } from '../../jobs/import-export.queue';
import auth from '../auth/auth';
import { loadExportConfig } from './config';
import { streamExportRecords } from './export.service';
import {
  AuthenticatedRequest,
  getIdempotencyKey,
  getQueryParamValue,
  isObject,
  normalizeFormat,
  normalizeJsonValue,
  parseCursor,
  parseEntityType,
  parseFormat,
  parseLimit,
  requireUserId,
} from '../shared/import-export/utils';
import { EntityType, FileFormat } from '../shared/import-export/types';

const router = Router();

interface ExportCreatePayload {
  resource?: string;
  format?: string;
  filters?: Prisma.InputJsonValue;
  fields?: Prisma.InputJsonValue;
}

router.get('/v1/exports/:jobId/download', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const currentUserId = requireUserId(req);
    const job = await prismaClient.exportJob.findFirst({
      where: { id: req.params.jobId, createdById: currentUserId },
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

router.get('/v1/exports', auth.required, async (req: Request, res: Response, next: NextFunction) => {
  let responseFormat: FileFormat | null = null;
  let limit = 0;
  let count = 0;
  let lastId: number | null = null;
  let isJsonStarted = false;
  let isJsonClosed = false;

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
      isJsonStarted = true;
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
      isJsonClosed = true;
    } else {
      const nextCursor = count === limit ? lastId : null;
      await writeChunk(`${JSON.stringify({ _type: 'cursor', nextCursor })}\n`);
    }

    res.end();
  } catch (error) {
    if (res.headersSent) {
      if (responseFormat === 'json' && isJsonStarted && !isJsonClosed) {
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

function getExportPayload(req: Request): ExportCreatePayload {
  const body = req.body?.export ?? req.body;
  if (!isObject(body)) {
    return {};
  }
  return body as ExportCreatePayload;
}

function parseExportQuery(req: Request): {
  entityType: EntityType;
  format: FileFormat;
  limit: number;
  cursor: number | null;
} {
  const config = loadExportConfig();
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