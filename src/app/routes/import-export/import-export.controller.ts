import { NextFunction, Request, Response, Router } from 'express';
import { once } from 'events';
import auth from '../auth/auth';
import HttpException from '../../models/http-exception.model';
import { loadConfig } from './config';
import { streamExportRecords } from './export.service';
import { EntityType, FileFormat } from './types';

const router = Router();

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
          // swallow to ensure response ends
        }
      }
      res.end();
      return;
    }
    next(error);
  }
});

export default router;

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

  if (!resource) {
    throw new HttpException(422, { errors: { resource: ['resource is required'] } });
  }

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

function parseEntityType(value: string): EntityType {
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
