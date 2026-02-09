import { createReadStream } from 'fs';
import { once } from 'events';
import { NextFunction, Request, Response, Router } from 'express';
import HttpException from '../../models/http-exception.model';
import { HttpStatusCode } from '../../models/http-status-code.model';
import { createLogger } from '../../logger';
import auth from '../auth/auth';
import { AuthenticatedRequest, getIdempotencyKey, requireUserId } from '../shared/import-export/utils';
import {
  buildExportStreamClosingChunk,
  createExportJob,
  getExportFileMetadata,
  getExportJob,
  getExportPayload,
  parseExportQuery,
  streamExports,
} from './export.service';

const router = Router();
const logger = createLogger({ component: 'exports.controller' });

router.get('/v1/exports/:jobId/download', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const metadata = await getExportFileMetadata({
      jobId: req.params.jobId,
      createdById,
    });

    res.setHeader('Content-Type', metadata.contentType);
    res.setHeader('Content-Disposition', metadata.contentDisposition);

    const stream = createReadStream(metadata.outputLocation);
    stream.on('error', () => {
      next(new HttpException(HttpStatusCode.NOT_FOUND, { errors: { job: ['export artifact not found'] } }));
    });
    stream.pipe(res);
  } catch (error) {
    next(error);
  }
});

router.post('/v1/exports', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const payload = getExportPayload(req.body);
    const result = await createExportJob({
      createdById,
      payload,
      idempotencyKey: getIdempotencyKey(req),
    });

    res.status(result.statusCode).json({ exportJob: result.exportJob });
  } catch (error) {
    next(error);
  }
});

router.get('/v1/exports/:jobId', auth.required, async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const result = await getExportJob({
      jobId: req.params.jobId,
      createdById,
    });

    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

router.get('/v1/exports', auth.required, async (req: Request, res: Response, next: NextFunction) => {
  let responseFormat: 'json' | 'ndjson' | null = null;
  let limit = 0;
  let count = 0;
  let lastId: number | null = null;
  let isJsonStarted = false;
  let isJsonClosed = false;

  try {
    const parsed = parseExportQuery(req.query as Record<string, unknown>);
    responseFormat = parsed.format;
    limit = parsed.limit;

    res.status(200);
    res.setHeader('Content-Type', parsed.format === 'ndjson' ? 'application/x-ndjson' : 'application/json');
    res.setHeader('Cache-Control', 'no-store');

    const abortController = new AbortController();
    req.on('close', () => abortController.abort());

    const writeChunk = async (chunk: string): Promise<void> => {
      if (!res.write(chunk)) {
        await once(res, 'drain');
      }
    };

    if (parsed.format === 'json') {
      isJsonStarted = true;
    }

    const result = await streamExports({
      entityType: parsed.entityType,
      format: parsed.format,
      limit: parsed.limit,
      cursor: parsed.cursor,
      filters: parsed.filters,
      fields: parsed.fields,
      signal: abortController.signal,
      writeChunk,
      onRecord: (progress) => {
        count = progress.count;
        lastId = progress.lastId;
      },
    });

    count = result.count;
    lastId = result.lastId;
    isJsonClosed = parsed.format === 'json';
    res.end();
  } catch (error) {
    if (res.headersSent) {
      if (responseFormat === 'json' && isJsonStarted && !isJsonClosed) {
        try {
          res.write(buildExportStreamClosingChunk('json', count, limit, lastId));
        } catch (writeError) {
          logger.debug({
            event: 'Export stream JSON closing chunk write failed',
            count,
            limit,
            lastId,
            errorName: writeError instanceof Error ? writeError.name : 'UnknownError',
            errorMessage: writeError instanceof Error ? writeError.message : String(writeError),
          });
        }
      }
      res.end();
      return;
    }
    next(error);
  }
});

export default router;
