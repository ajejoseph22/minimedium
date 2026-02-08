import { createReadStream } from 'fs';
import { NextFunction, Response, Router } from 'express';
import prismaClient from '../../../prisma/prisma-client';
import HttpException from '../../models/http-exception.model';
import { enqueueImportJob } from '../../jobs/import-export.queue';
import auth from '../auth/auth';
import {
  createImportUploadMiddleware,
  fetchRemoteImport,
  mapUploadedFileToImportIntakeResult,
  UploadedFile,
  validateUploadedFile,
} from './intake.service';
import {
  AuthenticatedRequest,
  getIdempotencyKey,
  isObject,
  parseEntityType,
  parseFormat,
  parsePositiveInteger,
  getQueryParamValue,
  requireUserId,
  toJsonObject,
} from '../shared/import-export/utils';
import { Prisma } from '@prisma/client';

const router = Router();
const importUploadMiddleware = createImportUploadMiddleware('file');

interface ImportCreatePayload {
  resource?: string;
  format?: string;
  url?: string;
}

interface ImportRequest extends AuthenticatedRequest {
  file?: UploadedFile;
}

router.post(
  '/v1/imports',
  auth.required,
  importUploadMiddleware,
  async (req: ImportRequest, res: Response, next: NextFunction) => {
    try {
      const createdById = requireUserId(req);
      const payload = getImportPayload(req);
      const resource = parseEntityType(payload.resource);
      const headerIdempotencyKey = getIdempotencyKey(req);
      const idempotencyKey = headerIdempotencyKey || null;

      if (idempotencyKey) {
        const isJobExisting = await prismaClient.importJob.findFirst({
          where: { createdById, idempotencyKey, resource },
        });

        if (isJobExisting) {
          res.status(200).json({ importJob: serializeImportJob(isJobExisting) });
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

router.get('/v1/imports/:jobId', auth.required, async (req: ImportRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const job = await prismaClient.importJob.findFirst({
      where: { id: req.params.jobId, createdById },
    });

    if (!job) {
      throw new HttpException(404, { errors: { job: ['import job not found'] } });
    }

    const rawPreviewLimit =
      getQueryParamValue(req.query.errorPreviewLimit) ?? getQueryParamValue(req.query.errorLimit);
    const previewLimit = parsePositiveInteger(rawPreviewLimit, 20, 100, 'errorPreviewLimit');
    const errorsPreview = await prismaClient.importError.findMany({
      where: { jobId: job.id },
      orderBy: { recordIndex: 'asc' },
      take: previewLimit,
    });

    const errorSummary = toJsonObject(job.errorSummary);
    const persistedErrorCount =
      typeof errorSummary?.persistedErrorCount === 'number' ? errorSummary.persistedErrorCount : null;
    const reportLocation =
      typeof errorSummary?.reportLocation === 'string' ? (errorSummary.reportLocation as string) : null;
    const errorReportStatus =
      typeof errorSummary?.reportStatus === 'string' ? (errorSummary.reportStatus as string) : undefined;
    const totalErrorCount = persistedErrorCount ?? job.errorCount;
    const errorReportUrl = reportLocation ? buildImportErrorReportDownloadUrl(job.id) : undefined;

    res.status(200).json({
      importJob: serializeImportJob(job),
      errorsPreview: errorsPreview.map(serializeImportError),
      errorsPreviewCount: errorsPreview.length,
      hasMoreErrors: totalErrorCount > errorsPreview.length,
      errorReportUrl,
      errorReportStatus,
    });
  } catch (error) {
    next(error);
  }
});

router.get(
  '/v1/imports/:jobId/errors/download',
  auth.required,
  async (req: ImportRequest, res: Response, next: NextFunction) => {
    try {
      const createdById = requireUserId(req);
      const job = await prismaClient.importJob.findFirst({
        where: { id: req.params.jobId, createdById },
      });

      if (!job) {
        throw new HttpException(404, { errors: { job: ['import job not found'] } });
      }

      const errorSummary = toJsonObject(job.errorSummary);
      const reportLocation =
        typeof errorSummary?.reportLocation === 'string' ? (errorSummary.reportLocation as string) : null;
      const reportStatus =
        typeof errorSummary?.reportStatus === 'string' ? (errorSummary.reportStatus as string) : undefined;
      const reportFormat = typeof errorSummary?.reportFormat === 'string' ? errorSummary.reportFormat : null;

      if (!reportLocation) {
        throw new HttpException(404, { errors: { job: ['import error report not found'] } });
      }

      if (reportStatus === 'failed') {
        throw new HttpException(409, { errors: { job: ['import error report is not available'] } });
      }

      const format = reportFormat === 'json' ? 'json' : 'ndjson';
      const extension = format === 'json' ? 'json' : 'ndjson';
      res.setHeader('Content-Type', format === 'json' ? 'application/json' : 'application/x-ndjson');
      res.setHeader('Content-Disposition', `attachment; filename="${job.id}-errors.${extension}"`);

      const stream = createReadStream(reportLocation);
      stream.on('error', () => {
        next(new HttpException(404, { errors: { job: ['import error report not found'] } }));
      });
      stream.pipe(res);
    } catch (error) {
      next(error);
    }
  },
);

function getImportPayload(req: ImportRequest): ImportCreatePayload {
  const body = req.body?.import ?? req.body;
  if (!isObject(body)) {
    return {};
  }
  return body as ImportCreatePayload;
}

async function resolveImportIntake(file: UploadedFile | undefined, payload: ImportCreatePayload) {
  if (file) {
    await validateUploadedFile(file);
    return mapUploadedFileToImportIntakeResult(file);
  }

  if (typeof payload.url === 'string' && payload.url.trim().length > 0) {
    return fetchRemoteImport({ url: payload.url.trim() });
  }

  throw new HttpException(422, {
    errors: { source: ['file upload or url is required'] },
  });
}

function resolveFormat(rawFormat: string | undefined, fileName: string) {
  if (rawFormat) {
    return parseFormat(rawFormat);
  }
  const ext = fileName.toLowerCase().split('.').pop();
  return ext === 'json' ? 'json' : 'ndjson';
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
    errorSummary: sanitizeImportErrorSummary(job.errorSummary),
  };
}

function sanitizeImportErrorSummary(
  value: Prisma.JsonValue | null | undefined,
): Record<string, unknown> | null {
  const summary = toJsonObject(value);
  if (!summary) {
    return null;
  }

  const { reportLocation: _reportLocation, ...safeSummary } = summary;
  return safeSummary;
}

function buildImportErrorReportDownloadUrl(jobId: string): string {
  const baseUrl = process.env.IMPORT_ERROR_REPORT_DOWNLOAD_BASE_URL?.replace(/\/$/, '');
  const pathSuffix = `/api/v1/imports/${jobId}/errors/download`;
  return baseUrl ? `${baseUrl}${pathSuffix}` : pathSuffix;
}

export default router;
