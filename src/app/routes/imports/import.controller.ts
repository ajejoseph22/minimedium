import { createReadStream } from 'fs';
import { NextFunction, Response, Router } from 'express';
import HttpException from '../../models/http-exception.model';
import { HttpStatusCode } from '../../models/http-status-code.model';
import auth from '../auth/auth';
import { createImportUploadMiddleware, ImportExportError, UploadedFile } from './intake.service';
import {
  AuthenticatedRequest,
  requireIdempotencyKey,
  requireUserId,
} from '../shared/import-export/utils';
import { FileErrorCode } from '../shared/import-export/types';
import {
  createImportJob,
  getErrorReportFileMetadata,
  getImportJobStatus,
  getImportPayload,
} from './import.service';

const router = Router();
const importUploadMiddleware = createImportUploadMiddleware('file');

interface ImportRequest extends AuthenticatedRequest {
  file?: UploadedFile;
}

function isMulterError(error: unknown): error is Error & { code: string } {
  return (
    error instanceof Error &&
    error.name === 'MulterError' &&
    typeof (error as { code?: unknown }).code === 'string'
  );
}

function mapImportUploadError(error: unknown): HttpException {
  if (error instanceof ImportExportError) {
    if (error.code === FileErrorCode.FILE_TOO_LARGE) {
      return new HttpException(HttpStatusCode.PAYLOAD_TOO_LARGE, {
        errors: { file: [error.message] },
      });
    }

    if (
      error.code === FileErrorCode.UNSUPPORTED_FORMAT ||
      error.code === FileErrorCode.EMPTY_FILE
    ) {
      return new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
        errors: { file: [error.message] },
      });
    }
  }

  if (isMulterError(error)) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return new HttpException(HttpStatusCode.PAYLOAD_TOO_LARGE, {
        errors: { file: ['Uploaded file exceeds size limit'] },
      });
    }

    return new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
      errors: { file: [error.message] },
    });
  }

  return new HttpException(HttpStatusCode.INTERNAL_SERVER_ERROR, {
    errors: { import: ['import upload failed'] },
  });
}

router.post(
  '/v1/imports',
  auth.required,
  (req: ImportRequest, res: Response, next: NextFunction) => {
    importUploadMiddleware(req, res, (error: unknown) => {
      if (!error) {
        next();
        return;
      }
      next(mapImportUploadError(error));
    });
  },
  async (req: ImportRequest, res: Response, next: NextFunction) => {
    try {
      const createdById = requireUserId(req);
      const payload = getImportPayload(req.body);
      const result = await createImportJob({
        createdById,
        payload,
        file: req.file,
        idempotencyKey: requireIdempotencyKey(req),
      });

      res.status(result.statusCode).json({ importJob: result.importJob });
    } catch (error) {
      next(error);
    }
  },
);

router.get('/v1/imports/:jobId', auth.required, async (req: ImportRequest, res: Response, next: NextFunction) => {
  try {
    const createdById = requireUserId(req);
    const result = await getImportJobStatus({
      jobId: req.params.jobId,
      createdById,
    });

    res.status(200).json(result);
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
      const report = await getErrorReportFileMetadata({
        jobId: req.params.jobId,
        createdById,
      });

      res.setHeader('Content-Type', report.contentType);
      res.setHeader('Content-Disposition', report.contentDisposition);

      const stream = createReadStream(report.reportLocation);
      stream.on('error', () => {
        next(new HttpException(HttpStatusCode.NOT_FOUND, { errors: { job: ['import error report not found'] } }));
      });
      stream.pipe(res);
    } catch (error) {
      next(error);
    }
  },
);

export default router;
