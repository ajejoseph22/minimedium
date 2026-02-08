import { createReadStream } from 'fs';
import { NextFunction, Response, Router } from 'express';
import HttpException from '../../models/http-exception.model';
import auth from '../auth/auth';
import { createImportUploadMiddleware, UploadedFile } from './intake.service';
import {
  AuthenticatedRequest,
  getIdempotencyKey,
  getQueryParamValue,
  requireUserId,
} from '../shared/import-export/utils';
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

router.post(
  '/v1/imports',
  auth.required,
  importUploadMiddleware,
  async (req: ImportRequest, res: Response, next: NextFunction) => {
    try {
      const createdById = requireUserId(req);
      const payload = getImportPayload(req.body);
      const result = await createImportJob({
        createdById,
        payload,
        file: req.file,
        idempotencyKey: getIdempotencyKey(req) || null,
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
      errorPreviewLimit: getQueryParamValue(req.query.errorPreviewLimit),
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
        next(new HttpException(404, { errors: { job: ['import error report not found'] } }));
      });
      stream.pipe(res);
    } catch (error) {
      next(error);
    }
  },
);

export default router;
