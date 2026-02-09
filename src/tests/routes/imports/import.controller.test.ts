import prismaMock from '../../prisma-mock';
import importController from '../../../app/routes/imports/import.controller';
import { enqueueImportJob } from '../../../app/jobs/import-export.queue';
import { fetchRemoteImport, ImportExportError } from '../../../app/routes/imports/intake.service';
import { createReadStream, promises as fsPromises } from 'fs';
import { createTestResponse } from '../../helpers/test-response';
import { FileErrorCode } from '../../../app/routes/shared/import-export/types';
import { HttpStatusCode } from '../../../app/models/http-status-code.model';

type UploadMiddleware = (req: unknown, res: unknown, next: (error?: unknown) => void) => void;
let uploadMiddlewareImpl: UploadMiddleware = (_req, _res, next) => next();

jest.mock('../../../app/routes/auth/auth', () => ({
  __esModule: true,
  default: { required: (_req, _res, next) => next() },
}));

jest.mock('../../../app/jobs/import-export.queue', () => ({
  enqueueImportJob: jest.fn().mockResolvedValue({ id: 'queue-import' }),
  enqueueExportJob: jest.fn().mockResolvedValue({ id: 'queue-export' }),
}));

jest.mock('../../../app/routes/imports/intake.service', () => {
  const actual = jest.requireActual('../../../app/routes/imports/intake.service');
  return {
    ...actual,
    createImportUploadMiddleware: jest.fn(() => (req, res, next) => uploadMiddlewareImpl(req, res, next)),
    fetchRemoteImport: jest.fn(),
    mapUploadedFileToImportIntakeResult: jest.fn(),
    validateUploadedFile: jest.fn(),
  };
});

jest.mock('fs', () => {
  const actualFs = jest.requireActual('fs');
  return {
    ...actualFs,
    promises: {
      ...actualFs.promises,
      rm: jest.fn().mockResolvedValue(undefined),
    },
    createReadStream: jest.fn(() => ({
      on: jest.fn().mockReturnThis(),
      pipe: jest.fn((res) => {
        res.write('{"recordIndex":3,"message":"user_id does not exist"}\n');
        res.end();
        return res;
      }),
    })),
  };
});

const prisma: any = prismaMock;

type RunRouteOptions = {
  method: string;
  url: string;
  query?: Record<string, unknown>;
  body?: unknown;
  headers?: Record<string, string>;
  auth?: { user?: { id?: number } };
  file?: unknown;
};

async function runRoute(options: RunRouteOptions) {
  const lowerHeaders = Object.fromEntries(
    Object.entries(options.headers ?? {}).map(([key, value]) => [key.toLowerCase(), value]),
  );
  const req = {
    method: options.method,
    url: options.url,
    query: options.query ?? {},
    body: options.body ?? {},
    headers: lowerHeaders,
    auth: options.auth,
    file: options.file,
    get(name: string) {
      return lowerHeaders[name.toLowerCase()];
    },
    on: jest.fn(),
  };

  const { res, done, getJsonBody, getTextBody } = createTestResponse();
  let nextError = null;
  const next = (error?: unknown) => {
    if (error) {
      nextError = error;
      return;
    }
    done.then(() => undefined).catch(() => undefined);
  };

  (importController as unknown as (req, res, next: (error?: unknown) => void) => void)(req, res, next);

  await Promise.race([done, new Promise<void>((resolve) => setImmediate(resolve))]);

  return {
    res,
    body: getJsonBody(),
    textBody: getTextBody(),
    nextError,
  };
}

describe('Import Controller', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    uploadMiddlewareImpl = (_req, _res, next) => next();
  });

  describe('Jobs', () => {
    it('should create and enqueue an import job from URL', async () => {
      (fetchRemoteImport as jest.Mock).mockResolvedValueOnce({
        key: 'remote-1.ndjson',
        location: '/tmp/imports/remote-1.ndjson',
        bytes: 200,
        fileName: 'remote-1.ndjson',
        mimeType: 'application/x-ndjson',
        sourceType: 'url',
        sourceUrl: 'https://example.com/remote-1.ndjson',
      });
      prisma.importJob.findFirst.mockResolvedValueOnce(null);
      prisma.importJob.create.mockResolvedValueOnce({
        id: 'imp-1',
        status: 'queued',
        resource: 'articles',
        format: 'ndjson',
        totalRecords: null,
        processedRecords: 0,
        successCount: 0,
        errorCount: 0,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: null,
        finishedAt: null,
        fileName: 'remote-1.ndjson',
        fileSize: 200,
        sourceLocation: '/tmp/imports/remote-1.ndjson',
        idempotencyKey: 'idem-import-1',
        errorSummary: null,
      });

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'articles', url: 'https://example.com/remote-1.ndjson' },
        headers: { 'idempotency-key': 'idem-import-1' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(202);
      expect(result.body.importJob.id).toBe('imp-1');
      expect(enqueueImportJob).toHaveBeenCalledWith({
        jobId: 'imp-1',
        resource: 'articles',
        format: 'ndjson',
      });
    });

    it('should return existing import job for same idempotency key', async () => {
      prisma.importJob.findFirst.mockResolvedValueOnce({
        id: 'imp-existing',
        status: 'running',
        resource: 'users',
        format: 'json',
        totalRecords: null,
        processedRecords: 5,
        successCount: 5,
        errorCount: 0,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: new Date('2026-02-06T12:01:00Z'),
        finishedAt: null,
        fileName: 'users.json',
        fileSize: 300,
        sourceLocation: '/tmp/imports/users.json',
        idempotencyKey: 'idem-existing',
        errorSummary: null,
      });

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'users', url: 'https://example.com/users.json' },
        headers: { 'idempotency-key': 'idem-existing' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      expect(result.body.importJob.id).toBe('imp-existing');
      expect(enqueueImportJob).not.toHaveBeenCalled();
    });

    it('should reject when explicit format mismatches file extension', async () => {
      (fetchRemoteImport as jest.Mock).mockResolvedValueOnce({
        key: 'remote-mismatch.json',
        location: '/tmp/imports/remote-mismatch.json',
        bytes: 200,
        fileName: 'remote-mismatch.json',
        mimeType: 'application/json',
        sourceType: 'url',
        sourceUrl: 'https://example.com/remote-mismatch.json',
      });

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'articles', format: 'ndjson', url: 'https://example.com/remote-mismatch.json' },
        headers: { 'idempotency-key': 'idem-format-mismatch' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.UNPROCESSABLE_ENTITY);
      expect(result.nextError.message).toEqual({
        errors: { format: ['format ndjson does not match file extension (.json)'] },
      });
      expect(prisma.importJob.create).not.toHaveBeenCalled();
      expect(enqueueImportJob).not.toHaveBeenCalled();
      expect(fsPromises.rm).toHaveBeenCalledWith('/tmp/imports/remote-mismatch.json', { force: true });
    });

    it('should return existing import job when create races on idempotency key', async () => {
      (fetchRemoteImport as jest.Mock).mockResolvedValueOnce({
        key: 'remote-2.ndjson',
        location: '/tmp/imports/remote-2.ndjson',
        bytes: 200,
        fileName: 'remote-2.ndjson',
        mimeType: 'application/x-ndjson',
        sourceType: 'url',
        sourceUrl: 'https://example.com/remote-2.ndjson',
      });
      prisma.importJob.findFirst
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({
          id: 'imp-race',
          status: 'queued',
          resource: 'articles',
          format: 'ndjson',
          totalRecords: null,
          processedRecords: 0,
          successCount: 0,
          errorCount: 0,
          createdAt: new Date('2026-02-06T12:00:00Z'),
          startedAt: null,
          finishedAt: null,
          fileName: 'remote-2.ndjson',
          fileSize: 200,
          sourceLocation: '/tmp/imports/existing-remote-2.ndjson',
          idempotencyKey: 'idem-race',
          errorSummary: null,
        });
      prisma.importJob.create.mockRejectedValueOnce(
        Object.assign(new Error('Unique constraint failed'), {
          code: 'P2002',
          clientVersion: 'test',
        }),
      );

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'articles', url: 'https://example.com/remote-2.ndjson' },
        headers: { 'idempotency-key': 'idem-race' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      expect(result.body.importJob.id).toBe('imp-race');
      expect(enqueueImportJob).not.toHaveBeenCalled();
      expect(fsPromises.rm).toHaveBeenCalledWith('/tmp/imports/remote-2.ndjson', { force: true });
    });

    it('should mark import job failed when enqueue fails', async () => {
      (fetchRemoteImport as jest.Mock).mockResolvedValueOnce({
        key: 'remote-3.ndjson',
        location: '/tmp/imports/remote-3.ndjson',
        bytes: 200,
        fileName: 'remote-3.ndjson',
        mimeType: 'application/x-ndjson',
        sourceType: 'url',
        sourceUrl: 'https://example.com/remote-3.ndjson',
      });
      prisma.importJob.findFirst.mockResolvedValueOnce(null);
      prisma.importJob.create.mockResolvedValueOnce({
        id: 'imp-enqueue-failed',
        status: 'queued',
        resource: 'articles',
        format: 'ndjson',
        totalRecords: null,
        processedRecords: 0,
        successCount: 0,
        errorCount: 0,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: null,
        finishedAt: null,
        fileName: 'remote-3.ndjson',
        fileSize: 200,
        sourceLocation: '/tmp/imports/remote-3.ndjson',
        idempotencyKey: 'idem-enqueue-failed',
        errorSummary: null,
      });
      (enqueueImportJob as jest.Mock).mockRejectedValueOnce(new Error('queue unavailable'));

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'articles', url: 'https://example.com/remote-3.ndjson' },
        headers: { 'idempotency-key': 'idem-enqueue-failed' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.SERVICE_UNAVAILABLE);
      expect(prisma.importJob.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'imp-enqueue-failed' },
          data: expect.objectContaining({
            status: 'failed',
          }),
        }),
      );
      expect(fsPromises.rm).toHaveBeenCalledWith('/tmp/imports/remote-3.ndjson', { force: true });
    });

    it('should map intake URL fetch failures to service unavailable', async () => {
      (fetchRemoteImport as jest.Mock).mockRejectedValueOnce(
        new ImportExportError(FileErrorCode.URL_FETCH_FAILED, 'Failed to fetch remote file'),
      );

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'users', url: 'https://example.com/users.json' },
        headers: { 'idempotency-key': 'idem-url-fetch-fail' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.SERVICE_UNAVAILABLE);
      expect(result.nextError.message).toEqual({ errors: { source: ['Failed to fetch remote file'] } });
    });

    it('should require idempotency key header for import create', async () => {
      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        body: { resource: 'users', url: 'https://example.com/users.json' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.UNPROCESSABLE_ENTITY);
      expect(result.nextError.message).toEqual({
        errors: { idempotencyKey: ['Idempotency-Key header is required'] },
      });
      expect(prisma.importJob.findFirst).not.toHaveBeenCalled();
      expect(enqueueImportJob).not.toHaveBeenCalled();
    });

    it('should map unsupported upload content type to 422', async () => {
      uploadMiddlewareImpl = (_req, _res, next) =>
        next(new ImportExportError(FileErrorCode.UNSUPPORTED_FORMAT, 'Unsupported content type'));

      const result = await runRoute({
        method: 'POST',
        url: '/v1/imports',
        headers: { 'idempotency-key': 'idem-upload-content-type' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.UNPROCESSABLE_ENTITY);
      expect(result.nextError.message).toEqual({
        errors: { file: ['Unsupported content type'] },
      });
      expect(prisma.importJob.create).not.toHaveBeenCalled();
      expect(enqueueImportJob).not.toHaveBeenCalled();
    });

    it('should return import job status with report metadata and no inline preview', async () => {
      prisma.importJob.findFirst.mockResolvedValueOnce({
        id: 'imp-2',
        status: 'partial',
        resource: 'comments',
        format: 'ndjson',
        totalRecords: 10,
        processedRecords: 10,
        successCount: 9,
        errorCount: 1,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: new Date('2026-02-06T12:00:10Z'),
        finishedAt: new Date('2026-02-06T12:00:50Z'),
        fileName: 'comments.ndjson',
        fileSize: 123,
        sourceLocation: '/tmp/imports/comments.ndjson',
        idempotencyKey: 'idem-2',
        errorSummary: {
          reportLocation: '/tmp/import-errors/imp-2.ndjson',
          reportStatus: 'complete',
          persistedErrorCount: 1,
          reportFormat: 'ndjson',
          lastError: {
            code: 3001,
            message: 'Invalid JSON payload',
          },
        },
      });

      const result = await runRoute({
        method: 'GET',
        url: '/v1/imports/imp-2',
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      const body = result.body;
      expect(body.importJob.id).toBe('imp-2');
      expect(body.errorReportUrl).toBe('/api/v1/imports/imp-2/errors/download');
      expect(body.errorReportStatus).toBe('complete');
      expect(body.importJob.errorSummary.reportLocation).toBeUndefined();
      expect(body.importJob.errorSummary.lastError).toEqual({
        code: 3001,
        message: 'Invalid JSON payload',
      });
      expect(prisma.importError.findMany).not.toHaveBeenCalled();
    });

    it('should stream full import error report from download endpoint', async () => {
      prisma.importJob.findFirst.mockResolvedValueOnce({
        id: 'imp-4',
        status: 'partial',
        resource: 'comments',
        format: 'ndjson',
        totalRecords: 10,
        processedRecords: 10,
        successCount: 9,
        errorCount: 1,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: new Date('2026-02-06T12:00:10Z'),
        finishedAt: new Date('2026-02-06T12:00:50Z'),
        fileName: 'comments.ndjson',
        fileSize: 123,
        sourceLocation: '/tmp/imports/comments.ndjson',
        idempotencyKey: 'idem-4',
        errorSummary: {
          reportLocation: '/tmp/import-errors/imp-4.ndjson',
          reportStatus: 'complete',
          reportFormat: 'ndjson',
        },
      });

      const result = await runRoute({
        method: 'GET',
        url: '/v1/imports/imp-4/errors/download',
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      expect(result.res.headers['Content-Type']).toBe('application/x-ndjson');
      expect(result.textBody).toContain('"recordIndex":3');
      expect(createReadStream).toHaveBeenCalledWith('/tmp/import-errors/imp-4.ndjson');
    });
  });
});
