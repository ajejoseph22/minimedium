import prismaMock from '../../prisma-mock';
import importController from '../../../app/routes/imports/import.controller';
import { enqueueImportJob } from '../../../app/jobs/import-export.queue';
import { fetchRemoteImport } from '../../../app/routes/imports/intake.service';
import { createReadStream } from 'fs';
import { createTestResponse } from '../../helpers/test-response';

jest.mock('../../../app/routes/auth/auth', () => ({
  __esModule: true,
  default: { required: (_req: any, _res: any, next: any) => next() },
}));

jest.mock('../../../app/jobs/import-export.queue', () => ({
  enqueueImportJob: jest.fn().mockResolvedValue({ id: 'queue-import' }),
  enqueueExportJob: jest.fn().mockResolvedValue({ id: 'queue-export' }),
}));

jest.mock('../../../app/routes/imports/intake.service', () => ({
  fetchRemoteImport: jest.fn(),
  mapUploadedFileToImportIntakeResult: jest.fn(),
  validateUploadedFile: jest.fn(),
}));

jest.mock('fs', () => ({
  createReadStream: jest.fn(() => ({
    on: jest.fn().mockReturnThis(),
    pipe: jest.fn((res: any) => {
      res.write('{"recordIndex":3,"message":"user_id does not exist"}\n');
      res.end();
      return res;
    }),
  })),
}));

const prisma = prismaMock as unknown as any;

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
  const req: any = {
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
  let nextError: any = null;
  const next = (error?: unknown) => {
    if (error) {
      nextError = error;
      return;
    }
    done.then(() => undefined).catch(() => undefined);
  };

  (importController as unknown as (req: any, res: any, next: (error?: unknown) => void) => void)(req, res, next);

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
      expect((result.body as any).importJob.id).toBe('imp-1');
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
      expect((result.body as any).importJob.id).toBe('imp-existing');
      expect(enqueueImportJob).not.toHaveBeenCalled();
    });

    it('should return import job status with error preview and report URL', async () => {
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
        },
      });
      prisma.importError.findMany.mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 3,
          recordId: 'c-3',
          errorCode: 1008,
          errorName: 'INVALID_REFERENCE',
          message: 'user_id does not exist',
          field: 'user_id',
          value: '999',
          details: null,
          createdAt: new Date('2026-02-06T12:00:30Z'),
        },
      ]);

      const result = await runRoute({
        method: 'GET',
        url: '/v1/imports/imp-2',
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      const body = result.body as any;
      expect(body.importJob.id).toBe('imp-2');
      expect(body.errorsPreview).toHaveLength(1);
      expect(body.errorsPreviewCount).toBe(1);
      expect(body.hasMoreErrors).toBe(false);
      expect(body.errorReportUrl).toBe('/api/v1/imports/imp-2/errors/download');
      expect(body.errorReportStatus).toBe('complete');
      expect(body.importJob.errorSummary.reportLocation).toBeUndefined();
    });

    it('should enforce error preview default and maximum', async () => {
      prisma.importJob.findFirst.mockResolvedValue({
        id: 'imp-3',
        status: 'partial',
        resource: 'users',
        format: 'json',
        totalRecords: 200,
        processedRecords: 200,
        successCount: 100,
        errorCount: 100,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: new Date('2026-02-06T12:00:10Z'),
        finishedAt: new Date('2026-02-06T12:00:50Z'),
        fileName: 'users.json',
        fileSize: 123,
        sourceLocation: '/tmp/imports/users.json',
        idempotencyKey: 'idem-3',
        errorSummary: { persistedErrorCount: 100, reportStatus: 'complete' },
      });
      prisma.importError.findMany.mockResolvedValue([]);

      await runRoute({
        method: 'GET',
        url: '/v1/imports/imp-3',
        auth: { user: { id: 42 } },
      });

      expect(prisma.importError.findMany).toHaveBeenCalledWith(
        expect.objectContaining({
          take: 20,
        }),
      );

      const tooLarge = await runRoute({
        method: 'GET',
        url: '/v1/imports/imp-3',
        query: { errorPreviewLimit: '101' },
        auth: { user: { id: 42 } },
      });

      expect(tooLarge.nextError).not.toBeNull();
      expect(tooLarge.nextError.errorCode).toBe(422);
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
