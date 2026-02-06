import prismaMock from '../prisma-mock';
import importExportController from '../../app/routes/import-export/import-export.controller';
import { enqueueExportJob, enqueueImportJob } from '../../app/jobs/import-export.queue';
import { fetchRemoteImport } from '../../app/routes/import-export/intake.service';

jest.mock('../../app/routes/auth/auth', () => ({
  __esModule: true,
  default: { required: (_req: any, _res: any, next: any) => next() },
}));

jest.mock('../../app/routes/import-export/config', () => ({
  loadConfig: () => ({
    exportStreamMaxLimit: 1000,
    batchSize: 1000,
    fileRetentionHours: 24,
  }),
}));

jest.mock('../../app/routes/import-export/export.service', () => ({
  streamExportRecords: jest.fn(),
}));

jest.mock('../../app/jobs/import-export.queue', () => ({
  enqueueImportJob: jest.fn().mockResolvedValue({ id: 'queue-import' }),
  enqueueExportJob: jest.fn().mockResolvedValue({ id: 'queue-export' }),
}));

jest.mock('../../app/routes/import-export/intake.service', () => ({
  createImportUploadMiddleware: () => (_req: any, _res: any, next: any) => next(),
  fetchRemoteImport: jest.fn(),
  mapUploadedFileToImportIntakeResult: jest.fn(),
  validateUploadedFile: jest.fn(),
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

function createTestResponse() {
  let resolveDone: (() => void) | null = null;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });
  let jsonBody: unknown;
  let textBody = '';

  const res: any = {
    statusCode: 200,
    headersSent: false,
    headers: {} as Record<string, string>,
    status(code: number) {
      this.statusCode = code;
      return this;
    },
    json(payload: unknown) {
      this.headersSent = true;
      jsonBody = payload;
      resolveDone?.();
      return this;
    },
    setHeader(key: string, value: string) {
      this.headers[key] = value;
    },
    write(chunk: string) {
      this.headersSent = true;
      textBody += chunk;
      return true;
    },
    end() {
      resolveDone?.();
      return this;
    },
  };

  return { res, done, getJsonBody: () => jsonBody, getTextBody: () => textBody };
}

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
    }
  };

  (
    importExportController as unknown as (
      req: any,
      res: any,
      next: (error?: unknown) => void
    ) => void
  )(req, res, next);

  await Promise.race([done, new Promise<void>((resolve) => setImmediate(resolve))]);

  return {
    res,
    body: getJsonBody(),
    textBody: getTextBody(),
    nextError,
  };
}

describe('Import/Export Jobs Controller', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

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

  it('should return import job status with inline errors and report URL', async () => {
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
      errorSummary: { reportLocation: '/tmp/reports/imp-2.ndjson' },
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
    expect((result.body as any).importJob.id).toBe('imp-2');
    expect((result.body as any).errors).toHaveLength(1);
    expect((result.body as any).errorReportUrl).toBe('/tmp/reports/imp-2.ndjson');
  });

  it('should create and enqueue an export job', async () => {
    prisma.exportJob.findFirst.mockResolvedValueOnce(null);
    prisma.exportJob.create.mockResolvedValueOnce({
      id: 'exp-1',
      status: 'queued',
      resource: 'articles',
      format: 'json',
      totalRecords: null,
      processedRecords: 0,
      createdAt: new Date('2026-02-06T12:00:00Z'),
      startedAt: null,
      finishedAt: null,
      expiresAt: null,
      idempotencyKey: 'idem-exp-1',
      outputLocation: null,
      downloadUrl: null,
      fileSize: null,
    });

    const result = await runRoute({
      method: 'POST',
      url: '/v1/exports',
      body: {
        resource: 'articles',
        format: 'json',
        filters: { status: 'published' },
        fields: ['id', 'slug'],
      },
      headers: { 'idempotency-key': 'idem-exp-1' },
      auth: { user: { id: 42 } },
    });

    expect(result.nextError).toBeNull();
    expect(result.res.statusCode).toBe(202);
    expect((result.body as any).exportJob.id).toBe('exp-1');
    expect(enqueueExportJob).toHaveBeenCalledWith({
      jobId: 'exp-1',
      resource: 'articles',
      format: 'json',
    });
  });

  it('should return export job status with download URL', async () => {
    prisma.exportJob.findFirst.mockResolvedValueOnce({
      id: 'exp-2',
      status: 'succeeded',
      resource: 'users',
      format: 'ndjson',
      totalRecords: 4,
      processedRecords: 4,
      createdAt: new Date('2026-02-06T12:00:00Z'),
      startedAt: new Date('2026-02-06T12:00:01Z'),
      finishedAt: new Date('2026-02-06T12:00:10Z'),
      expiresAt: new Date('2026-02-07T12:00:10Z'),
      idempotencyKey: 'idem-exp-2',
      outputLocation: '/tmp/exports/exp-2.ndjson',
      downloadUrl: '/api/v1/exports/exp-2/download',
      fileSize: 800,
    });

    const result = await runRoute({
      method: 'GET',
      url: '/v1/exports/exp-2',
      auth: { user: { id: 42 } },
    });

    expect(result.nextError).toBeNull();
    expect(result.res.statusCode).toBe(200);
    expect((result.body as any).exportJob.id).toBe('exp-2');
    expect((result.body as any).exportJob.downloadUrl).toBe('/api/v1/exports/exp-2/download');
  });
});
