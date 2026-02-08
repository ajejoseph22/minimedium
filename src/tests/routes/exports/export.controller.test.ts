import prismaMock from '../../prisma-mock';
import exportController from '../../../app/routes/exports/export.controller';
import { enqueueExportJob } from '../../../app/jobs/import-export.queue';
import { streamExportRecords } from '../../../app/routes/exports/export.service';
import { createTestResponse } from '../../helpers/test-response';

jest.mock('../../../app/routes/auth/auth', () => ({
  __esModule: true,
  default: { required: (_req, _res, next) => next() },
}));

jest.mock('../../../app/routes/exports/config', () => ({
  loadExportConfig: () => ({
    exportStreamMaxLimit: 2,
    batchSize: 1000,
    workerConcurrency: 4,
    fileRetentionHours: 24,
    exportStoragePath: './exports',
    exportRateLimitPerHour: 20,
    exportConcurrentLimitUser: 5,
    exportConcurrentLimitGlobal: 20,
  }),
}));

jest.mock('../../../app/routes/exports/export.service', () => ({
  streamExportRecords: jest.fn(),
}));

jest.mock('../../../app/jobs/import-export.queue', () => ({
  enqueueImportJob: jest.fn().mockResolvedValue({ id: 'queue-import' }),
  enqueueExportJob: jest.fn().mockResolvedValue({ id: 'queue-export' }),
}));

const prisma: any = prismaMock;

type RunRouteOptions = {
  method: string;
  url: string;
  query?: Record<string, unknown>;
  body?: unknown;
  headers?: Record<string, string>;
  auth?: { user?: { id?: number } };
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
    }
  };

  (exportController as unknown as (req: any, res: any, next: (error?: unknown) => void) => void)(req, res, next);

  await Promise.race([done, new Promise<void>((resolve) => setImmediate(resolve))]);

  return {
    res,
    body: getJsonBody(),
    textBody: getTextBody(),
    nextError,
  };
}

describe('Export Controller', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Streaming', () => {
    it('should stream JSON with nextCursor when limit is reached', async () => {
      (streamExportRecords as jest.Mock).mockImplementation(async function* () {
        yield {
          id: 1,
          email: 'first@example.com',
          name: 'First',
          role: 'user',
          active: true,
          created_at: '2026-02-05T00:00:00Z',
          updated_at: '2026-02-05T00:00:00Z',
        };
        yield {
          id: 2,
          email: 'second@example.com',
          name: 'Second',
          role: 'user',
          active: true,
          created_at: '2026-02-05T00:00:00Z',
          updated_at: '2026-02-05T00:00:00Z',
        };
      });

      const result = await runRoute({
        method: 'GET',
        url: '/v1/exports',
        query: { resource: 'users', format: 'json', limit: '2' },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      const parsed = JSON.parse(result.textBody);
      expect(parsed.data).toHaveLength(2);
      expect(parsed.nextCursor).toBe(2);
    });

    it('should stream NDJSON with a trailing cursor line', async () => {
      (streamExportRecords as jest.Mock).mockImplementation(async function* () {
        yield {
          id: 1,
          email: 'first@example.com',
          name: 'First',
          role: 'user',
          active: true,
          created_at: '2026-02-05T00:00:00Z',
          updated_at: '2026-02-05T00:00:00Z',
        };
      });

      const result = await runRoute({
        method: 'GET',
        url: '/v1/exports',
        query: { resource: 'users', format: 'ndjson', limit: '1' },
      });

      const lines = result.textBody.trim().split('\n');
      expect(lines).toHaveLength(2);
      const cursorLine = JSON.parse(lines[1] ?? '{}');
      expect(cursorLine._type).toBe('cursor');
      expect(cursorLine.nextCursor).toBe(1);
    });

    it('should reject limits above the max', async () => {
      const result = await runRoute({
        method: 'GET',
        url: '/v1/exports',
        query: { resource: 'users', limit: '10' },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(422);
    });
  });

  describe('Jobs', () => {
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
});
