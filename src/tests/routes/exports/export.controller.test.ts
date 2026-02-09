import prismaMock from '../../prisma-mock';
import exportController from '../../../app/routes/exports/export.controller';
import { enqueueExportJob } from '../../../app/jobs/import-export.queue';
import { streamExports } from '../../../app/routes/exports/export.service';
import { createTestResponse } from '../../helpers/test-response';
import { HttpStatusCode } from '../../../app/models/http-status-code.model';

jest.mock('../../../app/routes/auth/auth', () => ({
  __esModule: true,
  default: { required: (_req, _res, next) => next() },
}));

jest.mock('../../../app/routes/exports/config', () => ({
  loadExportConfig: () => ({
    exportStreamMaxLimit: 2,
    exportMaxRecords: 2,
    batchSize: 1000,
    workerConcurrency: 4,
    fileRetentionHours: 24,
    exportStoragePath: './exports',
    exportRateLimitPerHour: 20,
    exportConcurrentLimitUser: 5,
    exportConcurrentLimitGlobal: 20,
  }),
}));

jest.mock('../../../app/routes/exports/export.service', () => {
  const actual = jest.requireActual('../../../app/routes/exports/export.service');
  return {
    ...actual,
    streamExports: jest.fn(),
  };
});

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
      (streamExports as jest.Mock).mockImplementation(async ({ writeChunk }) => {
        await writeChunk('{"data":[{"id":1},{"id":2}],"nextCursor":2}');
        return { count: 2, lastId: 2 };
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
      (streamExports as jest.Mock).mockImplementation(async ({ writeChunk }) => {
        await writeChunk('{"id":1}\n');
        await writeChunk('{"_type":"cursor","nextCursor":1}\n');
        return { count: 1, lastId: 1 };
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

    it('should pass normalized filters and fields to streaming service', async () => {
      (streamExports as jest.Mock).mockImplementation(async ({ writeChunk }) => {
        await writeChunk('{"id":1}\n');
        await writeChunk('{"_type":"cursor","nextCursor":1}\n');
        return { count: 1, lastId: 1 };
      });

      const result = await runRoute({
        method: 'GET',
        url: '/v1/exports',
        query: {
          resource: 'articles',
          format: 'ndjson',
          limit: '1',
          filters: '{"status":"published","authorId":"13"}',
          fields: 'id,slug,publishedAt',
        },
      });

      expect(result.nextError).toBeNull();
      expect(streamExports).toHaveBeenCalledWith(
        expect.objectContaining({
          entityType: 'articles',
          filters: {
            status: 'published',
            author_id: 13,
          },
          fields: new Set(['id', 'slug', 'published_at']),
        }),
      );
    });

    it('should reject limits above the max', async () => {
      const result = await runRoute({
        method: 'GET',
        url: '/v1/exports',
        query: { resource: 'users', limit: '10' },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.UNPROCESSABLE_ENTITY);
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
      expect(result.body.exportJob.id).toBe('exp-1');
      expect(enqueueExportJob).toHaveBeenCalledWith({
        jobId: 'exp-1',
        resource: 'articles',
        format: 'json',
      });
    });

    it('should return existing export job when create races on idempotency key', async () => {
      prisma.exportJob.findFirst
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({
          id: 'exp-race',
          status: 'queued',
          resource: 'articles',
          format: 'json',
          totalRecords: null,
          processedRecords: 0,
          createdAt: new Date('2026-02-06T12:00:00Z'),
          startedAt: null,
          finishedAt: null,
          expiresAt: null,
          idempotencyKey: 'idem-exp-race',
          outputLocation: null,
          downloadUrl: null,
          fileSize: null,
        });
      prisma.exportJob.create.mockRejectedValueOnce(
        Object.assign(new Error('Unique constraint failed'), {
          code: 'P2002',
          clientVersion: 'test',
        }),
      );

      const result = await runRoute({
        method: 'POST',
        url: '/v1/exports',
        body: { resource: 'articles', format: 'json' },
        headers: { 'idempotency-key': 'idem-exp-race' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      expect(result.body.exportJob.id).toBe('exp-race');
      expect(enqueueExportJob).not.toHaveBeenCalled();
    });

    it('should mark export job failed when enqueue fails', async () => {
      prisma.exportJob.findFirst.mockResolvedValueOnce(null);
      prisma.exportJob.create.mockResolvedValueOnce({
        id: 'exp-enqueue-failed',
        status: 'queued',
        resource: 'articles',
        format: 'json',
        totalRecords: null,
        processedRecords: 0,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: null,
        finishedAt: null,
        expiresAt: null,
        idempotencyKey: 'idem-exp-enqueue-failed',
        outputLocation: null,
        downloadUrl: null,
        fileSize: null,
      });
      (enqueueExportJob as jest.Mock).mockRejectedValueOnce(new Error('queue unavailable'));

      const result = await runRoute({
        method: 'POST',
        url: '/v1/exports',
        body: { resource: 'articles', format: 'json' },
        headers: { 'idempotency-key': 'idem-exp-enqueue-failed' },
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).not.toBeNull();
      expect(result.nextError.errorCode).toBe(HttpStatusCode.SERVICE_UNAVAILABLE);
      expect(prisma.exportJob.update).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 'exp-enqueue-failed' },
          data: expect.objectContaining({
            status: 'failed',
          }),
        }),
      );
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
      expect(result.body.exportJob.id).toBe('exp-2');
      expect(result.body.exportJob.downloadUrl).toBe('/api/v1/exports/exp-2/download');
    });

    it('should include truncation metadata when export hits max record limit', async () => {
      prisma.exportJob.findFirst.mockResolvedValueOnce({
        id: 'exp-truncated',
        status: 'succeeded',
        resource: 'users',
        format: 'ndjson',
        totalRecords: 3,
        processedRecords: 2,
        createdAt: new Date('2026-02-06T12:00:00Z'),
        startedAt: new Date('2026-02-06T12:00:01Z'),
        finishedAt: new Date('2026-02-06T12:00:10Z'),
        expiresAt: new Date('2026-02-07T12:00:10Z'),
        idempotencyKey: 'idem-exp-truncated',
        outputLocation: '/tmp/exports/exp-truncated.ndjson',
        downloadUrl: '/api/v1/exports/exp-truncated/download',
        fileSize: 800,
      });

      const result = await runRoute({
        method: 'GET',
        url: '/v1/exports/exp-truncated',
        auth: { user: { id: 42 } },
      });

      expect(result.nextError).toBeNull();
      expect(result.res.statusCode).toBe(200);
      expect(result.body.exportJob.truncated).toBe(true);
      expect(result.body.exportJob.recordLimit).toBe(2);
      expect(result.body.exportJob.reason).toBe('max_records_reached');
    });
  });
});
