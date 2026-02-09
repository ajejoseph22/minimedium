import prismaMock from '../../prisma-mock';
import {
  parseExportQuery,
  runExportJob,
  streamExportRecords,
  streamExports,
} from '../../../app/routes/exports/export.service';
import { logJobLifecycleEvent } from '../../../app/jobs/observability';
import { createMemoryStorageAdapter } from '../../helpers/memory-storage';
import HttpException from '../../../app/models/http-exception.model';
import { HttpStatusCode } from '../../../app/models/http-status-code.model';

jest.mock('../../../app/routes/exports/config', () => ({
  loadExportConfig: () => ({
    batchSize: 1,
    exportMaxRecords: 2,
    exportStreamMaxLimit: 2,
    workerConcurrency: 4,
    fileRetentionHours: 24,
    exportStoragePath: './exports',
    exportRateLimitPerHour: 20,
    exportConcurrentLimitUser: 5,
    exportConcurrentLimitGlobal: 20,
  }),
}));

jest.mock('../../../app/jobs/observability', () => ({
  logJobLifecycleEvent: jest.fn(),
}));

const prisma: any = prismaMock;
const logJobLifecycleEventMock = logJobLifecycleEvent as jest.MockedFunction<typeof logJobLifecycleEvent>;

describe('Export Service', () => {
  it('should stream, at most, the requested limit', async () => {
    prisma.user.findMany
      .mockResolvedValueOnce([
        {
          id: 1,
          email: 'first@example.com',
          name: 'First',
          username: 'first',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 2,
          email: 'second@example.com',
          name: 'Second',
          username: 'second',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 3,
          email: 'third@example.com',
          name: 'Third',
          username: 'third',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ]);

    const records = [];
    for await (const record of streamExportRecords({
      prisma,
      entityType: 'users',
      limit: 2,
    })) {
      records.push(record);
    }

    expect(records).toHaveLength(2);
    expect(records[0]?.id).toBe(1);
    expect(records[1]?.id).toBe(2);
  });

  it('should honor cursor and fetch records after it', async () => {
    prisma.user.findMany.mockResolvedValueOnce([]);

    const records = [];
    for await (const record of streamExportRecords({
      prisma,
      entityType: 'users',
      limit: 1,
      cursor: 10,
    })) {
      records.push(record);
    }

    expect(records).toHaveLength(0);
    expect(prisma.user.findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: { gt: 10 } },
      }),
    );
  });

  it('should default to exportStreamMaxLimit when limit is omitted', async () => {
    prisma.user.findMany
      .mockResolvedValueOnce([
        {
          id: 1,
          email: 'first@example.com',
          name: 'First',
          username: 'first',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 2,
          email: 'second@example.com',
          name: 'Second',
          username: 'second',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 3,
          email: 'third@example.com',
          name: 'Third',
          username: 'third',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([]);

    const records = [];
    for await (const record of streamExportRecords({
      prisma,
      entityType: 'users',
    })) {
      records.push(record);
    }

    expect(records).toHaveLength(2);
  });

  it('should parse and normalize streaming filters and fields from query params', () => {
    const parsed = parseExportQuery({
      resource: 'articles',
      format: 'json',
      limit: '2',
      filters:
        '{"status":"published","authorId":"13","publishedAt":{"gte":"2026-01-01T00:00:00Z"},"createdAt":{"lte":"2026-02-01T00:00:00Z"}}',
      fields: 'id,slug,publishedAt',
    });

    expect(parsed.filters).toEqual({
      status: 'published',
      author_id: 13,
      published_at: { gte: '2026-01-01T00:00:00Z' },
      created_at: { lte: '2026-02-01T00:00:00Z' },
    });
    expect(Array.from(parsed.fields ?? [])).toEqual([
      'id',
      'slug',
      'published_at',
    ]);
  });

  it('should reject invalid filters JSON in streaming query params', () => {
    expect(() =>
      parseExportQuery({
        resource: 'articles',
        format: 'json',
        filters: '{"status":"published"',
      }),
    ).toThrow(HttpException);

    try {
      parseExportQuery({
        resource: 'articles',
        format: 'json',
        filters: '{"status":"published"',
      });
    } catch (error) {
      const typedError = error as HttpException;
      expect(typedError.errorCode).toBe(HttpStatusCode.UNPROCESSABLE_ENTITY);
      expect(typedError.message).toEqual(
        expect.objectContaining({
          errors: expect.objectContaining({
            filters: ['filters must be a valid JSON object'],
          }),
        }),
      );
    }
  });

  it('should apply filters and fields projection for streaming exports', async () => {
    const chunks: string[] = [];
    const streamRecords = jest.fn(async function* ({ filters }: { filters?: Record<string, unknown> | null }) {
      expect(filters).toEqual({ status: 'published' });
      yield {
        id: 101,
        slug: 'first-post',
        title: 'First Post',
        body: 'Hidden body',
        author_id: 42,
        tags: ['import'],
        published_at: '2026-02-09T00:00:00.000Z',
        status: 'published',
      };
    });

    const result = await streamExports({
      entityType: 'articles',
      format: 'json',
      limit: 1,
      cursor: null,
      filters: { status: 'published' },
      fields: new Set(['id', 'slug', 'status']),
      writeChunk: async (chunk) => {
        chunks.push(chunk);
      },
      streamRecords,
    });

    expect(result).toEqual({ count: 1, lastId: 101 });
    const body = chunks.join('');
    const parsed = JSON.parse(body);
    expect(parsed.data).toEqual([
      {
        id: 101,
        slug: 'first-post',
        status: 'published',
      },
    ]);
    expect(parsed.nextCursor).toBe(101);
  });

  it('should map created_at and published_at range filters to Prisma where clauses', async () => {
    prisma.article.findMany.mockResolvedValueOnce([]);

    const records: Array<{ id: number }> = [];
    for await (const record of streamExportRecords({
      prisma,
      entityType: 'articles',
      limit: 1,
      filters: {
        created_at: { gte: '2026-01-01T00:00:00Z', lt: '2026-02-01T00:00:00Z' },
        published_at: { gte: '2026-01-15T00:00:00Z' },
      },
    })) {
      records.push({ id: record.id });
    }

    expect(records).toHaveLength(0);
    expect(prisma.article.findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          createdAt: expect.objectContaining({
            gte: expect.any(Date),
            lt: expect.any(Date),
          }),
          publishedAt: expect.objectContaining({
            gte: expect.any(Date),
          }),
        }),
      }),
    );
  });
});

describe('runExportJob', () => {
  const fixedNow = new Date('2026-02-06T10:00:00.000Z');
  const now = () => fixedNow;

  beforeEach(() => {
    logJobLifecycleEventMock.mockClear();
    prisma.exportJob.updateMany.mockResolvedValue({ count: 1 });
  });

  it('should export NDJSON, persist metadata, and set download URL on success', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-1',
      status: 'queued',
      processedRecords: 0,
      fileSize: null,
      startedAt: null,
      resource: 'users',
      format: 'ndjson',
      outputLocation: null,
    });
    prisma.user.findMany
      .mockResolvedValueOnce([
        {
          id: 1,
          email: 'first@example.com',
          name: 'First',
          username: 'first',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([]);

    const result = await runExportJob('job-1', {
      prisma,
      storage,
      now,
      cancelCheckInterval: 0,
    });

    expect(result.status).toBe('succeeded');
    expect(result.processedRecords).toBe(1);
    expect(savedFiles).toHaveLength(1);
    expect(savedFiles[0]?.data).toContain('"email":"first@example.com"');
    expect(savedFiles[0]?.data).toContain('\n');
    expect(prisma.exportJob.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'succeeded',
          processedRecords: 1,
          totalRecords: 1,
          downloadUrl: '/api/v1/exports/job-1/download',
          outputLocation: expect.stringContaining('/tmp/job-1.ndjson'),
          fileSize: expect.any(Number),
          expiresAt: expect.any(Date),
        }),
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenCalledTimes(2);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        event: 'job.started',
        jobKind: 'export',
        jobId: 'job-1',
        status: 'running',
        counters: expect.objectContaining({
          processedRecords: 0,
          errorCount: 0,
        }),
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'export',
        jobId: 'job-1',
        status: 'succeeded',
        jobStartedAt: expect.any(Date),
        counters: expect.objectContaining({
          processedRecords: 1,
          errorCount: 0,
        }),
      }),
    );
  });

  it('should export JSON array output when format is json', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-2',
      status: 'queued',
      processedRecords: 0,
      fileSize: null,
      startedAt: null,
      resource: 'users',
      format: 'json',
      outputLocation: null,
    });
    prisma.user.findMany
      .mockResolvedValueOnce([
        {
          id: 7,
          email: 'json@example.com',
          name: 'Json',
          username: 'json',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([]);

    await runExportJob('job-2', {
      prisma,
      storage,
      now,
      cancelCheckInterval: 0,
    });

    expect(savedFiles).toHaveLength(1);
    expect(savedFiles[0]?.data.startsWith('[')).toBe(true);
    expect(savedFiles[0]?.data.endsWith(']')).toBe(true);
  });

  it('should apply async export filters and fields selection', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-filter-fields',
      status: 'queued',
      processedRecords: 0,
      fileSize: null,
      startedAt: null,
      resource: 'articles',
      format: 'ndjson',
      outputLocation: null,
      filters: { status: 'published', author_id: 13 },
      fields: ['id', 'slug', 'title', 'status', 'published_at'],
    });
    prisma.article.findMany
      .mockResolvedValueOnce([
        {
          id: 21,
          slug: 'filtered-article',
          title: 'Filtered Article',
          body: 'Should not be exported',
          authorId: 13,
          publishedAt: new Date('2026-02-05T00:00:00Z'),
          status: 'published',
          tagList: [{ name: 'backend' }],
        },
      ])
      .mockResolvedValueOnce([]);

    const result = await runExportJob('job-filter-fields', {
      prisma,
      storage,
      now,
      cancelCheckInterval: 0,
    });

    expect(result.status).toBe('succeeded');
    expect(result.processedRecords).toBe(1);
    expect(prisma.article.findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          status: 'published',
          authorId: 13,
        }),
      }),
    );

    const rows = savedFiles[0]?.data.trim().split('\n') ?? [];
    expect(rows).toHaveLength(1);
    const exported = JSON.parse(rows[0] as string);
    expect(Object.keys(exported).sort()).toEqual(
      ['id', 'slug', 'title', 'status', 'published_at'].sort(),
    );
    expect(exported).not.toHaveProperty('body');
    expect(exported).not.toHaveProperty('author_id');
    expect(exported).not.toHaveProperty('tags');
  });

  it('should cap async export at exportMaxRecords and mark metadata as truncated', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-truncated',
      status: 'queued',
      processedRecords: 0,
      fileSize: null,
      startedAt: null,
      resource: 'users',
      format: 'ndjson',
      outputLocation: null,
    });
    prisma.user.findMany
      .mockResolvedValueOnce([
        {
          id: 1,
          email: 'first@example.com',
          name: 'First',
          username: 'first',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 2,
          email: 'second@example.com',
          name: 'Second',
          username: 'second',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 3,
          email: 'third@example.com',
          name: 'Third',
          username: 'third',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ]);

    const result = await runExportJob('job-truncated', {
      prisma,
      storage,
      now,
      cancelCheckInterval: 0,
    });

    expect(result.status).toBe('succeeded');
    expect(result.processedRecords).toBe(2);
    expect(savedFiles).toHaveLength(1);
    expect(savedFiles[0]?.data).toContain('"id":1');
    expect(savedFiles[0]?.data).toContain('"id":2');
    expect(savedFiles[0]?.data).not.toContain('"id":3');

    expect(prisma.exportJob.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-truncated' },
        data: expect.objectContaining({
          status: 'succeeded',
          processedRecords: 2,
          totalRecords: 3,
        }),
      }),
    );

    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'export',
        jobId: 'job-truncated',
        status: 'succeeded',
        details: expect.objectContaining({
          truncated: true,
          recordLimit: 2,
          reason: 'max_records_reached',
        }),
      }),
    );
  });

  it('should return cancelled immediately when job is already cancelled', async () => {
    prisma.exportJob.updateMany.mockResolvedValue({ count: 1 });
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-3',
      status: 'cancelled',
      processedRecords: 9,
      fileSize: 123,
      startedAt: null,
      resource: 'users',
      format: 'ndjson',
      outputLocation: null,
    });

    const result = await runExportJob('job-3', {
      prisma,
      now,
      cancelCheckInterval: 1,
    });

    expect(result).toEqual({
      status: 'cancelled',
      processedRecords: 9,
      fileSize: 123,
    });
    expect(prisma.exportJob.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-3' },
        data: expect.objectContaining({
          status: 'cancelled',
          finishedAt: fixedNow,
        }),
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenCalledTimes(1);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'export',
        jobId: 'job-3',
        status: 'cancelled',
        jobStartedAt: expect.any(Date),
      }),
    );
  });

  it('should cancel during processing, delete output, and mark cancelled', async () => {
    const { storage, deleteMock } = createMemoryStorageAdapter();
    prisma.exportJob.findUnique
      .mockResolvedValueOnce({
        id: 'job-4',
        status: 'queued',
        processedRecords: 0,
        fileSize: null,
        startedAt: null,
        resource: 'users',
        format: 'ndjson',
        outputLocation: null,
      })
      .mockResolvedValueOnce({ status: 'cancelled' });
    prisma.user.findMany
      .mockResolvedValueOnce([
        {
          id: 11,
          email: 'cancel@example.com',
          name: 'Cancel',
          username: 'cancel',
          role: 'user',
          active: true,
          createdAt: new Date('2026-02-05T00:00:00Z'),
          updatedAt: new Date('2026-02-05T00:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([]);

    const result = await runExportJob('job-4', {
      prisma,
      storage,
      now,
      cancelCheckInterval: 1,
    });

    expect(result).toEqual({
      status: 'cancelled',
      processedRecords: 1,
      fileSize: null,
    });
    expect(deleteMock).toHaveBeenCalledWith('job-4.ndjson');
    expect(prisma.exportJob.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-4' },
        data: expect.objectContaining({
          status: 'cancelled',
          processedRecords: 1,
          totalRecords: 1,
        }),
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenCalledTimes(2);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        event: 'job.started',
        jobKind: 'export',
        jobId: 'job-4',
        status: 'running',
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'export',
        jobId: 'job-4',
        status: 'cancelled',
        jobStartedAt: expect.any(Date),
        counters: expect.objectContaining({
          processedRecords: 1,
          errorCount: 0,
        }),
      }),
    );
  });

  it('should mark failed and cleanup output when export stream fails', async () => {
    const { storage, deleteMock } = createMemoryStorageAdapter();
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-5',
      status: 'queued',
      processedRecords: 0,
      fileSize: null,
      startedAt: null,
      resource: 'users',
      format: 'ndjson',
      outputLocation: null,
    });
    prisma.user.findMany.mockRejectedValueOnce(new Error('database crashed'));

    await expect(
      runExportJob('job-5', {
        prisma,
        storage,
        now,
        cancelCheckInterval: 0,
      })
    ).rejects.toThrow('database crashed');

    expect(deleteMock).toHaveBeenCalledWith('job-5.ndjson');
    expect(prisma.exportJob.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-5' },
        data: expect.objectContaining({
          status: 'failed',
          processedRecords: 0,
          totalRecords: 0,
        }),
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenCalledTimes(2);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        event: 'job.started',
        jobKind: 'export',
        jobId: 'job-5',
        status: 'running',
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'export',
        jobId: 'job-5',
        status: 'failed',
        level: 'error',
        jobStartedAt: expect.any(Date),
        counters: expect.objectContaining({
          processedRecords: 0,
          errorCount: 1,
        }),
        details: expect.objectContaining({
          message: 'database crashed',
        }),
      }),
    );
  });

  it('should throw "job not found" error when export job is missing from DB', async () => {
    prisma.exportJob.updateMany.mockResolvedValue({ count: 0 });
    prisma.exportJob.findUnique.mockResolvedValueOnce(null);

    await expect(runExportJob('missing-job', { prisma, now })).rejects.toThrow(
      'Export job missing-job not found'
    );
  });

  it('should return latest job state when claim is not acquired', async () => {
    prisma.exportJob.updateMany.mockResolvedValue({ count: 0 });
    prisma.exportJob.findUnique.mockResolvedValueOnce({
      id: 'job-claim-missed',
      status: 'running',
      processedRecords: 12,
      fileSize: 1234,
      startedAt: new Date('2026-02-06T09:59:00.000Z'),
      resource: 'users',
      format: 'ndjson',
      outputLocation: null,
    });

    const result = await runExportJob('job-claim-missed', { prisma, now });

    expect(result).toEqual({
      status: 'running',
      processedRecords: 12,
      fileSize: 1234,
    });
    expect(prisma.user.findMany).not.toHaveBeenCalled();
  });
});
