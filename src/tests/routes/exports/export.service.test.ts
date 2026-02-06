import prismaMock from '../../prisma-mock';
import { streamExportRecords, runExportJob } from '../../../app/routes/exports/export.service';
import { StorageAdapter } from '../../../app/storage';

jest.mock('../../../app/routes/exports/config', () => ({
  loadExportConfig: () => ({
    batchSize: 1,
    exportStreamMaxLimit: 2,
    workerConcurrency: 4,
    fileRetentionHours: 24,
    exportStoragePath: './exports',
    exportRateLimitPerHour: 20,
    exportConcurrentLimitUser: 5,
    exportConcurrentLimitGlobal: 20,
  }),
}));

const prisma: any = prismaMock;

function createMemoryStorage() {
  const saved: Array<{ key: string; data: string; bytes: number; location: string }> = [];
  const saveStream = jest.fn(async (key: string, stream: NodeJS.ReadableStream) => {
    const chunks: Buffer[] = [];
    await new Promise<void>((resolve, reject) => {
      stream.on('data', (chunk) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
      });
      stream.on('end', () => resolve());
      stream.on('close', () => resolve());
      stream.on('error', (error) => reject(error));
    });
    const data = Buffer.concat(chunks).toString('utf8');
    const bytes = Buffer.byteLength(data);
    const result = { key, data, bytes, location: `/tmp/${key}` };
    saved.push(result);
    return { key, bytes, location: result.location };
  });
  const deleteFile = jest.fn(async () => undefined);

  const storage: StorageAdapter = {
    saveStream,
    saveBuffer: async (key: string, data: Buffer) => ({
      key,
      bytes: data.length,
      location: `/tmp/${key}`,
    }),
    createReadStream: () => {
      throw new Error('Not implemented for this test');
    },
    getLocalPath: (key: string) => `/tmp/${key}`,
    delete: deleteFile,
  };

  return { storage, saved, saveStream, deleteFile };
}

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
});

describe('runExportJob', () => {
  const fixedNow = new Date('2026-02-06T10:00:00.000Z');
  const now = () => fixedNow;

  it('should export NDJSON, persist metadata, and set download URL on success', async () => {
    const { storage, saved } = createMemoryStorage();
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
    expect(saved).toHaveLength(1);
    expect(saved[0]?.data).toContain('"email":"first@example.com"');
    expect(saved[0]?.data).toContain('\n');
    expect(prisma.exportJob.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 'job-1' },
        data: expect.objectContaining({
          status: 'succeeded',
          processedRecords: 1,
          totalRecords: 1,
          downloadUrl: '/api/v1/exports/job-1/download',
          outputLocation: expect.stringContaining('/tmp/exports/job-1.ndjson'),
          fileSize: expect.any(Number),
          expiresAt: expect.any(Date),
        }),
      }),
    );
  });

  it('should export JSON array output when format is json', async () => {
    const { storage, saved } = createMemoryStorage();
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

    expect(saved).toHaveLength(1);
    expect(saved[0]?.data.startsWith('[')).toBe(true);
    expect(saved[0]?.data.endsWith(']')).toBe(true);
  });

  it('should return cancelled immediately when job is already cancelled', async () => {
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
  });

  it('should cancel during processing, delete output, and mark cancelled', async () => {
    const { storage, deleteFile } = createMemoryStorage();
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
    expect(deleteFile).toHaveBeenCalledWith('exports/job-4.ndjson');
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
  });

  it('should mark failed and cleanup output when export stream fails', async () => {
    const { storage, deleteFile } = createMemoryStorage();
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
    prisma.user.findMany.mockRejectedValueOnce(new Error('database exploded'));

    await expect(
      runExportJob('job-5', {
        prisma,
        storage,
        now,
        cancelCheckInterval: 0,
      })
    ).rejects.toThrow('database exploded');

    expect(deleteFile).toHaveBeenCalledWith('exports/job-5.ndjson');
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
  });

  it('should throw job not found when export job is missing', async () => {
    prisma.exportJob.findUnique.mockResolvedValueOnce(null);

    await expect(runExportJob('missing-job', { prisma, now })).rejects.toThrow(
      'Export job missing-job not found'
    );
  });
});
