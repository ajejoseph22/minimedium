import prismaMock from '../../prisma-mock';
import { generateImportErrorReport } from '../../../app/routes/imports/error-report.service';
import { StorageAdapter } from '../../../app/storage';

const prisma = prismaMock as unknown as any;

type StoredStream = { key: string; data: string };

const createMemoryStorage = () => {
  const stored: StoredStream[] = [];

  const storage: StorageAdapter = {
    async saveStream(key, stream) {
      const chunks: Buffer[] = [];
      await new Promise<void>((resolve, reject) => {
        stream.on('data', (chunk) => {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
        });
        stream.on('end', () => resolve());
        stream.on('error', (error) => reject(error));
      });
      const data = Buffer.concat(chunks).toString('utf8');
      stored.push({ key, data });
      return { key, bytes: Buffer.byteLength(data), location: `/tmp/${key}` };
    },
    async saveBuffer(key, data) {
      stored.push({ key, data: data.toString('utf8') });
      return { key, bytes: data.length, location: `/tmp/${key}` };
    },
    createReadStream() {
      throw new Error('Not implemented for tests');
    },
    getLocalPath(key: string) {
      return `/tmp/${key}`;
    },
    async delete() {
      return;
    },
  };

  return { storage, stored };
};

describe('Error Report Service', () => {
  it('should write NDJSON with one line per error', async () => {
    const { storage, stored } = createMemoryStorage();
    prisma.importError.findMany
      .mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 0,
          recordId: 'user@example.com',
          errorCode: 1003,
          errorName: 'INVALID_FIELD_FORMAT',
          message: 'Invalid email',
          field: 'email',
          value: 'bad',
          details: null,
          createdAt: new Date('2026-02-05T10:00:00Z'),
        },
        {
          id: 'err-2',
          recordIndex: 1,
          recordId: '2',
          errorCode: 1007,
          errorName: 'DUPLICATE_VALUE',
          message: 'Duplicate email',
          field: 'email',
          value: 'user@example.com',
          details: null,
          createdAt: new Date('2026-02-05T10:01:00Z'),
        },
      ])
      .mockResolvedValueOnce([]);

    const result = await generateImportErrorReport('job-1', {
      prisma,
      storage,
      format: 'ndjson',
      key: 'imports/job-1.ndjson',
      pageSize: 1000,
    });

    expect(result.key).toBe('imports/job-1.ndjson');
    expect(result.format).toBe('ndjson');
    expect(result.errorCount).toBe(2);
    expect(stored).toHaveLength(1);
    const output = stored[0]?.data ?? '';
    const lines = output.trim().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0] ?? '{}').recordIndex).toBe(0);
    expect(JSON.parse(lines[1] ?? '{}').recordIndex).toBe(1);
  });

  it('should write JSON array output when format is json', async () => {
    const { storage, stored } = createMemoryStorage();
    prisma.importError.findMany
      .mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 5,
          recordId: 'abc',
          errorCode: 1001,
          errorName: 'MISSING_REQUIRED_FIELD',
          message: 'Missing id',
          field: 'id',
          value: null,
          details: null,
          createdAt: new Date('2026-02-05T10:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([]);

    const result = await generateImportErrorReport('job-2', {
      prisma,
      storage,
      format: 'json',
      key: 'imports/job-2.json',
    });

    expect(result.format).toBe('json');
    expect(result.errorCount).toBe(1);
    expect(stored).toHaveLength(1);
    const parsed = JSON.parse(stored[0]?.data ?? '[]');
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed[0]?.recordIndex).toBe(5);
  });

  it('should paginate results and preserve order across pages', async () => {
    const { storage, stored } = createMemoryStorage();
    prisma.importError.findMany
      .mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 0,
          recordId: 'a',
          errorCode: 1003,
          errorName: 'INVALID_FIELD_FORMAT',
          message: 'Bad',
          field: 'email',
          value: 'a',
          details: null,
          createdAt: new Date('2026-02-05T10:00:00Z'),
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 'err-2',
          recordIndex: 1,
          recordId: 'b',
          errorCode: 1003,
          errorName: 'INVALID_FIELD_FORMAT',
          message: 'Bad',
          field: 'email',
          value: 'b',
          details: null,
          createdAt: new Date('2026-02-05T10:01:00Z'),
        },
      ])
      .mockResolvedValueOnce([]); // ← Empty array = no more pages

    const result = await generateImportErrorReport('job-3', {
      prisma,
      storage,
      format: 'ndjson',
      pageSize: 1,
    });

    expect(result.errorCount).toBe(2);
    const output = stored[0]?.data ?? '';
    const lines = output.trim().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0] ?? '{}').recordIndex).toBe(0);
    expect(JSON.parse(lines[1] ?? '{}').recordIndex).toBe(1);
  });
});
