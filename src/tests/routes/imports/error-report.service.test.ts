import prismaMock from '../../prisma-mock';
import { generateImportErrorReport } from '../../../app/routes/imports/error-report.service';
import { createMemoryStorageAdapter } from '../../helpers/memory-storage';
import { ValidationErrorCode } from '../../../app/routes/shared/import-export/types';

const prisma = prismaMock as unknown as any;

describe('Error Report Service', () => {
  it('should write NDJSON with one line per error', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.importError.findMany
      .mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 0,
          recordId: 'user@example.com',
          errorCode: ValidationErrorCode.INVALID_FIELD_FORMAT,
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
          errorCode: ValidationErrorCode.DUPLICATE_VALUE,
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
    expect(savedFiles).toHaveLength(1);
    const output = savedFiles[0]?.data ?? '';
    const lines = output.trim().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0] ?? '{}').recordIndex).toBe(0);
    expect(JSON.parse(lines[1] ?? '{}').recordIndex).toBe(1);
  });

  it('should write JSON array output when format is json', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.importError.findMany
      .mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 5,
          recordId: 'abc',
          errorCode: ValidationErrorCode.MISSING_REQUIRED_FIELD,
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
    expect(savedFiles).toHaveLength(1);
    const parsed = JSON.parse(savedFiles[0]?.data ?? '[]');
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed[0]?.recordIndex).toBe(5);
  });

  it('should paginate results and preserve order across pages', async () => {
    const { storage, savedFiles } = createMemoryStorageAdapter();
    prisma.importError.findMany
      .mockResolvedValueOnce([
        {
          id: 'err-1',
          recordIndex: 0,
          recordId: 'a',
          errorCode: ValidationErrorCode.INVALID_FIELD_FORMAT,
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
          errorCode: ValidationErrorCode.INVALID_FIELD_FORMAT,
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
    const output = savedFiles[0]?.data ?? '';
    const lines = output.trim().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0] ?? '{}').recordIndex).toBe(0);
    expect(JSON.parse(lines[1] ?? '{}').recordIndex).toBe(1);
  });
});
