import { Readable } from 'stream';
import prismaMock from '../../prisma-mock';
import { runImportJob } from '../../../app/routes/imports/import.service';
import { ValidationErrorCode } from '../../../app/routes/shared/import-export/types';

jest.mock('../../../app/routes/imports/config', () => ({
  loadImportConfig: () => ({
    maxRecords: 100,
    batchSize: 1,
    workerConcurrency: 4,
    maxFileSize: 1024 * 1024 * 1024,
    allowedHosts: [],
    errorReportStoragePath: './import-errors',
    importStoragePath: './imports',
    importRateLimitPerHour: 10,
    importConcurrentLimitUser: 2,
    importConcurrentLimitGlobal: 10,
  }),
}));

jest.mock('../../../app/storage', () => ({
  createImportStorageAdapter: () => ({
    getLocalPath: (input: string) => input,
  }),
}));

jest.mock('fs', () => ({
  createReadStream: jest.fn(() => new Readable({ read() { this.push(null); } })),
  promises: {
    stat: jest.fn().mockResolvedValue({}),
  },
}));

jest.mock('../../../app/routes/imports/parsing.service', () => {
  const actual = jest.requireActual('../../../app/routes/imports/parsing.service');
  return {
    ...actual,
    parseJsonArrayStream: jest.fn(),
    parseNdjsonStream: jest.fn(),
  };
});

jest.mock('../../../app/routes/imports/validation/validation.service', () => ({
  validateImportRecord: jest.fn(),
}));

jest.mock('../../../app/routes/imports/upsert.service', () => ({
  upsertImportRecords: jest.fn(),
}));

jest.mock('../../../app/routes/imports/error-report.service', () => ({
  generateImportErrorReport: jest.fn().mockResolvedValue({
    key: 'import-errors/job-1.ndjson',
    location: '/tmp/import-errors/job-1.ndjson',
    bytes: 0,
    format: 'ndjson',
    errorCount: 2,
  }),
}));

import { parseJsonArrayStream } from '../../../app/routes/imports/parsing.service';
import { validateImportRecord } from '../../../app/routes/imports/validation/validation.service';
import { upsertImportRecords } from '../../../app/routes/imports/upsert.service';

const prisma = prismaMock as unknown as any;

describe('Import Service', () => {
  it('should count distinct records with errors', async () => {
    prisma.importJob.findUnique.mockResolvedValue({
      id: 'job-1',
      status: 'queued',
      resource: 'users',
      format: 'json',
      fileName: 'input.json',
      sourceLocation: '/tmp/input.json',
      processedRecords: 0,
      successCount: 0,
      errorCount: 0,
      startedAt: null,
    });
    prisma.importJob.update.mockResolvedValue({});
    prisma.importError.createMany.mockResolvedValue({ count: 2 });

    (parseJsonArrayStream as jest.Mock).mockImplementation(async function* () {
      yield { record: { email: 'bad' }, index: 0 };
      yield { record: { id: 2, email: 'ok@example.com', name: 'Ok', role: 'user', active: true }, index: 1 };
    });

    (validateImportRecord as jest.Mock)
      .mockResolvedValueOnce({
        valid: false,
        skip: false,
        errors: [
          {
            jobId: 'job-1',
            recordIndex: 0,
            errorCode: ValidationErrorCode.INVALID_FIELD_FORMAT,
            message: 'Invalid email',
            field: 'email',
            value: 'bad',
          },
        ],
      })
      .mockResolvedValueOnce({
        valid: true,
        skip: false,
        errors: [],
        record: { id: 2, email: 'ok@example.com', name: 'Ok', role: 'user', active: true },
      });

    (upsertImportRecords as jest.Mock).mockResolvedValue({
      attempted: 1,
      succeeded: 0,
      failed: 1,
      errors: [
        {
          jobId: 'job-1',
          recordIndex: 1,
          errorCode: ValidationErrorCode.DUPLICATE_VALUE,
          message: 'Duplicate email',
          field: 'email',
          value: 'ok@example.com',
        },
      ],
    });

    const result = await runImportJob('job-1', { prisma, cancelCheckInterval: 0 });

    expect(result.errorCount).toBe(2);
    expect(prisma.importError.createMany).toHaveBeenCalledTimes(1);
    expect(prisma.importError.createMany.mock.calls[0][0].data).toHaveLength(2);
  });
});
