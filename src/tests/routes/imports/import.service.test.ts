import { Readable } from 'stream';
import prismaMock from '../../prisma-mock';
import { runImportJob } from '../../../app/routes/imports/import.service';
import { ValidationErrorCode } from '../../../app/routes/shared/import-export/types';
import { logJobLifecycleEvent } from '../../../app/routes/shared/import-export/observability';

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

jest.mock('../../../app/routes/shared/import-export/observability', () => ({
  logJobLifecycleEvent: jest.fn(),
}));

import { parseJsonArrayStream } from '../../../app/routes/imports/parsing.service';
import { validateImportRecord } from '../../../app/routes/imports/validation/validation.service';
import { upsertImportRecords } from '../../../app/routes/imports/upsert.service';

const prisma: any = prismaMock;
const logJobLifecycleEventMock = logJobLifecycleEvent as jest.MockedFunction<typeof logJobLifecycleEvent>;

describe('Import Service', () => {
  beforeEach(() => {
    logJobLifecycleEventMock.mockClear();
  });

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
    expect(logJobLifecycleEventMock).toHaveBeenCalledTimes(2);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        event: 'job.started',
        jobKind: 'import',
        jobId: 'job-1',
        status: 'running',
        counters: expect.objectContaining({
          processedRecords: 0,
          successCount: 0,
          errorCount: 0,
        }),
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'import',
        jobId: 'job-1',
        status: 'partial',
        jobStartedAt: expect.any(Date),
        counters: expect.objectContaining({
          processedRecords: 2,
          successCount: 0,
          errorCount: 2,
        }),
      }),
    );
  });

  it('should log failed completion with metrics when import processing throws', async () => {
    prisma.importJob.findUnique.mockResolvedValue({
      id: 'job-failed',
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
    prisma.importError.createMany.mockResolvedValue({ count: 1 });

    (parseJsonArrayStream as jest.Mock).mockImplementation(async function* () {
      throw new Error('some error');
    });

    const result = await runImportJob('job-failed', { prisma, cancelCheckInterval: 0 });

    expect(result.status).toBe('failed');
    expect(logJobLifecycleEventMock).toHaveBeenCalledTimes(2);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        event: 'job.started',
        jobKind: 'import',
        jobId: 'job-failed',
        status: 'running',
      }),
    );
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'import',
        jobId: 'job-failed',
        status: 'failed',
        level: 'error',
        jobStartedAt: expect.any(Date),
        counters: expect.objectContaining({
          processedRecords: 0,
          successCount: 0,
          errorCount: 0,
        }),
        details: expect.objectContaining({
          message: 'some error',
        }),
      }),
    );
  });
});
