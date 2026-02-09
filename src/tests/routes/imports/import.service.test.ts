import { Readable } from 'stream';
import prismaMock from '../../prisma-mock';
import { runImportJob } from '../../../app/routes/imports/import.service';
import { ValidationErrorCode } from '../../../app/routes/shared/import-export/types';
import { logJobLifecycleEvent } from '../../../app/jobs/observability';

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

jest.mock('../../../app/jobs/observability', () => ({
  logJobLifecycleEvent: jest.fn(),
}));

import { parseJsonArrayStream, parseNdjsonStream } from '../../../app/routes/imports/parsing.service';
import { validateImportRecord } from '../../../app/routes/imports/validation/validation.service';
import { upsertImportRecords } from '../../../app/routes/imports/upsert.service';
import { ProcessingErrorCode } from '../../../app/routes/shared/import-export/types';

const prisma: any = prismaMock;
const logJobLifecycleEventMock = logJobLifecycleEvent as jest.MockedFunction<typeof logJobLifecycleEvent>;

describe('Import Service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    logJobLifecycleEventMock.mockClear();
  });

  it('should return latest job state when claim is not acquired', async () => {
    prisma.importJob.updateMany.mockResolvedValue({ count: 0 });
    prisma.importJob.findUnique.mockResolvedValue({
      id: 'job-claim-missed',
      status: 'running',
      resource: 'users',
      format: 'json',
      fileName: 'input.json',
      sourceLocation: '/tmp/input.json',
      processedRecords: 42,
      successCount: 40,
      errorCount: 2,
      startedAt: new Date('2026-02-08T00:00:00.000Z'),
    });

    const result = await runImportJob('job-claim-missed', { prisma, cancelCheckInterval: 0 });

    expect(result).toEqual({
      status: 'running',
      processedRecords: 42,
      successCount: 40,
      errorCount: 2,
    });
    expect(parseJsonArrayStream).not.toHaveBeenCalled();
    expect(validateImportRecord).not.toHaveBeenCalled();
    expect(upsertImportRecords).not.toHaveBeenCalled();
  });

  it('should mark failed when all processed records fail', async () => {
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
    prisma.importJob.updateMany.mockResolvedValue({ count: 1 });
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
        status: 'failed',
        jobStartedAt: expect.any(Date),
        counters: expect.objectContaining({
          processedRecords: 2,
          successCount: 0,
          errorCount: 2,
        }),
      }),
    );
  });

  it('should mark partial when some records succeed and some fail', async () => {
    prisma.importJob.findUnique.mockResolvedValue({
      id: 'job-partial',
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
    prisma.importJob.updateMany.mockResolvedValue({ count: 1 });
    prisma.importError.createMany.mockResolvedValue({ count: 1 });

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
            jobId: 'job-partial',
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
      succeeded: 1,
      failed: 0,
      errors: [],
    });

    const result = await runImportJob('job-partial', { prisma, cancelCheckInterval: 0 });

    expect(result.status).toBe('partial');
    expect(result.successCount).toBe(1);
    expect(result.errorCount).toBe(1);
    expect(logJobLifecycleEventMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        event: 'job.completed',
        jobKind: 'import',
        jobId: 'job-partial',
        status: 'partial',
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
    prisma.importJob.updateMany.mockResolvedValue({ count: 1 });
    prisma.importJob.update.mockResolvedValue({});
    prisma.importError.createMany.mockResolvedValue({ count: 1 });

    (parseJsonArrayStream as jest.Mock).mockImplementation(async function* () {
      throw new Error('some error');
    });

    const result = await runImportJob('job-failed', { prisma, cancelCheckInterval: 0 });

    expect(result.status).toBe('failed');
    expect(prisma.importJob.update).toHaveBeenLastCalledWith(
      expect.objectContaining({
        where: { id: 'job-failed' },
        data: expect.objectContaining({
          status: 'failed',
          errorSummary: expect.objectContaining({
            lastError: expect.objectContaining({
              message: 'some error',
            }),
          }),
        }),
      }),
    );
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

  it('should fail when ndjson parser yields non-object record structure', async () => {
    prisma.importJob.findUnique.mockResolvedValue({
      id: 'job-ndjson-structure',
      status: 'queued',
      resource: 'articles',
      format: 'ndjson',
      fileName: 'input.ndjson',
      sourceLocation: '/tmp/input.ndjson',
      processedRecords: 0,
      successCount: 0,
      errorCount: 0,
      startedAt: null,
    });
    prisma.importJob.updateMany.mockResolvedValue({ count: 1 });
    prisma.importJob.update.mockResolvedValue({});
    prisma.importError.createMany.mockResolvedValue({ count: 1 });

    (parseNdjsonStream as jest.Mock).mockImplementation(async function* () {
      yield { record: [{ id: 1 }], index: 0, lineNumber: 1 };
    });

    const result = await runImportJob('job-ndjson-structure', { prisma, cancelCheckInterval: 0 });

    expect(result).toEqual({
      status: 'failed',
      processedRecords: 0,
      successCount: 0,
      errorCount: 0,
    });
    expect(validateImportRecord).not.toHaveBeenCalled();
    expect(upsertImportRecords).not.toHaveBeenCalled();
    const createManyCall = prisma.importError.createMany.mock.calls[0]?.[0];
    expect(createManyCall?.data?.[0]).toEqual(
      expect.objectContaining({
        recordIndex: -1,
        errorCode: ProcessingErrorCode.INVALID_RECORD_STRUCTURE,
        errorName: 'INVALID_RECORD_STRUCTURE',
      }),
    );
  });

  it('should fallback to single insert when fatal error createMany fails', async () => {
    prisma.importJob.findUnique.mockResolvedValue({
      id: 'job-fatal-fallback',
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
    prisma.importJob.updateMany.mockResolvedValue({ count: 1 });
    prisma.importJob.update.mockResolvedValue({});
    prisma.importError.createMany.mockRejectedValueOnce(new Error('createMany failed'));
    prisma.importError.create.mockResolvedValueOnce({
      id: 'err-fallback',
    });

    (parseJsonArrayStream as jest.Mock).mockImplementation(async function* () {
      throw new Error('invalid json');
    });

    const result = await runImportJob('job-fatal-fallback', { prisma, cancelCheckInterval: 0 });

    expect(result.status).toBe('failed');
    expect(prisma.importError.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          jobId: 'job-fatal-fallback',
          recordIndex: -1,
        }),
      }),
    );
  });
});
