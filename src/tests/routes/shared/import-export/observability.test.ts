import { logJobLifecycleEvent } from '../../../../app/routes/shared/import-export/observability';

function createLogger() {
  return {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  };
}

function parsePayload(mockCallArg: unknown): Record<string, unknown> {
  if (typeof mockCallArg !== 'string') {
    throw new Error('Expected logger call argument to be string');
  }
  return JSON.parse(mockCallArg) as Record<string, unknown>;
}

describe('observability', () => {
  it('should emit started event without metrics when jobStartedAt is absent', () => {
    const logger = createLogger();
    const timestamp = new Date('2026-02-08T15:00:00.000Z');

    logJobLifecycleEvent(
      {
        event: 'job.started',
        jobKind: 'import',
        jobId: 'job-1',
        status: 'running',
        resource: 'articles',
        format: 'json',
        timestamp,
        counters: {
          processedRecords: 0,
          successCount: 0,
          errorCount: 0,
        },
      },
      logger,
    );

    expect(logger.info).toHaveBeenCalledTimes(1);
    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();

    const payload = parsePayload(logger.info.mock.calls[0][0]);
    expect(payload).toMatchObject({
      event: 'job.started',
      jobKind: 'import',
      jobId: 'job-1',
      status: 'running',
      resource: 'articles',
      format: 'json',
      timestamp: '2026-02-08T15:00:00.000Z',
      counters: {
        processedRecords: 0,
        successCount: 0,
        errorCount: 0,
      },
    });
    expect(payload).not.toHaveProperty('metrics');
  });

  it('should emit completed event with computed metrics when jobStartedAt is present', () => {
    const logger = createLogger();
    const startedAt = new Date('2026-02-08T15:00:00.000Z');
    const finishedAt = new Date('2026-02-08T15:00:10.000Z');

    logJobLifecycleEvent(
      {
        event: 'job.completed',
        jobKind: 'export',
        jobId: 'job-2',
        status: 'succeeded',
        resource: 'articles',
        format: 'ndjson',
        timestamp: finishedAt,
        jobStartedAt: startedAt,
        counters: {
          processedRecords: 5000,
          errorCount: 50,
        },
      },
      logger,
    );

    expect(logger.info).toHaveBeenCalledTimes(1);
    const payload = parsePayload(logger.info.mock.calls[0][0]);

    expect(payload).toMatchObject({
      event: 'job.completed',
      jobKind: 'export',
      jobId: 'job-2',
      status: 'succeeded',
      resource: 'articles',
      format: 'ndjson',
      timestamp: '2026-02-08T15:00:10.000Z',
      counters: {
        processedRecords: 5000,
        errorCount: 50,
      },
      metrics: {
        durationMs: 10000,
        rowsPerSecond: 500,
        errorRate: 0.01,
      },
    });
  });

  it('should route warn and error events to the matching logger methods', () => {
    const warnLogger = createLogger();
    const errorLogger = createLogger();
    const timestamp = new Date('2026-02-08T15:00:00.000Z');

    logJobLifecycleEvent(
      {
        event: 'job.completed',
        jobKind: 'import',
        jobId: 'job-warn',
        status: 'partial',
        resource: 'users',
        format: 'json',
        timestamp,
        jobStartedAt: timestamp,
        counters: {
          processedRecords: 10,
          successCount: 8,
          errorCount: 2,
        },
        level: 'warn',
      },
      warnLogger,
    );

    logJobLifecycleEvent(
      {
        event: 'job.completed',
        jobKind: 'import',
        jobId: 'job-error',
        status: 'failed',
        resource: 'users',
        format: 'json',
        timestamp,
        jobStartedAt: timestamp,
        counters: {
          processedRecords: 10,
          successCount: 8,
          errorCount: 2,
        },
        level: 'error',
      },
      errorLogger,
    );

    expect(warnLogger.warn).toHaveBeenCalledTimes(1);
    expect(warnLogger.info).not.toHaveBeenCalled();
    expect(warnLogger.error).not.toHaveBeenCalled();

    expect(errorLogger.error).toHaveBeenCalledTimes(1);
    expect(errorLogger.info).not.toHaveBeenCalled();
    expect(errorLogger.warn).not.toHaveBeenCalled();
  });
});
