import { round } from './utils';

export type JobKind = 'import' | 'export';
export type LogLevel = 'info' | 'warn' | 'error';

interface StructuredLogger {
  info: (message: string) => void;
  warn: (message: string) => void;
  error: (message: string) => void;
}

export interface JobMetrics {
  durationMs: number;
  rowsPerSecond: number;
  errorRate: number;
}

export interface JobLogEntry {
  event: 'job.started' | 'job.completed';
  jobKind: JobKind;
  jobId: string;
  status: string;
  resource: string;
  format: string | null;
  timestamp: Date;
  jobStartedAt?: Date;
  counters: {
    processedRecords: number;
    successCount?: number;
    errorCount: number;
  };
  level?: LogLevel;
  details?: Record<string, unknown>;
}

const FALLBACK_DURATION_MS = 1;

function computeJobMetrics(input: {
  startedAt: Date;
  finishedAt: Date;
  processedRecords: number;
  errorCount: number;
}): JobMetrics {
  const rawDuration = input.finishedAt.getTime() - input.startedAt.getTime();
  const durationMs = Math.max(rawDuration, FALLBACK_DURATION_MS);
  const rowsPerSecond = (input.processedRecords * 1000) / durationMs;
  const denominator = input.processedRecords > 0 ? input.processedRecords : 1;
  const errorRate = input.errorCount / denominator;

  return {
    durationMs,
    rowsPerSecond: round(rowsPerSecond, 3),
    errorRate: round(errorRate, 6),
  };
}

export function logJobLifecycleEvent(
  entry: JobLogEntry,
  logger: StructuredLogger = console,
): void {
  const { jobStartedAt, ...rest } = entry;
  const metrics = jobStartedAt
    ? computeJobMetrics({
        startedAt: jobStartedAt,
        finishedAt: entry.timestamp,
        processedRecords: entry.counters.processedRecords,
        errorCount: entry.counters.errorCount,
      })
    : undefined;

  const payload = {
    ...rest,
    timestamp: entry.timestamp.toISOString(),
    ...(metrics ? { metrics } : {}),
  };

  const line = JSON.stringify(payload);
  if (entry.level === 'error') {
    logger.error(line);
    return;
  }

  if (entry.level === 'warn') {
    logger.warn(line);
    return;
  }

  logger.info(line);
}
