import IORedis from 'ioredis';
import { JobsOptions, Queue } from 'bullmq';

export type ImportExportJobType = 'import' | 'export';

export interface ImportExportJobPayload {
  jobId: string;
  type: ImportExportJobType;
  resource: string;
  format: 'ndjson' | 'json';
}

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

export const importExportConnection = new IORedis(redisUrl, {
  maxRetriesPerRequest: null,
});

export const importExportQueue = new Queue('import-export', {
  connection: importExportConnection,
});

const defaultJobOptions: JobsOptions = {
  attempts: 3,
  backoff: { type: 'fixed', delay: 60_000 },
  removeOnComplete: true,
};

export function enqueueImportJob(payload: Omit<ImportExportJobPayload, 'type'>, options?: JobsOptions) {
  return importExportQueue.add('import', { ...payload, type: 'import' }, {
    ...defaultJobOptions,
    ...options,
  });
}

export function enqueueExportJob(payload: Omit<ImportExportJobPayload, 'type'>, options?: JobsOptions) {
  return importExportQueue.add('export', { ...payload, type: 'export' }, {
    ...defaultJobOptions,
    ...options,
  });
}
