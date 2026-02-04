import { Job, Worker } from 'bullmq';
import { loadConfig } from '../routes/import-export';
import { ImportExportJobPayload, importExportConnection } from './import-export.queue';

export interface ImportExportJobHandlers {
  import: (job: Job<ImportExportJobPayload>) => Promise<void>;
  export: (job: Job<ImportExportJobPayload>) => Promise<void>;
}

export function createImportExportWorker(handlers: ImportExportJobHandlers) {
  const { workerConcurrency } = loadConfig();

  return new Worker<ImportExportJobPayload>(
    'import-export',
    async (job) => {
      if (job.name === 'import') {
        await handlers.import(job);
        return;
      }
      if (job.name === 'export') {
        await handlers.export(job);
        return;
      }

      throw new Error(`Unsupported job type: ${job.name}`);
    },
    {
      connection: importExportConnection,
      concurrency: workerConcurrency,
    },
  );
}
