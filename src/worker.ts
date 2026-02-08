import { Job } from 'bullmq';
import { createImportExportWorker } from './app/jobs/import-export.worker';
import { ImportExportJobPayload, importExportConnection, importExportQueue } from './app/jobs/import-export.queue';
import { runImportJob } from './app/routes/imports/import.service';
import { runExportJob } from './app/routes/exports/export.service';
import prismaClient from './prisma/prisma-client';
import { createLogger } from './app/logger';

const importExportLogger = createLogger({ component: 'import-export.worker' });
const importExportWorker = createImportExportWorker({
  import: async (job: Job<ImportExportJobPayload>) => {
    await runImportJob(job.data.jobId);
  },
  export: async (job: Job<ImportExportJobPayload>) => {
    await runExportJob(job.data.jobId);
  },
});

importExportWorker.on('ready', () => {
  importExportLogger.info({ event: 'worker.ready' });
});

importExportWorker.on('completed', (job) => {
  importExportLogger.info({
    event: 'job.completed',
    queueJobName: job.name,
    queueJobId: job.id,
  });
});

importExportWorker.on('failed', (job, error) => {
  importExportLogger.error({
    event: 'job.failed',
    queueJobName: job?.name ?? null,
    queueJobId: job?.id ?? null,
    errorName: error?.name,
    errorMessage: error?.message,
    errorStack: error?.stack,
  });
});

let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  importExportLogger.info({ event: 'worker.shutdown.requested', signal });

  try {
    await importExportWorker.close();
    await importExportQueue.close();
    await importExportConnection.quit();
    await prismaClient.$disconnect();
    importExportLogger.info({ event: 'worker.shutdown.completed' });
    process.exit(0);
  } catch (error) {
    importExportLogger.error({
      event: 'worker.shutdown.failed',
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
      errorStack: error instanceof Error ? error.stack : undefined,
    });
    process.exit(1);
  }
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});
