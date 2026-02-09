import { Job } from 'bullmq';
import { createImportExportWorker } from './app/jobs/import-export.worker';
import { ImportExportJobPayload, importExportConnection, importExportQueue } from './app/jobs/import-export.queue';
import { runImportJob } from './app/routes/imports/import.service';
import { runExportJob } from './app/routes/exports/export.service';
import prismaClient from './prisma/prisma-client';
import { createLogger } from './app/logger';

const logger = createLogger({ component: 'import-export.worker' });
const worker = createImportExportWorker({
  import: async (job: Job<ImportExportJobPayload>) => {
    await runImportJob(job.data.jobId);
  },
  export: async (job: Job<ImportExportJobPayload>) => {
    await runExportJob(job.data.jobId);
  },
});

worker.on('ready', () => {
  logger.info({ event: 'worker.ready' });
});

worker.on('completed', (job) => {
  logger.info({
    event: 'job.completed',
    queueJobName: job.name,
    queueJobId: job.id,
  });
});

worker.on('failed', (job, error) => {
  logger.error({
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

  logger.info({ event: 'worker.shutdown.requested', signal });

  try {
    await worker.close();
    await importExportQueue.close();
    await importExportConnection.quit();
    await prismaClient.$disconnect();
    logger.info({ event: 'worker.shutdown.completed' });
    process.exit(0);
  } catch (error) {
    logger.error({
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
