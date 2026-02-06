import { Job } from 'bullmq';
import { createImportExportWorker } from './app/jobs/import-export.worker';
import { ImportExportJobPayload, importExportConnection, importExportQueue } from './app/jobs/import-export.queue';
import { runImportJob } from './app/routes/imports/import.service';
import { runExportJob } from './app/routes/exports/export.service';
import prismaClient from './prisma/prisma-client';

const worker = createImportExportWorker({
  import: async (job: Job<ImportExportJobPayload>) => {
    await runImportJob(job.data.jobId);
  },
  export: async (job: Job<ImportExportJobPayload>) => {
    await runExportJob(job.data.jobId);
  },
});

worker.on('ready', () => {
  console.info('import-export worker ready');
});

worker.on('completed', (job) => {
  console.info(`import-export job completed: ${job.name} (${job.id})`);
});

worker.on('failed', (job, error) => {
  const jobDescriptor = job ? `${job.name} (${job.id})` : 'unknown';
  console.error(`import-export job failed: ${jobDescriptor}`, error);
});

let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  console.info(`worker received ${signal}, shutting down`);

  try {
    await worker.close();
    await importExportQueue.close();
    await importExportConnection.quit();
    await prismaClient.$disconnect();
    console.info('worker shutdown complete');
    process.exit(0);
  } catch (error) {
    console.error('worker shutdown failed', error);
    process.exit(1);
  }
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});
