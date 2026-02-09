import { SharedImportExportConfig } from './types';

export const DEFAULT_SHARED_IMPORT_EXPORT_CONFIG: SharedImportExportConfig = {
  workerConcurrency: 4,
};

export function loadSharedImportExportConfig(): SharedImportExportConfig {
  const parsedConcurrency = Number.parseInt(
    process.env.JOB_WORKER_CONCURRENCY || '',
    10
  );
  const workerConcurrency =
    parsedConcurrency > 0
      ? parsedConcurrency
      : DEFAULT_SHARED_IMPORT_EXPORT_CONFIG.workerConcurrency;

  return {
    workerConcurrency,
  };
}
