import { SharedImportExportConfig } from './types';

export const DEFAULT_SHARED_IMPORT_EXPORT_CONFIG: SharedImportExportConfig = {
  workerConcurrency: 4,
};

export function loadSharedImportExportConfig(): SharedImportExportConfig {
  return {
    workerConcurrency:
      parseInt(process.env.JOB_WORKER_CONCURRENCY || '', 10) ||
      DEFAULT_SHARED_IMPORT_EXPORT_CONFIG.workerConcurrency,
  };
}
