import {
  SharedImportExportConfig,
} from '../shared/import-export/types';
import {
  DEFAULT_SHARED_IMPORT_EXPORT_CONFIG,
  loadSharedImportExportConfig,
} from '../shared/import-export/config';

interface ImportConfig extends SharedImportExportConfig {
  batchSize: number;
  maxFileSize: number; // bytes
  maxRecords: number;
  allowedHosts: string[];
  errorReportStoragePath: string;
  importStoragePath: string;
  importRateLimitPerHour: number;
  importConcurrentLimitUser: number;
  importConcurrentLimitGlobal: number;
}

export const DEFAULT_IMPORT_CONFIG: Omit<ImportConfig, 'workerConcurrency'> = {
  batchSize: 1000,
  maxFileSize: 1024 * 1024 * 1024, // 1GB
  maxRecords: 1000000,
  allowedHosts: [],
  errorReportStoragePath: './import-errors',
  importStoragePath: './imports',
  importRateLimitPerHour: 10,
  importConcurrentLimitUser: 2,
  importConcurrentLimitGlobal: 10,
};

export function loadImportConfig(): ImportConfig {
  const shared = loadSharedImportExportConfig();
  return {
    batchSize:
      parseInt(process.env.IMPORT_BATCH_SIZE || '', 10) || DEFAULT_IMPORT_CONFIG.batchSize,
    workerConcurrency:
      shared.workerConcurrency || DEFAULT_SHARED_IMPORT_EXPORT_CONFIG.workerConcurrency,
    maxFileSize:
      parseInt(process.env.IMPORT_MAX_FILE_SIZE || '', 10) || DEFAULT_IMPORT_CONFIG.maxFileSize,
    maxRecords:
      parseInt(process.env.IMPORT_MAX_RECORDS || '', 10) || DEFAULT_IMPORT_CONFIG.maxRecords,
    allowedHosts:
      process.env.IMPORT_ALLOWED_HOSTS?.split(',').filter(Boolean) || DEFAULT_IMPORT_CONFIG.allowedHosts,
    errorReportStoragePath:
      process.env.ERROR_REPORT_STORAGE_PATH || DEFAULT_IMPORT_CONFIG.errorReportStoragePath,
    importStoragePath: process.env.IMPORT_STORAGE_PATH || DEFAULT_IMPORT_CONFIG.importStoragePath,
    importRateLimitPerHour:
      parseInt(process.env.IMPORT_RATE_LIMIT_PER_HOUR || '', 10) ||
      DEFAULT_IMPORT_CONFIG.importRateLimitPerHour,
    importConcurrentLimitUser:
      parseInt(process.env.IMPORT_CONCURRENT_LIMIT_USER || '', 10) ||
      DEFAULT_IMPORT_CONFIG.importConcurrentLimitUser,
    importConcurrentLimitGlobal:
      parseInt(process.env.IMPORT_CONCURRENT_LIMIT_GLOBAL || '', 10) ||
      DEFAULT_IMPORT_CONFIG.importConcurrentLimitGlobal,
  };
}
