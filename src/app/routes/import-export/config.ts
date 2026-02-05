import { ImportExportConfig } from './types';

/**
 * Default configuration values
 */
export const DEFAULT_CONFIG: ImportExportConfig = {
  maxFileSize: 1024 * 1024 * 1024, // 1GB
  maxRecords: 1000000,
  batchSize: 1000,
  allowedHosts: [],

  fileRetentionHours: 24,
  exportStoragePath: './exports',
  errorReportStoragePath: './import-errors',
  importStoragePath: './imports',

  exportStreamMaxLimit: 1000,

  importRateLimitPerHour: 10,
  exportRateLimitPerHour: 20,

  importConcurrentLimitUser: 2,
  importConcurrentLimitGlobal: 10,
  exportConcurrentLimitUser: 5,
  exportConcurrentLimitGlobal: 20,

  workerConcurrency: 4,
};

/**
 * Load configuration from environment variables
 */
export function loadConfig(): ImportExportConfig {
  return {
    maxFileSize: parseInt(process.env.IMPORT_MAX_FILE_SIZE || '', 10) || DEFAULT_CONFIG.maxFileSize,
    maxRecords: parseInt(process.env.IMPORT_MAX_RECORDS || '', 10) || DEFAULT_CONFIG.maxRecords,
    batchSize: parseInt(process.env.IMPORT_BATCH_SIZE || '', 10) || DEFAULT_CONFIG.batchSize,
    allowedHosts: process.env.IMPORT_ALLOWED_HOSTS?.split(',').filter(Boolean) || DEFAULT_CONFIG.allowedHosts,

    fileRetentionHours:
      parseInt(process.env.EXPORT_FILE_RETENTION_HOURS || '', 10) || DEFAULT_CONFIG.fileRetentionHours,
    exportStoragePath: process.env.EXPORT_STORAGE_PATH || DEFAULT_CONFIG.exportStoragePath,
    errorReportStoragePath: process.env.ERROR_REPORT_STORAGE_PATH || DEFAULT_CONFIG.errorReportStoragePath,
    importStoragePath: process.env.IMPORT_STORAGE_PATH || DEFAULT_CONFIG.importStoragePath,

    exportStreamMaxLimit:
      parseInt(process.env.EXPORT_STREAM_MAX_LIMIT || '', 10) || DEFAULT_CONFIG.exportStreamMaxLimit,

    importRateLimitPerHour:
      parseInt(process.env.IMPORT_RATE_LIMIT_PER_HOUR || '', 10) || DEFAULT_CONFIG.importRateLimitPerHour,
    exportRateLimitPerHour:
      parseInt(process.env.EXPORT_RATE_LIMIT_PER_HOUR || '', 10) || DEFAULT_CONFIG.exportRateLimitPerHour,

    importConcurrentLimitUser:
      parseInt(process.env.IMPORT_CONCURRENT_LIMIT_USER || '', 10) || DEFAULT_CONFIG.importConcurrentLimitUser,
    importConcurrentLimitGlobal:
      parseInt(process.env.IMPORT_CONCURRENT_LIMIT_GLOBAL || '', 10) || DEFAULT_CONFIG.importConcurrentLimitGlobal,
    exportConcurrentLimitUser:
      parseInt(process.env.EXPORT_CONCURRENT_LIMIT_USER || '', 10) || DEFAULT_CONFIG.exportConcurrentLimitUser,
    exportConcurrentLimitGlobal:
      parseInt(process.env.EXPORT_CONCURRENT_LIMIT_GLOBAL || '', 10) || DEFAULT_CONFIG.exportConcurrentLimitGlobal,

    workerConcurrency: parseInt(process.env.JOB_WORKER_CONCURRENCY || '', 10) || DEFAULT_CONFIG.workerConcurrency,
  };
}
