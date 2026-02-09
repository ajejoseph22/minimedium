import {
  SharedImportExportConfig,
} from '../shared/import-export/types';
import {
  DEFAULT_SHARED_IMPORT_EXPORT_CONFIG,
  loadSharedImportExportConfig,
} from '../shared/import-export/config';

interface ExportConfig extends SharedImportExportConfig {
  batchSize: number;
  exportMaxRecords: number;
  fileRetentionHours: number;
  exportStoragePath: string;
  exportStreamMaxLimit: number;
  exportRateLimitPerHour: number;
  exportConcurrentLimitUser: number;
  exportConcurrentLimitGlobal: number;
}

export const DEFAULT_EXPORT_CONFIG: Omit<ExportConfig, 'workerConcurrency'> = {
  batchSize: 1000,
  exportMaxRecords: 1000000,
  fileRetentionHours: 24,
  exportStoragePath: './exports',
  exportStreamMaxLimit: 1000,
  exportRateLimitPerHour: 20,
  exportConcurrentLimitUser: 5,
  exportConcurrentLimitGlobal: 20,
};

export function loadExportConfig(): ExportConfig {
  const shared = loadSharedImportExportConfig();
  return {
    batchSize:
      parseInt(process.env.EXPORT_BATCH_SIZE || '', 10) || DEFAULT_EXPORT_CONFIG.batchSize,
    exportMaxRecords:
      parseInt(process.env.EXPORT_MAX_RECORDS || '', 10) || DEFAULT_EXPORT_CONFIG.exportMaxRecords,
    workerConcurrency:
      shared.workerConcurrency || DEFAULT_SHARED_IMPORT_EXPORT_CONFIG.workerConcurrency,
    fileRetentionHours:
      parseInt(process.env.EXPORT_FILE_RETENTION_HOURS || '', 10) ||
      DEFAULT_EXPORT_CONFIG.fileRetentionHours,
    exportStoragePath: process.env.EXPORT_STORAGE_PATH || DEFAULT_EXPORT_CONFIG.exportStoragePath,
    exportStreamMaxLimit:
      parseInt(process.env.EXPORT_STREAM_MAX_LIMIT || '', 10) ||
      DEFAULT_EXPORT_CONFIG.exportStreamMaxLimit,
    exportRateLimitPerHour:
      parseInt(process.env.EXPORT_RATE_LIMIT_PER_HOUR || '', 10) ||
      DEFAULT_EXPORT_CONFIG.exportRateLimitPerHour,
    exportConcurrentLimitUser:
      parseInt(process.env.EXPORT_CONCURRENT_LIMIT_USER || '', 10) ||
      DEFAULT_EXPORT_CONFIG.exportConcurrentLimitUser,
    exportConcurrentLimitGlobal:
      parseInt(process.env.EXPORT_CONCURRENT_LIMIT_GLOBAL || '', 10) ||
      DEFAULT_EXPORT_CONFIG.exportConcurrentLimitGlobal,
  };
}
