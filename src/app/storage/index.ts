import { loadImportConfig } from '../routes/imports/config';
import { loadExportConfig } from '../routes/exports/config';
import { LocalStorage } from './local-storage';
import { StorageAdapter } from './storage';

export function createExportStorageAdapter(): StorageAdapter {
  const { exportStoragePath } = loadExportConfig();
  return new LocalStorage(exportStoragePath);
}

export function createErrorReportStorageAdapter(): StorageAdapter {
  const { errorReportStoragePath } = loadImportConfig();
  return new LocalStorage(errorReportStoragePath);
}

export function createImportStorageAdapter(): StorageAdapter {
  const { importStoragePath } = loadImportConfig();
  return new LocalStorage(importStoragePath);
}

export * from './storage';
export * from './local-storage';
