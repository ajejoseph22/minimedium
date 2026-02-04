import { loadConfig } from '../routes/import-export';
import { LocalStorage } from './local-storage';
import { StorageAdapter } from './storage';

export function createExportStorageAdapter(): StorageAdapter {
  const { exportStoragePath } = loadConfig();
  return new LocalStorage(exportStoragePath);
}

export function createImportStorageAdapter(): StorageAdapter {
  const { importStoragePath } = loadConfig();
  return new LocalStorage(importStoragePath);
}

export * from './storage';
export * from './local-storage';
