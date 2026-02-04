import { Readable } from 'stream';

export interface StorageSaveResult {
  key: string;
  bytes: number;
  location: string;
}

export interface StorageAdapter {
  saveStream(key: string, stream: Readable): Promise<StorageSaveResult>;
  saveBuffer(key: string, data: Buffer): Promise<StorageSaveResult>;
  createReadStream(key: string): Readable;
  getLocalPath(key: string): string;
  delete(key: string): Promise<void>;
}
