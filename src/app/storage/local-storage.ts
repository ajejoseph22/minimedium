import { createReadStream, createWriteStream, promises as fs } from 'fs';
import path from 'path';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';
import { StorageAdapter, StorageSaveResult } from './storage';

export class LocalStorage implements StorageAdapter {
  constructor(private basePath: string) {}

  async saveStream(key: string, stream: Readable): Promise<StorageSaveResult> {
    const fullPath = this.getLocalPath(key);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });

    let bytes = 0;
    stream.on('data', (chunk) => {
      bytes += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(String(chunk));
    });

    await pipeline(stream, createWriteStream(fullPath));

    return { key, bytes, location: fullPath };
  }

  async saveBuffer(key: string, data: Buffer): Promise<StorageSaveResult> {
    const fullPath = this.getLocalPath(key);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, data);
    return { key, bytes: data.length, location: fullPath };
  }

  createReadStream(key: string): Readable {
    return createReadStream(this.getLocalPath(key));
  }

  getLocalPath(key: string): string {
    return path.join(this.basePath, key);
  }

  async delete(key: string): Promise<void> {
    const fullPath = this.getLocalPath(key);
    await fs.rm(fullPath, { force: true });
  }
}
