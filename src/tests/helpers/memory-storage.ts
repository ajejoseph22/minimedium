import { StorageAdapter } from '../../app/storage';

export interface MemoryStoredFile {
  key: string;
  data: string;
  bytes: number;
  location: string;
}

export interface MemoryStorageHarness {
  storage: StorageAdapter;
  savedFiles: MemoryStoredFile[];
  saveStreamMock: jest.Mock;
  deleteMock: jest.Mock;
}

export function createMemoryStorageAdapter(basePath = '/tmp'): MemoryStorageHarness {
  const savedFiles: MemoryStoredFile[] = [];

  const saveStreamMock = jest.fn(async (key: string, stream: NodeJS.ReadableStream) => {
    const chunks: Buffer[] = [];
    await new Promise<void>((resolve, reject) => {
      stream.on('data', (chunk) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
      });
      stream.on('end', () => resolve());
      stream.on('close', () => resolve());
      stream.on('error', (error) => reject(error));
    });

    const data = Buffer.concat(chunks).toString('utf8');
    const bytes = Buffer.byteLength(data);
    const location = `${basePath}/${key}`;
    savedFiles.push({ key, data, bytes, location });

    return { key, bytes, location };
  });

  const deleteMock = jest.fn(async () => undefined);

  const storage: StorageAdapter = {
    saveStream: saveStreamMock,
    saveBuffer: async (key: string, data: Buffer) => {
      const content = data.toString('utf8');
      const bytes = data.length;
      const location = `${basePath}/${key}`;
      savedFiles.push({ key, data: content, bytes, location });
      return { key, bytes, location };
    },
    createReadStream: () => {
      throw new Error('Not implemented for test memory storage');
    },
    getLocalPath: (key: string) => `${basePath}/${key}`,
    delete: deleteMock,
  };

  return { storage, savedFiles, saveStreamMock, deleteMock };
}
