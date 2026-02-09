import { Readable } from 'stream';

export function createUploadedFile(
  overrides: Partial<Express.Multer.File> = {},
): Express.Multer.File {
  return {
    fieldname: 'file',
    originalname: 'import.ndjson',
    encoding: '7bit',
    mimetype: 'application/x-ndjson',
    size: 128,
    destination: '/tmp',
    filename: 'upload.ndjson',
    path: '/tmp/upload.ndjson',
    buffer: Buffer.alloc(0),
    stream: Readable.from([]),
    ...overrides,
  };
}
