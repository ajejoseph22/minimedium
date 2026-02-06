import axios from 'axios';
import { randomUUID } from 'crypto';
import { createReadStream, createWriteStream, promises as fs } from 'fs';
import { lookup } from 'dns/promises';
import multer from 'multer';
import path from 'path';
import { Readable, Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { createImportStorageAdapter } from '../../storage';
import { loadImportConfig } from './config';
import { FileErrorCode, ImportExportErrorCode } from '../shared/import-export/types';
import { isLocalHostname, isPrivateIp } from '../shared/import-export/utils';

const DEFAULT_URL_TIMEOUT_MS = 30000;
const ALLOWED_CONTENT_TYPES = new Set([
  'application/json',
  'application/ndjson',
  'application/x-ndjson',
  'application/jsonl',
  'text/plain',
  'text/json',
]);

const config = loadImportConfig();

export class ImportExportError extends Error {
  constructor(
    public code: ImportExportErrorCode,
    message: string,
  ) {
    super(message);
    this.name = 'ImportExportError';
  }
}

export interface ImportIntakeResult {
  key: string;
  location: string;
  bytes: number;
  fileName: string;
  mimeType: string | null;
  sourceType: 'upload' | 'url';
  sourceUrl?: string;
}

export interface UploadedFile {
  filename: string;
  path: string;
  originalname: string;
  mimetype: string;
  size: number;
}

export interface RemoteFetchOptions {
  url: string;
  timeoutMs?: number;
  maxFileSize?: number;
  allowedHosts?: string[];
}

export function createImportUploadMiddleware(fieldName = 'file') {
  const uploadPath = config.importStoragePath;

  const storage = multer.diskStorage({
    destination: async (_req, _file, cb) => {
      try {
        await fs.mkdir(uploadPath, { recursive: true });
        cb(null, uploadPath);
      } catch (error) {
        cb(error as Error, uploadPath);
      }
    },
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase();
      cb(null, `${Date.now()}-${randomUUID()}${ext}`);
    },
  });

  const upload = multer({
    storage,
    limits: {
      fileSize: config.maxFileSize,
      files: 1,
    },
    fileFilter: (_req, file, cb) => {
      const contentType = normalizeContentType(file.mimetype);
      if (!contentType || !ALLOWED_CONTENT_TYPES.has(contentType)) {
        return cb(new ImportExportError(FileErrorCode.UNSUPPORTED_FORMAT, 'Unsupported content type'));
      }
      cb(null, true);
    },
  });

  return upload.single(fieldName);
}

export function mapUploadedFileToImportIntakeResult(file: UploadedFile): ImportIntakeResult {
  return {
    key: file.filename,
    location: file.path,
    bytes: file.size,
    fileName: file.originalname,
    mimeType: file.mimetype || null,
    sourceType: 'upload',
  };
}

export async function validateUploadedFile(file: UploadedFile): Promise<void> {
  const contentType = normalizeContentType(file.mimetype);
  if (!contentType || !ALLOWED_CONTENT_TYPES.has(contentType)) {
    throw new ImportExportError(FileErrorCode.UNSUPPORTED_FORMAT, 'Unsupported content type');
  }
  if (file.size === 0) {
    throw new ImportExportError(FileErrorCode.EMPTY_FILE, 'Uploaded file is empty');
  }
  if (file.size > config.maxFileSize) {
    throw new ImportExportError(FileErrorCode.FILE_TOO_LARGE, 'Uploaded file exceeds size limit');
  }
}

export async function fetchRemoteImport(options: RemoteFetchOptions): Promise<ImportIntakeResult> {
  const url = parseUrl(options.url);
  await assertRemoteUrlAllowed(url, options.allowedHosts ?? config.allowedHosts);

  const timeoutMs = options.timeoutMs ?? DEFAULT_URL_TIMEOUT_MS;
  const maxFileSize = options.maxFileSize ?? config.maxFileSize;
  const storage = createImportStorageAdapter();

  const fileName = decodeURIComponent(path.basename(url.pathname)) || `import-${randomUUID()}`;
  const key = `${randomUUID()}${path.extname(fileName)}`;

  let response;

  try {
    response = await axios.get(url.toString(), {
      responseType: 'stream',
      timeout: timeoutMs,
      maxRedirects: 0,
      validateStatus: (status) => status >= 200 && status < 300,
      headers: {
        Accept: Array.from(ALLOWED_CONTENT_TYPES).join(','),
      },
    });
  } catch (error) {
    throw new ImportExportError(FileErrorCode.URL_FETCH_FAILED, 'Failed to fetch remote file');
  }

  const contentType = normalizeContentType(response.headers['content-type']);
  if (!contentType || !ALLOWED_CONTENT_TYPES.has(contentType)) {
    throw new ImportExportError(FileErrorCode.UNSUPPORTED_FORMAT, 'Unsupported content type');
  }

  const contentLength = Number(response.headers['content-length'] ?? 0);
  if (contentLength && contentLength > maxFileSize) {
    throw new ImportExportError(FileErrorCode.FILE_TOO_LARGE, 'Remote file exceeds size limit');
  }

  const { stream: limitedStream } = createSizeLimitStream(response.data, maxFileSize);
  let result;

  try {
    result = await storage.saveStream(key, limitedStream);
  } catch (error) {
    await storage.delete(key);
    if (error instanceof ImportExportError) {
      throw error;
    }
    throw new ImportExportError(FileErrorCode.FILE_WRITE_ERROR, 'Failed to store remote file');
  }

  if (result.bytes === 0) {
    await storage.delete(key);
    throw new ImportExportError(FileErrorCode.EMPTY_FILE, 'Remote file is empty');
  }

  return {
    key: result.key,
    location: result.location,
    bytes: result.bytes,
    fileName,
    mimeType: contentType,
    sourceType: 'url',
    sourceUrl: url.toString(),
  };
}

function normalizeContentType(contentType?: string): string | null {
  if (!contentType) {
    return null;
  }
  return contentType.split(';')[0]?.trim().toLowerCase() ?? null;
}

function parseUrl(value: string): URL {
  let url: URL;

  try {
    url = new URL(value);
  } catch (error) {
    throw new ImportExportError(FileErrorCode.URL_FETCH_FAILED, 'Invalid URL');
  }
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new ImportExportError(FileErrorCode.URL_NOT_ALLOWED, 'URL scheme must be http or https');
  }
  return url;
}

async function assertRemoteUrlAllowed(url: URL, allowedHosts: string[]): Promise<void> {
  const hostname = url.hostname.toLowerCase();
  if (!isHostAllowed(hostname, allowedHosts)) {
    throw new ImportExportError(FileErrorCode.URL_NOT_ALLOWED, 'Host is not in allowlist');
  }

  if (isLocalHostname(hostname)) {
    throw new ImportExportError(FileErrorCode.URL_NOT_ALLOWED, 'Localhost URLs are not allowed');
  }

  const resolved = await lookup(hostname, { all: true });
  for (const { address } of resolved) {
    if (isPrivateIp(address)) {
      throw new ImportExportError(FileErrorCode.URL_NOT_ALLOWED, 'Private IPs are not allowed');
    }
  }
}

function isHostAllowed(host: string, allowedHosts: string[]): boolean {
  if (!allowedHosts.length) {
    return true;
  }

  return allowedHosts.some((allowed) => {
    const normalized = allowed.toLowerCase();
    return host === normalized || host.endsWith(`.${normalized}`);
  });
}

function createSizeLimitStream(stream: Readable, maxBytes: number): { stream: Readable } {
  let total = 0;
  const limiter = new Transform({
    transform(chunk, _encoding, callback) {
      total += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(String(chunk));
      if (total > maxBytes) {
        callback(new ImportExportError(FileErrorCode.FILE_TOO_LARGE, 'File exceeds size limit'));
        return;
      }
      callback(null, chunk);
    },
  });

  const limited = stream.pipe(limiter);
  limited.on('error', () => {
    stream.destroy();
  });

  return { stream: limited };
}

export async function saveStreamToStorage(key: string, stream: Readable): Promise<ImportIntakeResult> {
  const storage = createImportStorageAdapter();
  const result = await storage.saveStream(key, stream);
  return {
    key: result.key,
    location: result.location,
    bytes: result.bytes,
    fileName: path.basename(key),
    mimeType: null,
    sourceType: 'upload',
  };
}

export async function moveUploadedFileToStorage(file: UploadedFile): Promise<ImportIntakeResult> {
  const storage = createImportStorageAdapter();
  const key = file.filename;
  const destPath = storage.getLocalPath(key);

  if (file.path !== destPath) {
    await fs.mkdir(path.dirname(destPath), { recursive: true });
    try {
      await fs.rename(file.path, destPath);
    } catch (error) {
      await pipeline(createReadStream(file.path), createWriteStream(destPath));
      await fs.rm(file.path, { force: true });
    }
  }

  return {
    key,
    location: destPath,
    bytes: file.size,
    fileName: file.originalname,
    mimeType: file.mimetype || null,
    sourceType: 'upload',
  };
}
