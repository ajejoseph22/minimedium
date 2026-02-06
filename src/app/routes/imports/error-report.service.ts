import { PassThrough } from 'stream';
import path from 'path';
import type { PrismaClient } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import { createErrorReportStorageAdapter, StorageAdapter } from '../../storage';
import { FileFormat } from '../shared/import-export/types';

export interface GenerateImportErrorReportOptions {
  prisma?: PrismaClient;
  storage?: StorageAdapter;
  format: FileFormat;
  pageSize?: number;
  key?: string;
}

export interface GenerateImportErrorReportResult {
  key: string;
  location: string;
  bytes: number;
  format: FileFormat;
  errorCount: number;
}

const DEFAULT_PAGE_SIZE = 1000;

export async function generateImportErrorReport(
  jobId: string,
  options: GenerateImportErrorReportOptions,
): Promise<GenerateImportErrorReportResult> {
  const prisma = options.prisma ?? prismaClient;
  const storage = options.storage ?? createErrorReportStorageAdapter();
  const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;
  const format = options.format;
  const key =
    options.key ?? path.posix.join('import-errors', `${jobId}.${format === 'json' ? 'json' : 'ndjson'}`);

  const stream = new PassThrough();
  const savePromise = storage.saveStream(key, stream);

  let cursor: string | null = null;
  let total = 0;
  let first = true;

  if (format === 'json') {
    stream.write('[');
  }

  while (true) {
    const batch = await prisma.importError.findMany({
      where: { jobId },
      orderBy: { id: 'asc' },
      take: pageSize,
      ...(cursor ? { cursor: { id: cursor }, skip: 1 } : {}),
      select: {
        id: true,
        recordIndex: true,
        recordId: true,
        errorCode: true,
        errorName: true,
        message: true,
        field: true,
        value: true,
        details: true,
        createdAt: true,
      },
    });

    if (!batch.length) {
      break;
    }

    for (const error of batch) {
      const payload = {
        recordIndex: error.recordIndex,
        recordId: error.recordId,
        errorCode: error.errorCode,
        errorName: error.errorName,
        message: error.message,
        field: error.field,
        value: error.value,
        details: error.details,
        createdAt: error.createdAt,
      };
      const chunk = JSON.stringify(payload);
      if (format === 'ndjson') {
        stream.write(`${chunk}\n`);
      } else {
        if (!first) {
          stream.write(',');
        }
        stream.write(chunk);
        first = false;
      }
      total += 1;
    }

    cursor = batch[batch.length - 1]?.id ?? null;
  }

  if (format === 'json') {
    stream.write(']');
  }
  stream.end();

  const saved = await savePromise;
  return {
    key: saved.key,
    location: saved.location,
    bytes: saved.bytes,
    format,
    errorCount: total,
  };
}
