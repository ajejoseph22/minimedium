import { createReadStream } from 'fs';
import path from 'path';
import { Readable } from 'stream';
import type { Prisma, PrismaClient } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import { createImportStorageAdapter } from '../../storage';
import { loadConfig } from './config';
import { ImportExportError } from './intake.service';
import { ImportExportParseError, parseJsonArrayStream, parseNdjsonStream } from './parsing.service';
import { upsertImportRecords, IndexedImportRecord } from './upsert.service';
import { validateImportRecord } from './validation.service';
import { createValidationCache } from './validation.validators';
import {
  CreateRecordErrorOptions,
  EntityType,
  ErrorCodeNames,
  FileErrorCode,
  FileFormat,
  ImportExportErrorCode,
  ImportRecord,
  JobStatus,
  ResourceErrorCode,
  SystemErrorCode,
} from './types';
import { pathExists } from './utils';

export interface RunImportJobOptions {
  prisma?: PrismaClient;
  now?: () => Date;
  cancelCheckInterval?: number;
}

export interface RunImportJobResult {
  status: JobStatus;
  processedRecords: number;
  successCount: number;
  errorCount: number;
}

interface RecordErrorPayload {
  error: CreateRecordErrorOptions;
  recordId: string | null;
  details?: Prisma.InputJsonValue | null;
}

const ERROR_FLUSH_SIZE = 500;
const DEFAULT_CANCEL_CHECK_INTERVAL = 500;

class ImportPipelineError extends Error {
  constructor(
    public code: ImportExportErrorCode,
    message: string,
    public details?: Prisma.InputJsonValue,
  ) {
    super(message);
    this.name = 'ImportPipelineError';
  }
}

export async function runImportJob(jobId: string, options: RunImportJobOptions = {}): Promise<RunImportJobResult> {
  const prisma = options.prisma ?? prismaClient;
  const now = options.now ?? (() => new Date());
  const cancelCheckInterval = options.cancelCheckInterval ?? DEFAULT_CANCEL_CHECK_INTERVAL;
  const config = loadConfig();

  const job = await prisma.importJob.findUnique({ where: { id: jobId } });
  if (!job) {
    throw new ImportPipelineError(ResourceErrorCode.JOB_NOT_FOUND, `Import job ${jobId} not found`);
  }

  if (job.status === 'cancelled') {
    await markJobCancelled(prisma, jobId, now());
    return {
      status: 'cancelled',
      processedRecords: job.processedRecords,
      successCount: job.successCount,
      errorCount: job.errorCount,
    };
  }

  await prisma.importJob.update({
    where: { id: jobId },
    data: {
      status: 'running',
      startedAt: job.startedAt ?? now(),
    },
  });

  const entityType = normalizeEntityType(job.resource);
  const format = detectFormat(job.format, job.fileName ?? job.sourceLocation ?? '');
  const inputStream = await openImportSource(job.sourceLocation);
  const parser = format === 'ndjson' ? parseNdjsonStream : parseJsonArrayStream;

  const validationCache = createValidationCache();
  let processedRecords = 0;
  let successCount = 0;
  let errorCount = 0;
  let cancelled = false;

  let pendingRecords: IndexedImportRecord[] = [];
  let pendingErrors: RecordErrorPayload[] = [];
  const errorRecordIndexes = new Set<number>();

  const addErrorRecordIndexes = (entries: RecordErrorPayload[]): number => {
    let added = 0;
    for (const entry of entries) {
      const index = entry.error.recordIndex;
      if (index >= 0 && !errorRecordIndexes.has(index)) {
        errorRecordIndexes.add(index);
        added += 1;
      }
    }
    return added;
  };

  const flushErrors = async (): Promise<void> => {
    if (!pendingErrors.length) {
      return;
    }

    const payload = pendingErrors.map((entry) =>
      buildImportErrorPayload(entry.error, entry.recordId, entry.details),
    );
    const result = await prisma.importError.createMany({ data: payload });
    pendingErrors = [];
    void result;
  };

  const flushRecords = async () => {
    if (!pendingRecords.length) {
      return;
    }

    const recordMap = new Map<number, ImportRecord>();

    for (const entry of pendingRecords) {
      recordMap.set(entry.recordIndex, entry.record);
    }

    const result = await upsertImportRecords(pendingRecords, entityType, {
      jobId,
      batchSize: config.batchSize,
      prisma,
    });

    successCount += result.succeeded;

    if (result.errors.length) {
      const newErrors = result.errors.map((error) => ({
        error,
        recordId: extractRecordId(entityType, recordMap.get(error.recordIndex)),
      }));
      pendingErrors.push(...newErrors);
      errorCount += addErrorRecordIndexes(newErrors);
    }

    pendingRecords = [];
    if (pendingErrors.length >= ERROR_FLUSH_SIZE) {
      await flushErrors();
    }
  };

  try {
    for await (const parsed of parser(inputStream, { maxRecords: config.maxRecords })) {
      processedRecords += 1;

      const validation = await validateImportRecord(parsed.record, entityType, {
        jobId,
        recordIndex: parsed.index,
        prisma,
        cache: validationCache,
      });

      if (!validation.valid) {
        const newErrors = validation.errors.map((error) => ({
          error,
          recordId: extractRecordId(entityType, parsed.record as ImportRecord),
        }));
        pendingErrors.push(...newErrors);
        errorCount += addErrorRecordIndexes(newErrors);
        if (pendingErrors.length >= ERROR_FLUSH_SIZE) {
          await flushErrors();
        }
      } else if (validation.record) {
        pendingRecords.push({ record: validation.record, recordIndex: parsed.index });
        if (pendingRecords.length >= config.batchSize) {
          await flushRecords();
        }
      }

      if (cancelCheckInterval && processedRecords % cancelCheckInterval === 0) {
        if (await jobIsCancelled(prisma, jobId)) {
          cancelled = true;
          break;
        }
      }
    }

    if (!cancelled) {
      await flushRecords();
    }
    await flushErrors();

    if (cancelled) {
      await finalizeJob(prisma, jobId, {
        status: 'cancelled',
        processedRecords,
        successCount,
        errorCount,
        totalRecords: processedRecords,
        finishedAt: now(),
      });
      return { status: 'cancelled', processedRecords, successCount, errorCount };
    }

    if (processedRecords === 0) {
      throw new ImportPipelineError(FileErrorCode.EMPTY_FILE, 'Import file contained no records');
    }

    const status: JobStatus = errorCount > 0 ? 'partial' : 'succeeded';
    await finalizeJob(prisma, jobId, {
      status,
      processedRecords,
      successCount,
      errorCount,
      totalRecords: processedRecords,
      finishedAt: now(),
    });

    return { status, processedRecords, successCount, errorCount };
  } catch (error) {
    await safeFlushErrors(pendingErrors, prisma);
    const { code, details, message } = normalizePipelineError(error);
    const recordIndex = -1;
    const fatalError: RecordErrorPayload = {
      error: {
        jobId,
        recordIndex,
        errorCode: code,
        message,
      },
      recordId: null,
      details,
    };

    try {
      await prisma.importError.createMany({
        data: [buildImportErrorPayload(fatalError.error, fatalError.recordId, fatalError.details)],
      });
    } catch {
      // swallow to ensure job status is updated even if error persistence fails
    }

    await finalizeJob(prisma, jobId, {
      status: 'failed',
      processedRecords,
      successCount,
      errorCount,
      totalRecords: processedRecords,
      finishedAt: now(),
    });

    return { status: 'failed', processedRecords, successCount, errorCount };
  }
}

function detectFormat(format: string | null, fileName: string): FileFormat {
  if (format === 'ndjson' || format === 'json') {
    return format;
  }

  const ext = path.extname(fileName).toLowerCase();
  if (['.ndjson', '.jsonl'].includes(ext)) {
    return 'ndjson';
  }
  if (ext === '.json') {
    return 'json';
  }

  throw new ImportPipelineError(FileErrorCode.UNSUPPORTED_FORMAT, 'Unsupported import format');
}

function normalizeEntityType(resource: string): EntityType {
  if (resource === 'users' || resource === 'articles' || resource === 'comments') {
    return resource;
  }
  throw new ImportPipelineError(ResourceErrorCode.UNSUPPORTED_RESOURCE, `Unsupported resource ${resource}`);
}

async function openImportSource(sourceLocation: string | null): Promise<Readable> {
  if (!sourceLocation) {
    throw new ImportPipelineError(FileErrorCode.FILE_READ_ERROR, 'Import source location missing');
  }

  const storage = createImportStorageAdapter();
  const localCandidate = storage.getLocalPath(sourceLocation);

  if (await pathExists(sourceLocation)) {
    return createReadStream(sourceLocation);
  }
  if (await pathExists(localCandidate)) {
    return createReadStream(localCandidate);
  }

  throw new ImportPipelineError(FileErrorCode.FILE_READ_ERROR, 'Import source file not found');
}



function buildImportErrorPayload(
  error: CreateRecordErrorOptions,
  recordId: string | null,
  details?: Prisma.InputJsonValue | null,
): Prisma.ImportErrorCreateManyInput {
  const errorName = ErrorCodeNames[error.errorCode as ImportExportErrorCode] ?? 'UNKNOWN';
  return {
    jobId: error.jobId,
    recordIndex: error.recordIndex,
    recordId,
    errorCode: error.errorCode,
    errorName,
    message: error.message,
    field: error.field ?? null,
    value: (error.value ?? null) as Prisma.InputJsonValue,
    details: details ?? undefined,
  };
}

function extractRecordId(entityType: EntityType, record?: ImportRecord): string | null {
  if (!record || typeof record !== 'object') {
    return null;
  }

  if ('id' in record && record.id !== undefined && record.id !== null) {
    return String(record.id);
  }

  if (entityType === 'users' && 'email' in record && record.email) {
    return String(record.email);
  }

  if (entityType === 'articles' && 'slug' in record && record.slug) {
    return String(record.slug);
  }

  return null;
}

async function jobIsCancelled(prisma: PrismaClient, jobId: string): Promise<boolean> {
  const status = await prisma.importJob.findUnique({
    where: { id: jobId },
    select: { status: true },
  });
  return status?.status === 'cancelled';
}

async function markJobCancelled(prisma: PrismaClient, jobId: string, timestamp: Date): Promise<void> {
  await prisma.importJob.update({
    where: { id: jobId },
    data: {
      status: 'cancelled',
      finishedAt: timestamp,
    },
  });
}

async function finalizeJob(
  prisma: PrismaClient,
  jobId: string,
  update: {
    status: JobStatus;
    processedRecords: number;
    successCount: number;
    errorCount: number;
    totalRecords: number | null;
    finishedAt: Date;
  },
): Promise<void> {
  await prisma.importJob.update({
    where: { id: jobId },
    data: {
      status: update.status,
      processedRecords: update.processedRecords,
      successCount: update.successCount,
      errorCount: update.errorCount,
      totalRecords: update.totalRecords,
      finishedAt: update.finishedAt,
    },
  });
}

function normalizePipelineError(error: unknown): {
  code: ImportExportErrorCode;
  message: string;
  details?: Prisma.InputJsonValue;
} {
  if (error instanceof ImportExportParseError) {
    return {
      code: error.code,
      message: error.message,
      details: error.details as Prisma.InputJsonValue,
    };
  }

  if (error instanceof ImportExportError || error instanceof ImportPipelineError) {
    return {
      code: error.code,
      message: error.message,
      details: error instanceof ImportPipelineError ? error.details : undefined,
    };
  }

  if (error instanceof Error) {
    return {
      code: SystemErrorCode.INTERNAL_ERROR,
      message: error.message,
      details: { name: error.name, stack: error.stack } as Prisma.InputJsonValue,
    };
  }

  return {
    code: SystemErrorCode.INTERNAL_ERROR,
    message: 'Unknown error while processing import',
    details: { error: String(error) } as Prisma.InputJsonValue,
  };
}

async function safeFlushErrors(
  pendingErrors: RecordErrorPayload[],
  prisma: PrismaClient,
): Promise<void> {
  if (!pendingErrors.length) {
    return;
  }

  try {
    const payload = pendingErrors.map((entry) =>
      buildImportErrorPayload(entry.error, entry.recordId, entry.details),
    );
    const result = await prisma.importError.createMany({ data: payload });
    void result;
  } catch {
    // ignore to avoid masking the primary failure
  }
}
