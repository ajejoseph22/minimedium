import { createReadStream, promises as fs } from 'fs';
import path from 'path';
import { Readable } from 'stream';
import type { Prisma, PrismaClient } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import { createImportStorageAdapter } from '../../storage';
import { loadImportConfig } from './config';
import {
  fetchRemoteImport,
  ImportExportError,
  mapUploadedFileToImportIntakeResult,
  UploadedFile,
  validateUploadedFile
} from './intake.service';
import { ImportExportParseError, parseJsonArrayStream, parseNdjsonStream } from './parsing.service';
import { IndexedImportRecord, upsertImportRecords } from './upsert.service';
import { validateImportRecord } from './validation/validation.service';
import { createValidationCache } from './validation/validation.validators';
import { generateImportErrorReport } from './error-report.service';
import {
  CreateRecordErrorOptions,
  EntityType,
  ErrorCodeNames,
  FileErrorCode,
  FileFormat,
  ImportExportErrorCode,
  ImportRecord,
  JobStatus,
  ProcessingErrorCode,
  ResourceErrorCode,
  SystemErrorCode
} from '../shared/import-export/types';
import { logJobLifecycleEvent } from '../../jobs/observability';
import { createLogger } from '../../logger';
import {
  isObject,
  isPrismaUniqueConstraintError,
  parseEntityType,
  parseFormat,
  pathExists,
  toJsonObject
} from '../shared/import-export/utils';
import type {
  CreateImportJobOptions,
  CreateImportJobResult,
  ErrorReportFileMetadata,
  GetErrorReportFileOptions,
  GetImportJobStatusOptions,
  ImportCreatePayload,
  ImportIntakeResult,
  RecordErrorPayload,
  RunImportJobOptions,
  RunImportJobResult
} from './import.model';
import HttpException from '../../models/http-exception.model';
import { HttpStatusCode } from '../../models/http-status-code.model';

const ERROR_FLUSH_SIZE = 500;
const DEFAULT_CANCEL_CHECK_INTERVAL = 500;
const logger = createLogger({ component: 'import.service' });

class ImportServiceError extends Error {
  constructor(
    public code: ImportExportErrorCode,
    message: string,
    public details?: Prisma.InputJsonValue,
  ) {
    super(message);
    this.name = 'ImportServiceError';
  }
}

export async function runImportJob(jobId: string, options: RunImportJobOptions = {}): Promise<RunImportJobResult> {
  const prisma = options.prisma ?? prismaClient;
  const now = options.now ?? (() => new Date());
  const cancelCheckInterval = options.cancelCheckInterval ?? DEFAULT_CANCEL_CHECK_INTERVAL;
  const config = loadImportConfig();

  const startedAt = now();
  const claimResult = await prisma.importJob.updateMany({
    where: {
      id: jobId,
      status: 'queued',
    },
    data: {
      status: 'running',
      startedAt,
    },
  });


  const job = await prisma.importJob.findUnique({ where: { id: jobId } });
  if (!job) {
    throw new ImportServiceError(ResourceErrorCode.JOB_NOT_FOUND, `Import job ${jobId} not found`);
  }

  // Job claimed by another worker process, return current status without processing
  if (!claimResult.count) {
    return {
      status: job.status,
      processedRecords: job.processedRecords,
      successCount: job.successCount,
      errorCount: job.errorCount,
    };
  }

  // Job claimed successfully by this worker process, proceeed with processing.
  // Pre-run cancellation check
  if (job.status === 'cancelled') {
    const finishedAt = now();
    await markJobCancelled(prisma, jobId, finishedAt);
    logJobLifecycleEvent({
      event: 'job.completed',
      jobKind: 'import',
      jobId,
      status: 'cancelled',
      resource: job.resource,
      format: job.format,
      timestamp: finishedAt,
      jobStartedAt: job.startedAt ?? finishedAt,
      counters: {
        processedRecords: job.processedRecords,
        successCount: job.successCount,
        errorCount: job.errorCount,
      },
    });

    return {
      status: 'cancelled',
      processedRecords: job.processedRecords,
      successCount: job.successCount,
      errorCount: job.errorCount,
    };
  }

  logJobLifecycleEvent({
    event: 'job.started',
    jobKind: 'import',
    jobId,
    status: 'running',
    resource: job.resource,
    format: job.format,
    timestamp: job.startedAt ?? startedAt,
    counters: {
      processedRecords: job.processedRecords,
      successCount: job.successCount,
      errorCount: job.errorCount,
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
  let persistedErrorCount = 0;
  let errorPersistenceFailures = 0;
  let cancelled = false;

  let pendingRecords: IndexedImportRecord[] = [];
  let pendingErrors: RecordErrorPayload[] = [];
  const errorRecordIndexes = new Set<number>();
  let errorReportLocation: string | null = null;
  let errorReportFormat: FileFormat | null = null;
  let errorReportGenerationFailed = false;

  const generateErrorReport = async (): Promise<void> => {
    if (persistedErrorCount <= 0) {
      return;
    }
    const reportFormat = job.format === 'json' ? 'json' : 'ndjson';
    try {
      const report = await generateImportErrorReport(jobId, { prisma, format: reportFormat });
      errorReportLocation = report.location;
      errorReportFormat = report.format;
    } catch (error) {
      errorReportGenerationFailed = true;
      logger.warn({
        event: 'Import error report generation failed',
        jobId,
        reportFormat,
        errorName: error instanceof Error ? error.name : 'UnknownError',
        errorMessage: error instanceof Error ? error.message : String(error),
      });
    }
  };

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
    try {
      const result = await prisma.importError.createMany({ data: payload });
      persistedErrorCount += typeof result?.count === 'number' ? result.count : payload.length;
    } catch (error) {
      errorPersistenceFailures += payload.length;
      logger.warn({
        event: 'Import error persistence failed',
        jobId,
        batchSize: payload.length,
        errorName: error instanceof Error ? error.name : 'UnknownError',
        errorMessage: error instanceof Error ? error.message : String(error),
      });
    }
    pendingErrors = [];
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
      assertRecordShapeMatchesFormat(parsed.record, format, parsed.index, parsed.lineNumber);
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
      const finishedAt = now();
      await finalizeJob(prisma, jobId, {
        status: 'cancelled',
        processedRecords,
        successCount,
        errorCount,
        totalRecords: processedRecords,
        finishedAt,
        errorSummary: buildErrorSummary(
          persistedErrorCount,
          errorPersistenceFailures,
          errorReportLocation,
          errorReportFormat,
          errorReportGenerationFailed,
        ),
      });
      logJobLifecycleEvent({
        event: 'job.completed',
        jobKind: 'import',
        jobId,
        status: 'cancelled',
        resource: job.resource,
        format: job.format,
        timestamp: finishedAt,
        jobStartedAt: startedAt,
        counters: {
          processedRecords,
          successCount,
          errorCount,
        },
      });
      return { status: 'cancelled', processedRecords, successCount, errorCount };
    }

    if (processedRecords === 0) {
      throw new ImportServiceError(FileErrorCode.EMPTY_FILE, 'Import file contained no records');
    }

    await generateErrorReport();

    let status: JobStatus;
    if (errorCount === 0) {
      status = 'succeeded';
    } else if (successCount > 0) {
      status = 'partial';
    } else {
      status = 'failed';
    }
    const finishedAt = now();
    await finalizeJob(prisma, jobId, {
      status,
      processedRecords,
      successCount,
      errorCount,
      totalRecords: processedRecords,
      finishedAt,
      errorSummary: buildErrorSummary(
        persistedErrorCount,
        errorPersistenceFailures,
        errorReportLocation,
        errorReportFormat,
        errorReportGenerationFailed,
      ),
    });
    logJobLifecycleEvent({
      event: 'job.completed',
      jobKind: 'import',
      jobId,
      status,
      resource: job.resource,
      format: job.format,
      timestamp: finishedAt,
      jobStartedAt: startedAt,
      counters: {
        processedRecords,
        successCount,
        errorCount,
      },
    });

    return { status, processedRecords, successCount, errorCount };
  } catch (error) {
    await safeFlushErrors(pendingErrors, prisma, {
      onPersisted: (count) => {
        persistedErrorCount += count;
      },
      onFailed: (count) => {
        errorPersistenceFailures += count;
      },
    });
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

    errorCount += addErrorRecordIndexes([fatalError]);

    const fatalPayload = buildImportErrorPayload(fatalError.error, fatalError.recordId, fatalError.details);
    const fatalPersisted = await persistFatalImportError(prisma, fatalPayload, jobId);
    if (fatalPersisted) {
      persistedErrorCount += 1;
    } else {
      errorPersistenceFailures += 1;
    }

    await generateErrorReport();

    const finishedAt = now();
    await finalizeJob(prisma, jobId, {
      status: 'failed',
      processedRecords,
      successCount,
      errorCount,
      totalRecords: processedRecords,
      finishedAt,
      errorSummary: buildErrorSummary(
        persistedErrorCount,
        errorPersistenceFailures,
        errorReportLocation,
        errorReportFormat,
        errorReportGenerationFailed,
        {
          code,
          message,
          details,
        },
      ),
    });
    logJobLifecycleEvent({
      event: 'job.completed',
      jobKind: 'import',
      jobId,
      status: 'failed',
      resource: job.resource,
      format: job.format,
      timestamp: finishedAt,
      jobStartedAt: startedAt,
      counters: {
        processedRecords,
        successCount,
        errorCount,
      },
      level: 'error',
      details: {
        errorCode: code,
        message,
        details,
      },
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

  throw new ImportServiceError(FileErrorCode.UNSUPPORTED_FORMAT, 'Unsupported import format');
}

function normalizeEntityType(resource: string): EntityType {
  if (resource === 'users' || resource === 'articles' || resource === 'comments') {
    return resource;
  }
  throw new ImportServiceError(ResourceErrorCode.UNSUPPORTED_RESOURCE, `Unsupported resource ${resource}`);
}

async function openImportSource(sourceLocation: string | null): Promise<Readable> {
  if (!sourceLocation) {
    throw new ImportServiceError(FileErrorCode.FILE_READ_ERROR, 'Import source location missing');
  }

  const storage = createImportStorageAdapter();
  const localCandidate = storage.getLocalPath(sourceLocation);

  if (await pathExists(sourceLocation)) {
    return createReadStream(sourceLocation);
  }
  if (await pathExists(localCandidate)) {
    return createReadStream(localCandidate);
  }

  throw new ImportServiceError(FileErrorCode.FILE_READ_ERROR, 'Import source file not found');
}



function buildImportErrorPayload(
  error: CreateRecordErrorOptions,
  recordId: string | null,
  details?: Prisma.InputJsonValue,
): Prisma.ImportErrorCreateManyInput {
  const errorName =
    ErrorCodeNames[error.errorCode as ImportExportErrorCode] ?? 'UNKNOWN';

  return {
    jobId: error.jobId,
    recordIndex: error.recordIndex,
    recordId,
    errorCode: error.errorCode,
    errorName,
    message: error.message,
    ...(error.field && { field: error.field }),
    ...(error.value && { value: error.value as Prisma.InputJsonValue }),
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

async function persistFatalImportError(
  prisma: PrismaClient,
  payload: Prisma.ImportErrorCreateManyInput,
  jobId: string,
): Promise<boolean> {
  try {
    const result = await prisma.importError.createMany({ data: [payload] });
    return (typeof result?.count === 'number' ? result.count : 1) > 0;
  } catch (error) {
    logger.warn({
      event: 'Failed to persist bulk import fatal error',
      jobId,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }

  try {
    await prisma.importError.create({
      data: payload,
    });
    return true;
  } catch (error) {
    logger.warn({
      event: "Failed to persist import fatal error'",
      jobId,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
    });
    return false;
  }
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
    errorSummary?: Prisma.InputJsonValue;
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
      errorSummary: update.errorSummary,
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

  if (error instanceof ImportExportError || error instanceof ImportServiceError) {
    return {
      code: error.code,
      message: error.message,
      details: error instanceof ImportServiceError ? error.details : undefined,
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

function assertRecordShapeMatchesFormat(
  record: unknown,
  format: FileFormat,
  recordIndex: number,
  lineNumber?: number,
): void {
  if (format !== 'ndjson') {
    return;
  }

  const isObjectRecord = typeof record === 'object' && record !== null && !Array.isArray(record);
  if (isObjectRecord) {
    return;
  }

  throw new ImportServiceError(
    ProcessingErrorCode.INVALID_RECORD_STRUCTURE,
    'NDJSON records must be JSON objects.',
    {
      recordIndex,
      lineNumber,
      actualType: describeJsonType(record),
    } as Prisma.InputJsonValue,
  );
}

function describeJsonType(value: unknown): string {
  if (Array.isArray(value)) {
    return 'array';
  }
  if (value === null) {
    return 'null';
  }
  return typeof value;
}

async function safeFlushErrors(
  pendingErrors: RecordErrorPayload[],
  prisma: PrismaClient,
  handlers: { onPersisted: (count: number) => void; onFailed: (count: number) => void },
): Promise<void> {
  if (!pendingErrors.length) {
    return;
  }

  try {
    const payload = pendingErrors.map((entry) =>
      buildImportErrorPayload(entry.error, entry.recordId, entry.details),
    );
    const result = await prisma.importError.createMany({ data: payload });
    handlers.onPersisted(typeof result?.count === 'number' ? result.count : payload.length);
  } catch (error) {
    handlers.onFailed(pendingErrors.length);
    logger.warn({
      event: 'Import pending error flush failed',
      batchSize: pendingErrors.length,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

function buildErrorSummary(
  persistedErrorCount: number,
  errorPersistenceFailures: number,
  errorReportLocation: string | null,
  errorReportFormat: FileFormat | null,
  reportGenerationFailed: boolean,
  lastError?: {
    code: ImportExportErrorCode;
    message: string;
    details?: Prisma.InputJsonValue;
  },
): Prisma.InputJsonValue {
  const reportStatus = reportGenerationFailed
    ? 'failed'
    : errorPersistenceFailures === 0
      ? 'complete'
      : persistedErrorCount === 0
        ? 'failed'
        : 'partial';

  const summary: Record<string, unknown> = {
    reportStatus,
    persistedErrorCount,
    persistenceFailures: errorPersistenceFailures,
    reportLocation: errorReportLocation,
    reportFormat: errorReportFormat,
    reportGenerationFailed,
  };

  if (lastError) {
    summary.lastError = {
      code: lastError.code,
      message: lastError.message,
      details: lastError.details ?? undefined,
    };
  }

  return summary as Prisma.InputJsonValue;
}

export function getImportPayload(body: unknown): ImportCreatePayload {
  const candidate = isObject(body) && 'import' in body ? (body as Record<string, unknown>).import : body;
  if (!isObject(candidate)) {
    return {};
  }
  return candidate as ImportCreatePayload;
}

export async function createImportJob(options: CreateImportJobOptions): Promise<CreateImportJobResult> {
  const prisma = options.prisma ?? prismaClient;
  const resource = parseEntityType(options.payload.resource);
  const idempotencyKey = options.idempotencyKey;

  const existing = await prisma.importJob.findFirst({
    where: { createdById: options.createdById, idempotencyKey, resource },
  });

  if (existing) {
    logger.info({
      event: 'Import request deduplicated',
      jobId: existing.id,
      userId: options.createdById,
      resource,
      status: existing.status,
    });
    return { statusCode: 200, importJob: serializeImportJob(existing) };
  }

  let intake: ImportIntakeResult;
  try {
    intake = await resolveImportIntake(options.file, options.payload);
  } catch (error) {
    throw mapImportCreateError(error);
  }

  let format: FileFormat;
  try {
    format = resolveImportFormat(options.payload.format, intake.fileName);
  } catch (error) {
    await cleanupImportIntake(intake);
    throw error;
  }

  let created: Awaited<ReturnType<typeof prisma.importJob.create>>;
  try {
    created = await prisma.importJob.create({
      data: {
        status: 'queued',
        resource,
        format,
        sourceType: intake.sourceType,
        sourceLocation: intake.location,
        fileName: intake.fileName,
        fileSize: intake.bytes,
        idempotencyKey,
        createdById: options.createdById,
        requestHash: null,
      },
    });
  } catch (error) {
    if (isPrismaUniqueConstraintError(error)) {
      const existing = await prisma.importJob.findFirst({
        where: { createdById: options.createdById, idempotencyKey, resource },
      });

      if (existing) {
        await cleanupImportIntake(intake);
        logger.info({
          event: 'Import request deduplicated after create race',
          jobId: existing.id,
          userId: options.createdById,
          resource,
          status: existing.status,
        });
        return { statusCode: 200, importJob: serializeImportJob(existing) };
      }
    }

    await cleanupImportIntake(intake);
    throw error;
  }

  const { enqueueImportJob } = await import('../../jobs/import-export.queue');
  try {
    await enqueueImportJob({
      jobId: created.id,
      resource: created.resource,
      format: created.format,
    });
  } catch (error) {
    await markImportJobEnqueueFailed(prisma, created.id);
    await cleanupImportIntake(intake);
    logger.error({
      event: 'Import job enqueue failed',
      jobId: created.id,
      userId: options.createdById,
      resource: created.resource,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
    });
    throw new HttpException(HttpStatusCode.SERVICE_UNAVAILABLE, {
      errors: { queue: ['failed to enqueue import job'] },
    });
  }

  logger.info({
    event: 'Import job queued',
    jobId: created.id,
    userId: options.createdById,
    resource: created.resource,
    format: created.format,
    sourceType: created.sourceType,
    fileSize: created.fileSize,
    hasIdempotencyKey: true,
  });

  return { statusCode: 202, importJob: serializeImportJob(created) };
}

export async function getImportJobStatus(options: GetImportJobStatusOptions) {
  const prisma = options.prisma ?? prismaClient;
  const job = await prisma.importJob.findFirst({
    where: { id: options.jobId, createdById: options.createdById },
  });

  if (!job) {
    throw new HttpException(HttpStatusCode.NOT_FOUND, { errors: { job: ['import job not found'] } });
  }

  const errorSummary = toJsonObject(job.errorSummary);
  const reportLocation =
    typeof errorSummary?.reportLocation === 'string' ? (errorSummary.reportLocation as string) : null;
  const errorReportStatus =
    typeof errorSummary?.reportStatus === 'string' ? (errorSummary.reportStatus as string) : undefined;

  return {
    importJob: serializeImportJob(job),
    errorReportUrl: reportLocation ? buildImportErrorReportDownloadUrl(job.id) : undefined,
    errorReportStatus,
  };
}

export async function getErrorReportFileMetadata(
  options: GetErrorReportFileOptions,
): Promise<ErrorReportFileMetadata> {
  const prisma = options.prisma ?? prismaClient;
  const job = await prisma.importJob.findFirst({
    where: { id: options.jobId, createdById: options.createdById },
  });

  if (!job) {
    throw new HttpException(HttpStatusCode.NOT_FOUND, { errors: { job: ['import job not found'] } });
  }

  const errorSummary = toJsonObject(job.errorSummary);
  const reportLocation =
    typeof errorSummary?.reportLocation === 'string' ? (errorSummary.reportLocation as string) : null;
  const reportStatus =
    typeof errorSummary?.reportStatus === 'string' ? (errorSummary.reportStatus as string) : undefined;
  const reportFormat = typeof errorSummary?.reportFormat === 'string' ? errorSummary.reportFormat : null;

  if (!reportLocation) {
    throw new HttpException(HttpStatusCode.NOT_FOUND, { errors: { job: ['import error report not found'] } });
  }

  if (reportStatus === 'failed') {
    throw new HttpException(HttpStatusCode.CONFLICT, { errors: { job: ['import error report is not available'] } });
  }

  const format = reportFormat === 'json' ? 'json' : 'ndjson';
  const extension = format === 'json' ? 'json' : 'ndjson';

  return {
    reportLocation,
    contentType: format === 'json' ? 'application/json' : 'application/x-ndjson',
    contentDisposition: `attachment; filename="${job.id}-errors.${extension}"`,
  };
}

export function serializeImportJob(job: {
  id: string;
  status: string;
  resource: string;
  format: string;
  totalRecords: number | null;
  processedRecords: number;
  successCount: number;
  errorCount: number;
  createdAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  fileName: string | null;
  fileSize: number | null;
  sourceLocation: string | null;
  idempotencyKey: string | null;
  errorSummary?: Prisma.JsonValue | null;
}) {
  return {
    id: job.id,
    status: job.status,
    entityType: job.resource,
    format: job.format,
    totalRecords: job.totalRecords,
    processedRecords: job.processedRecords,
    successCount: job.successCount,
    errorCount: job.errorCount,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    completedAt: job.finishedAt,
    createdBy: undefined,
    idempotencyKey: job.idempotencyKey,
    fileName: job.fileName,
    fileSize: job.fileSize,
    sourceUrl: job.sourceLocation,
    errorSummary: sanitizeImportErrorSummary(job.errorSummary),
  };
}

async function resolveImportIntake(file: UploadedFile | undefined, payload: ImportCreatePayload): Promise<ImportIntakeResult> {
  if (file) {
    await validateUploadedFile(file);
    return mapUploadedFileToImportIntakeResult(file);
  }

  if (typeof payload.url === 'string' && payload.url.trim().length > 0) {
    return fetchRemoteImport({ url: payload.url.trim() });
  }

  throw new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
    errors: { source: ['file upload or url is required'] },
  });
}

function resolveImportFormat(rawFormat: string | undefined, fileName: string) {
  const inferred = inferImportFormatFromFileName(fileName);

  if (rawFormat) {
    const explicit = parseFormat(rawFormat);

    if (inferred && explicit !== inferred) {
      throw new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
        errors: {
          format: [
            `format ${explicit} does not match file extension (${path.extname(fileName).toLowerCase()})`,
          ],
        },
      });
    }

    return explicit;
  }

  return inferred ?? 'ndjson';
}

function inferImportFormatFromFileName(fileName: string): FileFormat | null {
  const ext = path.extname(fileName).toLowerCase();

  if (ext === '.json') {
    return 'json';
  }

  if (ext === '.ndjson' || ext === '.jsonl') {
    return 'ndjson';
  }

  return null;
}

function sanitizeImportErrorSummary(
  value: Prisma.JsonValue | null | undefined,
): Record<string, unknown> | null {
  const summary = toJsonObject(value);
  if (!summary) {
    return null;
  }

  // Exclude reportLocation from the summary returned by the API to avoid exposing internal storage details.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { reportLocation, ...safeSummary } = summary;
  return safeSummary;
}

function buildImportErrorReportDownloadUrl(jobId: string): string {
  const baseUrl = process.env.IMPORT_ERROR_REPORT_DOWNLOAD_BASE_URL?.replace(/\/$/, '');
  const pathSuffix = `/api/v1/imports/${jobId}/errors/download`;
  return baseUrl ? `${baseUrl}${pathSuffix}` : pathSuffix;
}

async function cleanupImportIntake(intake: ImportIntakeResult): Promise<void> {
  if (!intake.location) {
    return;
  }

  try {
    await fs.rm(intake.location, { force: true });
  } catch (error) {
    logger.debug({
      event: 'Import intake cleanup skipped',
      intakePath: intake.location,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

async function markImportJobEnqueueFailed(prisma: PrismaClient, jobId: string): Promise<void> {
  try {
    await prisma.importJob.update({
      where: { id: jobId },
      data: {
        status: 'failed',
        finishedAt: new Date(),
      },
    });
  } catch (error) {
    logger.warn({
      event: 'Import enqueue failure status update failed',
      jobId,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
    });
  }
}

function mapImportCreateError(error: unknown): HttpException {
  if (error instanceof HttpException) {
    return error;
  }

  if (error instanceof ImportExportError) {
    switch (error.code) {
      case FileErrorCode.FILE_TOO_LARGE:
        return new HttpException(HttpStatusCode.PAYLOAD_TOO_LARGE, {
          errors: { file: [error.message] },
        });
      case FileErrorCode.UNSUPPORTED_FORMAT:
      case FileErrorCode.EMPTY_FILE:
        return new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
          errors: { file: [error.message] },
        });
      case FileErrorCode.URL_NOT_ALLOWED:
        return new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
          errors: { url: [error.message] },
        });
      case FileErrorCode.URL_FETCH_FAILED:
      case FileErrorCode.FILE_READ_ERROR:
      case FileErrorCode.FILE_WRITE_ERROR:
        return new HttpException(HttpStatusCode.SERVICE_UNAVAILABLE, {
          errors: { source: [error.message] },
        });
      default:
        return new HttpException(HttpStatusCode.INTERNAL_SERVER_ERROR, {
          errors: { import: ['import intake failed'] },
        });
    }
  }

  return new HttpException(HttpStatusCode.INTERNAL_SERVER_ERROR, {
    errors: { import: ['import intake failed'] },
  });
}
