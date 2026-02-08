import { once } from 'events';
import { PassThrough } from 'stream';
import type { Prisma, PrismaClient } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import { createExportStorageAdapter } from '../../storage';
import { loadExportConfig } from './config';
import {
  EntityType,
  ExportRecord,
  FileFormat,
  ImportExportErrorCode,
  JobStatus,
  ResourceErrorCode,
  SystemErrorCode,
} from '../shared/import-export/types';
import {
  logJobLifecycleEvent,
} from '../../jobs/observability';
import {
  getQueryParamValue,
  isObject,
  normalizeFormat as normalizeDownloadFormat,
  normalizeJsonValue,
  parseCursor,
  parseEntityType,
  parseFormat,
  parseLimit,
} from '../shared/import-export/utils';
import type {
  ArticleRow,
  CommentRow,
  CreateExportJobOptions,
  CreateExportJobResult,
  ExportCreatePayload,
  ExportFileMetadata,
  ExportQuery,
  GetExportFileMetadataOptions,
  GetExportJobOptions,
  RunExportJobOptions,
  RunExportJobResult,
  StreamExportOptions,
  StreamExportsOptions,
  StreamExportsResult,
  UserRow,
} from './export.model';
import HttpException from '../../models/http-exception.model';

const DEFAULT_CANCEL_CHECK_INTERVAL = 500;

class ExportServiceError extends Error {
  constructor(
    public code: ImportExportErrorCode,
    message: string,
    public details?: Prisma.InputJsonValue
  ) {
    super(message);
    this.name = 'ExportServiceError';
  }
}

export async function* streamExportRecords(options: StreamExportOptions): AsyncGenerator<ExportRecord> {
  const prisma = options.prisma ?? prismaClient;
  const config = loadExportConfig();
  const limit = options.limit ?? config.exportStreamMaxLimit;
  const batchSize = options.batchSize ?? config.batchSize;

  let remaining = limit;
  let cursor = options.cursor ?? null;

  while (remaining > 0) {
    if (options.signal?.aborted) {
      return;
    }

    const take = Math.min(batchSize, remaining);
    const batch = await fetchExportBatch(prisma, options.entityType, take, cursor);

    if (!batch.length) {
      return;
    }

    for (const record of batch) {
      if (options.signal?.aborted) {
        return;
      }

      remaining -= 1;
      cursor = record.id;
      yield mapExportRecord(options.entityType, record);

      if (remaining <= 0) {
        return;
      }
    }
  }
}

export async function runExportJob(
  jobId: string,
  options: RunExportJobOptions = {}
): Promise<RunExportJobResult> {
  const prisma = options.prisma ?? prismaClient;
  const storage = options.storage ?? createExportStorageAdapter();
  const now = options.now ?? (() => new Date());
  const cancelCheckInterval =
    options.cancelCheckInterval ?? DEFAULT_CANCEL_CHECK_INTERVAL;
  const config = loadExportConfig();

  const startedAt = now();
  const claimResult = await prisma.exportJob.updateMany({
    where: {
      id: jobId,
      status: 'queued',
    },
    data: {
      status: 'running',
      startedAt,
    },
  });

  const job = await prisma.exportJob.findUnique({ where: { id: jobId } });
  if (!job) {
    throw new ExportServiceError(
      ResourceErrorCode.JOB_NOT_FOUND,
      `Export job ${jobId} not found`
    );
  }

  // Job claimed by another worker process, return current status without processing
  if (!claimResult.count) {
    return {
      status: job.status,
      processedRecords: job.processedRecords,
      fileSize: job.fileSize ?? null,
    };
  }

  // Job claimed successfully by this worker process, proceeed with processing.
  // Pre-run cancellation check
  if (job.status === 'cancelled') {
    const finishedAt = now();
    await markJobCancelled(prisma, jobId, finishedAt);
    logJobLifecycleEvent({
      event: 'job.completed',
      jobKind: 'export',
      jobId,
      status: 'cancelled',
      resource: job.resource,
      format: job.format,
      timestamp: finishedAt,
      jobStartedAt: job.startedAt ?? finishedAt, // fallback so metrics compute duration as 0ms
      counters: {
        processedRecords: job.processedRecords,
        errorCount: 0,
      },
    });

    return {
      status: 'cancelled',
      processedRecords: job.processedRecords,
      fileSize: job.fileSize ?? null,
    };
  }

  logJobLifecycleEvent({
    event: 'job.started',
    jobKind: 'export',
    jobId,
    status: 'running',
    resource: job.resource,
    format: job.format,
    timestamp: job.startedAt ?? startedAt,
    counters: {
      processedRecords: job.processedRecords,
      errorCount: 0,
    },
  });

  const entityType = normalizeEntityType(job.resource);
  const format = normalizeExportJobFormat(job.format);
  const outputKey = job.outputLocation ?? buildOutputKey(jobId, format);
  const outputStream = new PassThrough();
  const savePromise = storage.saveStream(outputKey, outputStream);

  let processedRecords = 0;
  let cancelled = false;

  const writeChunk = async (chunk: string): Promise<void> => {
    if (!outputStream.write(chunk)) {
      await once(outputStream, 'drain');
    }
  };

  try {
    let first = true;

    if (format === 'json') {
      await writeChunk('[');
    }

    for await (const record of streamExportRecords({
      entityType,
      prisma,
      batchSize: config.batchSize,
      limit: Number.MAX_SAFE_INTEGER,
    })) {
      const payload = JSON.stringify(record);
      if (format === 'json') {
        await writeChunk(first ? payload : `,${payload}`);
      } else {
        await writeChunk(`${payload}\n`);
      }
      processedRecords += 1;
      first = false;

      if (cancelCheckInterval && processedRecords % cancelCheckInterval === 0) {
        if (await isJobCancelled(prisma, jobId)) {
          cancelled = true;
          break;
        }
      }
    }

    if (format === 'json') {
      await writeChunk(']');
    }

    outputStream.end();
    const saved = await savePromise;

    if (cancelled) {
      await storage.delete(outputKey);
      const finishedAt = now();
      await finalizeJob(prisma, jobId, {
        status: 'cancelled',
        processedRecords,
        totalRecords: processedRecords,
        finishedAt,
      });

      logJobLifecycleEvent({
        event: 'job.completed',
        jobKind: 'export',
        jobId,
        status: 'cancelled',
        resource: job.resource,
        format: job.format,
        timestamp: finishedAt,
        jobStartedAt: startedAt,
        counters: {
          processedRecords,
          errorCount: 0,
        },
      });
      return { status: 'cancelled', processedRecords, fileSize: null };
    }

    const finishedAt = now();
    await finalizeJob(prisma, jobId, {
      status: 'succeeded',
      processedRecords,
      totalRecords: processedRecords,
      finishedAt,
      outputLocation: saved.location,
      downloadUrl: buildDownloadUrl(jobId),
      fileSize: saved.bytes,
      expiresAt: buildExpiry(now, config.fileRetentionHours),
    });
    logJobLifecycleEvent({
      event: 'job.completed',
      jobKind: 'export',
      jobId,
      status: 'succeeded',
      resource: job.resource,
      format: job.format,
      timestamp: finishedAt,
      jobStartedAt: startedAt,
      counters: {
        processedRecords,
        errorCount: 0,
      },
    });

    return { status: 'succeeded', processedRecords, fileSize: saved.bytes };
  } catch (error) {
    outputStream.destroy();
    try {
      await storage.delete(outputKey);
    } catch {
      // ignore cleanup errors
    }

    const finishedAt = now();
    await finalizeJob(prisma, jobId, {
      status: 'failed',
      processedRecords,
      totalRecords: processedRecords,
      finishedAt,
    });

    const { code, message, details } = normalizeError(error);
    const failedErrorCount = 1;
    logJobLifecycleEvent({
      event: 'job.completed',
      jobKind: 'export',
      jobId,
      status: 'failed',
      resource: job.resource,
      format: job.format,
      timestamp: finishedAt,
      jobStartedAt: startedAt,
      counters: {
        processedRecords,
        errorCount: failedErrorCount,
      },
      level: 'error',
      details: {
        errorCode: code,
        message,
        details,
      },
    });
    throw new ExportServiceError(code, message, details);
  }
}

function normalizeEntityType(resource: string): EntityType {
  if (
    resource === 'users' ||
    resource === 'articles' ||
    resource === 'comments'
  ) {
    return resource;
  }

  throw new ExportServiceError(
    ResourceErrorCode.UNSUPPORTED_RESOURCE,
    `Unsupported resource ${resource}`
  );
}

function normalizeExportJobFormat(format: string | null): FileFormat {
  if (format === 'ndjson' || format === 'json') {
    return format;
  }
  throw new ExportServiceError(
    SystemErrorCode.INTERNAL_ERROR,
    'Export format missing'
  );
}

function buildOutputKey(jobId: string, format: FileFormat): string {
  const ext = format === 'json' ? 'json' : 'ndjson';
  return `${jobId}.${ext}`;
}

function buildDownloadUrl(jobId: string): string {
  const baseUrl = process.env.EXPORT_DOWNLOAD_BASE_URL?.replace(/\/$/, '');
  const pathSuffix = `/api/v1/exports/${jobId}/download`;
  return baseUrl ? `${baseUrl}${pathSuffix}` : pathSuffix;
}

function buildExpiry(now: () => Date, retentionHours: number): Date | null {
  if (!Number.isFinite(retentionHours) || retentionHours <= 0) {
    return null;
  }
  return new Date(now().getTime() + retentionHours * 60 * 60 * 1000);
}

async function isJobCancelled(
  prisma: PrismaClient,
  jobId: string
): Promise<boolean> {
  const status = await prisma.exportJob.findUnique({
    where: { id: jobId },
    select: { status: true },
  });
  return status?.status === 'cancelled';
}

async function markJobCancelled(
  prisma: PrismaClient,
  jobId: string,
  timestamp: Date
): Promise<void> {
  await prisma.exportJob.update({
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
    totalRecords: number | null;
    finishedAt: Date;
    outputLocation?: string;
    downloadUrl?: string;
    fileSize?: number;
    expiresAt?: Date | null;
  }
): Promise<void> {
  await prisma.exportJob.update({
    where: { id: jobId },
    data: {
      status: update.status,
      processedRecords: update.processedRecords,
      totalRecords: update.totalRecords,
      finishedAt: update.finishedAt,
      outputLocation: update.outputLocation,
      downloadUrl: update.downloadUrl,
      fileSize: update.fileSize,
      expiresAt: update.expiresAt,
    },
  });
}

function normalizeError(error: unknown): {
  code: ImportExportErrorCode;
  message: string;
  details?: Prisma.InputJsonValue;
} {
  if (error instanceof ExportServiceError) {
    return { code: error.code, message: error.message, details: error.details };
  }

  if (error instanceof Error) {
    return {
      code: SystemErrorCode.INTERNAL_ERROR,
      message: error.message,
      details: {
        name: error.name,
        stack: error.stack,
      } as Prisma.InputJsonValue,
    };
  }

  return {
    code: SystemErrorCode.INTERNAL_ERROR,
    message: 'Unknown error while processing export',
    details: { error: String(error) } as Prisma.InputJsonValue,
  };
}


async function fetchExportBatch(
  prisma: PrismaClient,
  entityType: EntityType,
  take: number,
  cursor: number | null,
): Promise<UserRow[] | ArticleRow[] | CommentRow[]> {
  const where = cursor ? { id: { gt: cursor } } : undefined;

  switch (entityType) {
    case 'users':
      return prisma.user.findMany({
        where,
        orderBy: { id: 'asc' },
        take,
        select: {
          id: true,
          email: true,
          name: true,
          username: true,
          role: true,
          active: true,
          createdAt: true,
          updatedAt: true,
        },
      });
    case 'articles':
      return prisma.article.findMany({
        where,
        orderBy: { id: 'asc' },
        take,
        select: {
          id: true,
          slug: true,
          title: true,
          body: true,
          authorId: true,
          publishedAt: true,
          status: true,
          tagList: {
            select: { name: true },
            orderBy: { name: 'asc' },
          },
        },
      });
    case 'comments':
      return prisma.comment.findMany({
        where,
        orderBy: { id: 'asc' },
        take,
        select: {
          id: true,
          articleId: true,
          userId: true,
          body: true,
          createdAt: true,
        },
      });
    default:
      return [];
  }
}

function mapExportRecord(entityType: EntityType, record: UserRow | ArticleRow | CommentRow): ExportRecord {
  switch (entityType) {
    case 'users':
      return mapUserExport(record as UserRow);
    case 'articles':
      return mapArticleExport(record as ArticleRow);
    case 'comments':
      return mapCommentExport(record as CommentRow);
    default:
      throw new Error(`Unsupported entity type ${entityType}`);
  }
}

function mapUserExport(user: UserRow): ExportRecord {
  return {
    id: user.id,
    email: user.email,
    name: user.name ?? user.username,
    role: user.role,
    active: user.active,
    created_at: user.createdAt.toISOString(),
    updated_at: user.updatedAt.toISOString(),
  };
}

function mapArticleExport(article: ArticleRow): ExportRecord {
  return {
    id: article.id,
    slug: article.slug,
    title: article.title,
    body: article.body,
    author_id: article.authorId,
    tags: article.tagList.map((tag) => tag.name),
    published_at: article.publishedAt ? article.publishedAt.toISOString() : null,
    status: article.status,
  };
}

function mapCommentExport(comment: CommentRow): ExportRecord {
  return {
    id: comment.id,
    article_id: comment.articleId,
    user_id: comment.userId,
    body: comment.body,
    created_at: comment.createdAt.toISOString(),
  };
}

export function getExportPayload(body: unknown): ExportCreatePayload {
  const candidate = isObject(body) && 'export' in body ? (body as Record<string, unknown>).export : body;
  if (!isObject(candidate)) {
    return {};
  }
  return candidate as ExportCreatePayload;
}

export function parseExportQuery(query: Record<string, unknown>): ExportQuery {
  const config = loadExportConfig();
  const resource = getQueryParamValue(query.resource);
  const formatValue = getQueryParamValue(query.format);
  const limitValue = getQueryParamValue(query.limit);
  const cursorValue = getQueryParamValue(query.cursor);

  const entityType = parseEntityType(resource);
  const format = parseFormat(formatValue);
  const limit = parseLimit(limitValue, config.exportStreamMaxLimit);
  const cursor = parseCursor(cursorValue);

  return { entityType, format, limit, cursor };
}

export async function createExportJob(options: CreateExportJobOptions): Promise<CreateExportJobResult> {
  const prisma = options.prisma ?? prismaClient;
  const resource = parseEntityType(options.payload.resource);
  const format = parseFormat(options.payload.format);
  const idempotencyKey = options.idempotencyKey ?? null;

  if (idempotencyKey) {
    const existing = await prisma.exportJob.findFirst({
      where: { createdById: options.createdById, idempotencyKey, resource },
    });

    if (existing) {
      return { statusCode: 200, exportJob: serializeExportJob(existing) };
    }
  }

  const normalizedFilters = normalizeJsonValue(options.payload.filters);
  const normalizedFields = normalizeJsonValue(options.payload.fields);

  const created = await prisma.exportJob.create({
    data: {
      status: 'queued',
      resource,
      format,
      ...(normalizedFilters !== null ? { filters: normalizedFilters } : {}),
      ...(normalizedFields !== null ? { fields: normalizedFields } : {}),
      idempotencyKey,
      createdById: options.createdById,
      requestHash: null,
    },
  });

  const { enqueueExportJob } = await import('../../jobs/import-export.queue');
  await enqueueExportJob({
    jobId: created.id,
    resource: created.resource,
    format: created.format,
  });

  return { statusCode: 202, exportJob: serializeExportJob(created) };
}

export async function getExportJob(options: GetExportJobOptions) {
  const prisma = options.prisma ?? prismaClient;
  const job = await prisma.exportJob.findFirst({
    where: { id: options.jobId, createdById: options.createdById },
  });

  if (!job) {
    throw new HttpException(404, { errors: { job: ['export job not found'] } });
  }

  return { exportJob: serializeExportJob(job) };
}

export async function getExportFileMetadata(
  options: GetExportFileMetadataOptions,
): Promise<ExportFileMetadata> {
  const prisma = options.prisma ?? prismaClient;
  const now = options.now ?? Date.now;
  const job = await prisma.exportJob.findFirst({
    where: { id: options.jobId, createdById: options.createdById },
  });

  if (!job) {
    throw new HttpException(404, { errors: { job: ['export job not found'] } });
  }

  if (job.status !== 'succeeded' || !job.outputLocation) {
    throw new HttpException(409, { errors: { job: ['export is not ready for download'] } });
  }

  if (job.expiresAt && job.expiresAt.getTime() < now()) {
    throw new HttpException(410, { errors: { job: ['download URL has expired'] } });
  }

  const format = normalizeDownloadFormat(job.format);

  return {
    outputLocation: job.outputLocation,
    contentType: format === 'json' ? 'application/json' : 'application/x-ndjson',
    contentDisposition: `attachment; filename="${job.id}.${format}"`,
  };
}

export async function streamExports(options: StreamExportsOptions): Promise<StreamExportsResult> {
  const { entityType, format, limit, cursor, signal, writeChunk, onRecord } = options;
  const streamRecords = options.streamRecords ?? streamExportRecords;
  let count = 0;
  let lastId: number | null = null;
  let first = true;

  if (format === 'json') {
    await writeChunk('{"data":[');
  }

  for await (const record of streamRecords({ entityType, limit, cursor, signal })) {
    const payload = JSON.stringify(record);

    if (format === 'json') {
      await writeChunk(first ? payload : `,${payload}`);
    } else {
      await writeChunk(`${payload}\n`);
    }

    count += 1;
    lastId = record.id;
    onRecord?.({ count, lastId });
    first = false;
  }

  const nextCursor = count === limit ? lastId : null;

  if (format === 'json') {
    await writeChunk(`],"nextCursor":${nextCursor ?? 'null'}}`);
  } else {
    await writeChunk(`${JSON.stringify({ _type: 'cursor', nextCursor })}\n`);
  }

  return { count, lastId };
}

export function buildExportStreamClosingChunk(
  format: FileFormat,
  count: number,
  limit: number,
  lastId: number | null,
): string {
  const nextCursor = count === limit ? lastId : null;

  if (format === 'json') {
    return `],"nextCursor":${nextCursor ?? 'null'}}`;
  }

  return `${JSON.stringify({ _type: 'cursor', nextCursor })}\n`;
}

export function serializeExportJob(job: {
  id: string;
  status: string;
  resource: string;
  format: string;
  totalRecords: number | null;
  processedRecords: number;
  createdAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  expiresAt: Date | null;
  idempotencyKey: string | null;
  outputLocation: string | null;
  downloadUrl: string | null;
  fileSize: number | null;
}) {
  return {
    id: job.id,
    status: job.status,
    entityType: job.resource,
    format: job.format,
    totalRecords: job.totalRecords,
    processedRecords: job.processedRecords,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    completedAt: job.finishedAt,
    expiresAt: job.expiresAt,
    createdBy: undefined,
    idempotencyKey: job.idempotencyKey,
    outputPath: job.outputLocation,
    downloadUrl: job.downloadUrl,
    fileSize: job.fileSize,
  };
}
