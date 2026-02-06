import { once } from 'events';
import path from 'path';
import { PassThrough } from 'stream';
import type { Prisma, PrismaClient } from '@prisma/client';
import { createExportStorageAdapter, StorageAdapter } from '../../storage';
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
import prismaClient from '../../../prisma/prisma-client';


export interface StreamExportOptions {
  entityType: EntityType;
  limit?: number;
  cursor?: number | null;
  prisma?: PrismaClient;
  batchSize?: number;
  signal?: AbortSignal;
}

interface UserRow {
  id: number;
  email: string;
  name: string | null;
  username: string;
  role: string;
  active: boolean;
  createdAt: Date;
  updatedAt: Date;
}

interface ArticleRow {
  id: number;
  slug: string;
  title: string;
  body: string;
  authorId: number;
  publishedAt: Date | null;
  status: string;
  tagList: { name: string }[];
}

interface CommentRow {
  id: number;
  articleId: number;
  userId: number;
  body: string;
  createdAt: Date;
}

export interface RunExportJobOptions {
  prisma?: PrismaClient;
  storage?: StorageAdapter;
  now?: () => Date;
  cancelCheckInterval?: number;
}

export interface RunExportJobResult {
  status: JobStatus;
  processedRecords: number;
  fileSize: number | null;
}

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

  const job = await prisma.exportJob.findUnique({ where: { id: jobId } });
  if (!job) {
    throw new ExportServiceError(
      ResourceErrorCode.JOB_NOT_FOUND,
      `Export job ${jobId} not found`
    );
  }

  if (job.status === 'cancelled') {
    await markJobCancelled(prisma, jobId, now());
    return {
      status: 'cancelled',
      processedRecords: job.processedRecords,
      fileSize: job.fileSize ?? null,
    };
  }

  await prisma.exportJob.update({
    where: { id: jobId },
    data: {
      status: 'running',
      startedAt: job.startedAt ?? now(),
    },
  });

  const entityType = normalizeEntityType(job.resource);
  const format = normalizeFormat(job.format);
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
      await finalizeJob(prisma, jobId, {
        status: 'cancelled',
        processedRecords,
        totalRecords: processedRecords,
        finishedAt: now(),
      });
      return { status: 'cancelled', processedRecords, fileSize: null };
    }

    await finalizeJob(prisma, jobId, {
      status: 'succeeded',
      processedRecords,
      totalRecords: processedRecords,
      finishedAt: now(),
      outputLocation: saved.location,
      downloadUrl: buildDownloadUrl(jobId),
      fileSize: saved.bytes,
      expiresAt: buildExpiry(now, config.fileRetentionHours),
    });

    return { status: 'succeeded', processedRecords, fileSize: saved.bytes };
  } catch (error) {
    outputStream.destroy();
    try {
      await storage.delete(outputKey);
    } catch {
      // ignore cleanup errors
    }

    await finalizeJob(prisma, jobId, {
      status: 'failed',
      processedRecords,
      totalRecords: processedRecords,
      finishedAt: now(),
    });

    const { code, message, details } = normalizeError(error);
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

function normalizeFormat(format: string | null): FileFormat {
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
  return path.posix.join('exports', `${jobId}.${ext}`);
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
