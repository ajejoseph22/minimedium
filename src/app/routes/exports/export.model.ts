import type { Prisma, PrismaClient } from '@prisma/client';
import type { StorageAdapter } from '../../storage';
import type { EntityType, ExportRecord, FileFormat, JobStatus } from '../shared/import-export/types';

export interface StreamExportOptions {
  entityType: EntityType;
  limit?: number;
  cursor?: number | null;
  prisma?: PrismaClient;
  batchSize?: number;
  signal?: AbortSignal;
}

export interface UserRow {
  id: number;
  email: string;
  name: string | null;
  username: string;
  role: string;
  active: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface ArticleRow {
  id: number;
  slug: string;
  title: string;
  body: string;
  authorId: number;
  publishedAt: Date | null;
  status: string;
  tagList: { name: string }[];
}

export interface CommentRow {
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

export interface ExportCreatePayload {
  resource?: string;
  format?: string;
  filters?: Prisma.InputJsonValue;
  fields?: Prisma.InputJsonValue;
}

export interface CreateExportJobOptions {
  createdById: number;
  payload: ExportCreatePayload;
  idempotencyKey?: string | null;
  prisma?: PrismaClient;
}

export interface GetExportJobOptions {
  jobId: string;
  createdById: number;
  prisma?: PrismaClient;
}

export interface GetExportFileMetadataOptions {
  jobId: string;
  createdById: number;
  now?: () => number;
  prisma?: PrismaClient;
}

export interface StreamExportsOptions {
  entityType: EntityType;
  format: FileFormat;
  limit: number;
  cursor: number | null;
  signal?: AbortSignal;
  writeChunk: (chunk: string) => Promise<void>;
  onRecord?: (progress: { count: number; lastId: number }) => void;
  streamRecords?: (options: {
    entityType: EntityType;
    limit: number;
    cursor: number | null;
    signal?: AbortSignal;
  }) => AsyncGenerator<ExportRecord>;
}

export interface StreamExportsResult {
  count: number;
  lastId: number | null;
}

export interface ExportQuery {
  entityType: EntityType;
  format: FileFormat;
  limit: number;
  cursor: number | null;
}

export interface ExportFileMetadata {
  outputLocation: string;
  contentType: string;
  contentDisposition: string;
}

export interface CreateExportJobResult {
  statusCode: 200 | 202;
  exportJob: Record<string, unknown>;
}
