import type { Prisma, PrismaClient } from '@prisma/client';
import type { UploadedFile } from './intake.service';
import type { CreateRecordErrorOptions, JobStatus } from '../shared/import-export/types';

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

export interface RecordErrorPayload {
  error: CreateRecordErrorOptions;
  recordId: string | null;
  details?: Prisma.InputJsonValue | null;
}

export interface ImportCreatePayload {
  resource?: string;
  format?: string;
  url?: string;
}

export interface ImportIntakeResult {
  location: string;
  bytes: number;
  fileName: string;
  sourceType: 'upload' | 'url';
}

export interface CreateImportJobOptions {
  createdById: number;
  payload: ImportCreatePayload;
  file?: UploadedFile;
  idempotencyKey: string;
  prisma?: PrismaClient;
}

export interface GetImportJobStatusOptions {
  jobId: string;
  createdById: number;
  prisma?: PrismaClient;
}

export interface GetErrorReportFileOptions {
  jobId: string;
  createdById: number;
  prisma?: PrismaClient;
}

export interface ErrorReportFileMetadata {
  reportLocation: string;
  contentType: string;
  contentDisposition: string;
}

export interface CreateImportJobResult {
  statusCode: 200 | 202;
  importJob: Record<string, unknown>;
}
