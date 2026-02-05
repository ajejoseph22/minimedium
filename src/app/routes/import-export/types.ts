/**
 * Import/Export Feature Type Definitions
 *
 * This file defines all types for the bulk import/export feature,
 * aligned with existing MiniMedium API conventions.
 */

// =============================================================================
// Job Status Types
// =============================================================================

/**
 * Possible states for import/export jobs
 */
export type JobStatus = 'queued' | 'running' | 'partial' | 'succeeded' | 'failed' | 'cancelled';

/**
 * Entity types that can be imported/exported
 */
export type EntityType = 'users' | 'articles' | 'comments';

/**
 * Supported file formats
 */
export type FileFormat = 'ndjson' | 'json';

// =============================================================================
// Import Job Types
// =============================================================================

/**
 * Import job metadata and state
 */
export interface ImportJob {
  id: string;
  status: JobStatus;
  entityType: EntityType;
  format: FileFormat;

  // Progress tracking
  totalRecords: number | null; // null if unknown (streaming input)
  processedRecords: number;
  successCount: number;
  errorCount: number;

  // Timing
  createdAt: Date;
  startedAt: Date | null;
  completedAt: Date | null;

  // Context
  createdBy: number; // User ID
  idempotencyKey: string | null;

  // File info
  fileName: string | null;
  fileSize: number | null;
  sourceUrl: string | null;

  // Error reporting
  errorSummary?: ErrorReportSummary | null;
}

/**
 * Options for creating an import job
 */
export interface CreateImportJobOptions {
  entityType: EntityType;
  format: FileFormat;
  createdBy: number;
  idempotencyKey?: string;
  fileName?: string;
  fileSize?: number;
  sourceUrl?: string;
}

// =============================================================================
// Error Report Types
// =============================================================================

export type ErrorReportStatus = 'complete' | 'partial' | 'failed';

export interface ErrorReportSummary {
  reportStatus: ErrorReportStatus;
  persistedErrorCount: number;
  persistenceFailures: number;
  reportLocation?: string | null;
  reportFormat?: FileFormat | null;
  reportGenerationFailed?: boolean;
}

// =============================================================================
// Export Job Types
// =============================================================================

/**
 * Export job metadata and state
 */
export interface ExportJob {
  id: string;
  status: JobStatus;
  entityType: EntityType;
  format: FileFormat;

  // Progress tracking
  totalRecords: number | null;
  processedRecords: number;

  // Timing
  createdAt: Date;
  startedAt: Date | null;
  completedAt: Date | null;
  expiresAt: Date | null; // When download URL expires

  // Context
  createdBy: number;
  idempotencyKey: string | null;

  // Output
  outputPath: string | null;
  downloadUrl: string | null;
  fileSize: number | null;
}

/**
 * Options for creating an export job
 */
export interface CreateExportJobOptions {
  entityType: EntityType;
  format: FileFormat;
  createdBy: number;
  idempotencyKey?: string;
}

/**
 * Options for streaming export
 */
export interface StreamingExportOptions {
  entityType: EntityType;
  format: FileFormat;
  limit?: number;
  cursor?: string;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error category ranges
 */
export enum ErrorCategory {
  VALIDATION = 1000,
  FILE = 2000,
  PROCESSING = 3000,
  RESOURCE = 4000,
  SYSTEM = 5000,
}

/**
 * Validation error codes (1000-1999)
 */
export enum ValidationErrorCode {
  MISSING_REQUIRED_FIELD = 1001,
  INVALID_FIELD_TYPE = 1002,
  INVALID_FIELD_FORMAT = 1003,
  FIELD_TOO_LONG = 1004,
  FIELD_TOO_SHORT = 1005,
  INVALID_ENUM_VALUE = 1006,
  DUPLICATE_VALUE = 1007,
  INVALID_REFERENCE = 1008,
  CIRCULAR_REFERENCE = 1009,
}

/**
 * File error codes (2000-2999)
 */
export enum FileErrorCode {
  FILE_TOO_LARGE = 2001,
  UNSUPPORTED_FORMAT = 2002,
  FILE_READ_ERROR = 2003,
  FILE_WRITE_ERROR = 2004,
  URL_FETCH_FAILED = 2005,
  URL_NOT_ALLOWED = 2006,
  EMPTY_FILE = 2007,
  TOO_MANY_RECORDS = 2008,
}

/**
 * Processing error codes (3000-3999)
 */
export enum ProcessingErrorCode {
  PARSE_ERROR = 3001,
  INVALID_RECORD_STRUCTURE = 3002,
  BATCH_FAILED = 3003,
  STREAM_ERROR = 3004,
  ENCODING_ERROR = 3005,
}

/**
 * Resource error codes (4000-4999)
 */
export enum ResourceErrorCode {
  JOB_NOT_FOUND = 4001,
  UNAUTHORIZED = 4002,
  FORBIDDEN = 4003,
  RATE_LIMITED = 4004,
  CONCURRENT_LIMIT = 4005,
  DOWNLOAD_EXPIRED = 4006,
  UNSUPPORTED_RESOURCE = 4007,
}

/**
 * System error codes (5000-5999)
 */
export enum SystemErrorCode {
  DATABASE_ERROR = 5001,
  STORAGE_ERROR = 5002,
  QUEUE_ERROR = 5003,
  INTERNAL_ERROR = 5004,
  TIMEOUT = 5005,
}

/**
 * Union of all error codes
 */
export type ImportExportErrorCode =
  | ValidationErrorCode
  | FileErrorCode
  | ProcessingErrorCode
  | ResourceErrorCode
  | SystemErrorCode;

/**
 * Human-readable error names mapped to codes
 */
export const ErrorCodeNames: Record<ImportExportErrorCode, string> = {
  // Validation errors
  [ValidationErrorCode.MISSING_REQUIRED_FIELD]: 'MISSING_REQUIRED_FIELD',
  [ValidationErrorCode.INVALID_FIELD_TYPE]: 'INVALID_FIELD_TYPE',
  [ValidationErrorCode.INVALID_FIELD_FORMAT]: 'INVALID_FIELD_FORMAT',
  [ValidationErrorCode.FIELD_TOO_LONG]: 'FIELD_TOO_LONG',
  [ValidationErrorCode.FIELD_TOO_SHORT]: 'FIELD_TOO_SHORT',
  [ValidationErrorCode.INVALID_ENUM_VALUE]: 'INVALID_ENUM_VALUE',
  [ValidationErrorCode.DUPLICATE_VALUE]: 'DUPLICATE_VALUE',
  [ValidationErrorCode.INVALID_REFERENCE]: 'INVALID_REFERENCE',
  [ValidationErrorCode.CIRCULAR_REFERENCE]: 'CIRCULAR_REFERENCE',

  // File errors
  [FileErrorCode.FILE_TOO_LARGE]: 'FILE_TOO_LARGE',
  [FileErrorCode.UNSUPPORTED_FORMAT]: 'UNSUPPORTED_FORMAT',
  [FileErrorCode.FILE_READ_ERROR]: 'FILE_READ_ERROR',
  [FileErrorCode.FILE_WRITE_ERROR]: 'FILE_WRITE_ERROR',
  [FileErrorCode.URL_FETCH_FAILED]: 'URL_FETCH_FAILED',
  [FileErrorCode.URL_NOT_ALLOWED]: 'URL_NOT_ALLOWED',
  [FileErrorCode.EMPTY_FILE]: 'EMPTY_FILE',
  [FileErrorCode.TOO_MANY_RECORDS]: 'TOO_MANY_RECORDS',

  // Processing errors
  [ProcessingErrorCode.PARSE_ERROR]: 'PARSE_ERROR',
  [ProcessingErrorCode.INVALID_RECORD_STRUCTURE]: 'INVALID_RECORD_STRUCTURE',
  [ProcessingErrorCode.BATCH_FAILED]: 'BATCH_FAILED',
  [ProcessingErrorCode.STREAM_ERROR]: 'STREAM_ERROR',
  [ProcessingErrorCode.ENCODING_ERROR]: 'ENCODING_ERROR',

  // Resource errors
  [ResourceErrorCode.JOB_NOT_FOUND]: 'JOB_NOT_FOUND',
  [ResourceErrorCode.UNAUTHORIZED]: 'UNAUTHORIZED',
  [ResourceErrorCode.FORBIDDEN]: 'FORBIDDEN',
  [ResourceErrorCode.RATE_LIMITED]: 'RATE_LIMITED',
  [ResourceErrorCode.CONCURRENT_LIMIT]: 'CONCURRENT_LIMIT',
  [ResourceErrorCode.DOWNLOAD_EXPIRED]: 'DOWNLOAD_EXPIRED',
  [ResourceErrorCode.UNSUPPORTED_RESOURCE]: 'UNSUPPORTED_RESOURCE',

  // System errors
  [SystemErrorCode.DATABASE_ERROR]: 'DATABASE_ERROR',
  [SystemErrorCode.STORAGE_ERROR]: 'STORAGE_ERROR',
  [SystemErrorCode.QUEUE_ERROR]: 'QUEUE_ERROR',
  [SystemErrorCode.INTERNAL_ERROR]: 'INTERNAL_ERROR',
  [SystemErrorCode.TIMEOUT]: 'TIMEOUT',
};

/**
 * Per-record error for import operations
 */
export interface RecordError {
  id: string;
  jobId: string;
  recordIndex: number; // 0-based line/index in source
  errorCode: ImportExportErrorCode;
  errorName: string;
  message: string;
  field: string | null;
  value: unknown | null; // Problematic value (sanitized)
  createdAt: Date;
}

/**
 * Options for creating a record error
 */
export interface CreateRecordErrorOptions {
  jobId: string;
  recordIndex: number;
  errorCode: ImportExportErrorCode;
  message: string;
  field?: string;
  value?: unknown;
}

// =============================================================================
// API Response Types
// =============================================================================

/**
 * Standard error response format (aligned with existing API)
 */
export interface ErrorResponse {
  errors: {
    [field: string]: string[];
  };
}

/**
 * Import job response wrapper
 */
export interface ImportJobResponse {
  importJob: ImportJob;
  errors?: RecordError[];
  errorReportUrl?: string;
}

/**
 * Export job response wrapper
 */
export interface ExportJobResponse {
  exportJob: ExportJob;
}

/**
 * List of import jobs response
 */
export interface ImportJobsListResponse {
  importJobs: ImportJob[];
  importJobsCount: number;
}

/**
 * List of export jobs response
 */
export interface ExportJobsListResponse {
  exportJobs: ExportJob[];
  exportJobsCount: number;
}

/**
 * Record errors list response
 */
export interface RecordErrorsResponse {
  errors: RecordError[];
  errorsCount: number;
  limit: number;
  offset: number;
}

// =============================================================================
// Import Record Types (Input Shapes)
// =============================================================================

/**
 * User record for import
 */
export interface UserImportRecord {
  id?: number;
  email?: string;
  name: string;
  role?: string;
  active?: boolean;
  created_at?: string; // ISO date string
  updated_at?: string; // ISO date string
}

/**
 * Article record for import
 */
export interface ArticleImportRecord {
  id?: number;
  slug?: string;
  title: string;
  body: string;
  author_id: number;
  tags?: string[];
  published_at?: string; // ISO date string
  status?: string;
}

/**
 * Comment record for import
 */
export interface CommentImportRecord {
  id?: number;
  body: string;
  article_id: number;
  user_id: number;
  created_at?: string; // ISO date string
}

/**
 * Union of all import record types
 */
export type ImportRecord = UserImportRecord | ArticleImportRecord | CommentImportRecord;

// =============================================================================
// Export Record Types (Output Shapes)
// =============================================================================

/**
 * User record for export (excludes password)
 */
export interface UserExportRecord {
  id: number;
  email: string;
  name: string;
  role: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

/**
 * Article record for export
 */
export interface ArticleExportRecord {
  id: number;
  slug: string;
  title: string;
  body: string;
  author_id: number;
  tags: string[];
  published_at: string | null;
  status: string;
}

/**
 * Comment record for export
 */
export interface CommentExportRecord {
  id: number;
  article_id: number;
  user_id: number;
  body: string;
  created_at: string;
}

/**
 * Union of all export record types
 */
export type ExportRecord = UserExportRecord | ArticleExportRecord | CommentExportRecord;

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Import/export feature configuration
 */
export interface ImportExportConfig {
  // File handling
  maxFileSize: number; // bytes
  maxRecords: number;
  batchSize: number;
  allowedHosts: string[];

  // Export
  fileRetentionHours: number;
  exportStoragePath: string;
  errorReportStoragePath: string;
  importStoragePath: string;
  exportStreamMaxLimit: number;

  // Rate limits
  importRateLimitPerHour: number;
  exportRateLimitPerHour: number;

  // Concurrency
  importConcurrentLimitUser: number;
  importConcurrentLimitGlobal: number;
  exportConcurrentLimitUser: number;
  exportConcurrentLimitGlobal: number;

  // Worker
  workerConcurrency: number;
}
