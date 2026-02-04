import {
  ArticleImportRecord,
  CommentImportRecord,
  CreateRecordErrorOptions,
  EntityType,
  ImportRecord,
  UserImportRecord,
  ValidationErrorCode,
} from './types';
import type { PrismaClient } from '@prisma/client';
import {
  buildArticleSchema,
  buildCommentSchema,
  buildUserSchema,
  ZodIssueCode
} from './validation.schemas';
import {
  addError,
  createValidationCache,
  validateArticleExists,
  validateArticleSlugUniqueness,
  validateUserEmailUniqueness,
  validateUserExists,
  ValidationCache,
} from './validation.validators';
import { z } from 'zod';

export interface RecordValidationContext {
  jobId: string;
  recordIndex: number;
  prisma?: PrismaClient;
  cache?: ValidationCache;
  allowedUserRoles?: Set<string>;
  allowedArticleStatuses?: Set<string>;
}

export interface RecordValidationResult<TRecord> {
  valid: boolean;
  skip: boolean;
  errors: CreateRecordErrorOptions[];
  record?: TRecord;
}

export async function validateImportRecord(
  record: unknown,
  entityType: EntityType,
  context: RecordValidationContext,
): Promise<RecordValidationResult<ImportRecord>> {
  switch (entityType) {
    case 'users':
      return validateUserRecord(record as UserImportRecord, context);
    case 'articles':
      return validateArticleRecord(record as ArticleImportRecord, context);
    case 'comments':
      return validateCommentRecord(record as CommentImportRecord, context);
    default:
      return invalidRecordResult(context, 'Unsupported entity type');
  }
}

export async function validateUserRecord(
  record: UserImportRecord,
  context: RecordValidationContext,
): Promise<RecordValidationResult<UserImportRecord>> {
  if (!isPlainObject(record)) {
    return invalidRecordResult(context, 'Record must be an object');
  }

  const schema = buildUserSchema(context.allowedUserRoles);
  const parsed = schema.safeParse(record);
  if (!parsed.success) {
    return finalizeResult(buildErrorsFromIssues(parsed.error.issues, context, record));
  }

  const normalized = parsed.data as UserImportRecord;
  const cache = context.cache ?? createValidationCache();
  const errors: CreateRecordErrorOptions[] = [];
  if (normalized.email) {
    await validateUserEmailUniqueness(normalized.email, normalized.id, errors, context, cache);
  }

  return finalizeResult(errors, normalized);
}

export async function validateArticleRecord(
  record: ArticleImportRecord,
  context: RecordValidationContext,
): Promise<RecordValidationResult<ArticleImportRecord>> {
  if (!isPlainObject(record)) {
    return invalidRecordResult(context, 'Record must be an object');
  }

  const schema = buildArticleSchema(context.allowedArticleStatuses);
  const parsed = schema.safeParse(record);
  if (!parsed.success) {
    return finalizeResult(buildErrorsFromIssues(parsed.error.issues, context, record));
  }

  const normalized = parsed.data as ArticleImportRecord;
  const cache = context.cache ?? createValidationCache();
  const errors: CreateRecordErrorOptions[] = [];

  await validateUserExists(normalized.author_id, errors, context, cache, 'author_id');
  if (normalized.slug) {
    await validateArticleSlugUniqueness(normalized.slug, normalized.id, errors, context, cache);
  }

  return finalizeResult(errors, normalized);
}

export async function validateCommentRecord(
  record: CommentImportRecord,
  context: RecordValidationContext,
): Promise<RecordValidationResult<CommentImportRecord>> {
  if (!isPlainObject(record)) {
    return invalidRecordResult(context, 'Record must be an object');
  }

  const schema = buildCommentSchema();
  const parsed = schema.safeParse(record);
  if (!parsed.success) {
    return finalizeResult(buildErrorsFromIssues(parsed.error.issues, context, record));
  }

  const normalized = parsed.data as CommentImportRecord;
  const cache = context.cache ?? createValidationCache();
  const errors: CreateRecordErrorOptions[] = [];

  await validateArticleExists(normalized.article_id, errors, context, cache, 'article_id');
  await validateUserExists(normalized.user_id, errors, context, cache, 'user_id');

  return finalizeResult(errors, normalized);
}

function invalidRecordResult<TRecord>(
  context: RecordValidationContext,
  message: string,
): RecordValidationResult<TRecord> {
  const errors: CreateRecordErrorOptions[] = [];
  addError(errors, context, ValidationErrorCode.INVALID_FIELD_TYPE, message, 'record', null);
  return finalizeResult(errors, undefined);
}

function finalizeResult<TRecord>(
  errors: CreateRecordErrorOptions[],
  record?: TRecord,
): RecordValidationResult<TRecord> {
  const valid = errors.length === 0;
  return {
    valid,
    skip: !valid,
    errors,
    record: valid ? record : undefined,
  };
}

function buildErrorsFromIssues(
  issues: z.core.$ZodIssue[],
  context: RecordValidationContext,
  record: Record<string, unknown>
): CreateRecordErrorOptions[] {
  return issues.map((issue) => {
    const field = issue.path.length ? String(issue.path[0]) : 'record';
    const value = getValueAtPath(record, issue.path);
    return {
      jobId: context.jobId,
      recordIndex: context.recordIndex,
      errorCode: mapZodIssueToErrorCode(issue, field, value),
      message: issue.message,
      field,
      value: sanitizeValue(value),
    };
  });
}

function mapZodIssueToErrorCode(
  issue: z.core.$ZodIssue,
  field: string,
  value: unknown
): ValidationErrorCode {
  if (issue.code === ZodIssueCode.custom) {
    if (field === 'role' || field === 'status') {
      return ValidationErrorCode.INVALID_ENUM_VALUE;
    }
    if (field === 'id') {
      return ValidationErrorCode.MISSING_REQUIRED_FIELD;
    }
    if (field === 'body' && issue.message.toLowerCase().includes('500 words')) {
      return ValidationErrorCode.FIELD_TOO_LONG;
    }
    return ValidationErrorCode.INVALID_FIELD_FORMAT;
  }

  switch (issue.code) {
    case ZodIssueCode.invalid_type:
      return value === undefined || value === null
        ? ValidationErrorCode.MISSING_REQUIRED_FIELD
        : ValidationErrorCode.INVALID_FIELD_TYPE;
    case ZodIssueCode.invalid_value:
      return ValidationErrorCode.INVALID_ENUM_VALUE;
    case ZodIssueCode.too_small:
      return ValidationErrorCode.FIELD_TOO_SHORT;
    case ZodIssueCode.too_big:
      return ValidationErrorCode.FIELD_TOO_LONG;
    case ZodIssueCode.invalid_format:
      return ValidationErrorCode.INVALID_FIELD_FORMAT;
    default:
      return ValidationErrorCode.INVALID_FIELD_FORMAT;
  }
}

function sanitizeValue(value: unknown): unknown {
  if (value === undefined) {
    return null;
  }

  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (value === null) {
    return null;
  }

  if (Array.isArray(value)) {
    return value.slice(0, 10);
  }

  if (typeof value === 'object') {
    return '[object]';
  }

  return String(value);
}

function getValueAtPath(record: Record<string, unknown>, path: PropertyKey[]): unknown {
  if (!path.length) {
    return record;
  }

  let current: unknown = record;

  for (const part of path) {
    if (typeof part === 'symbol') {
      return null;
    }

    if (current && typeof current === 'object') {
      current = current[part as keyof typeof current];
    } else {
      return null;
    }
  }
  return current;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
