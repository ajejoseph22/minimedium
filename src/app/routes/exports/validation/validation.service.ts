import type { Prisma } from '@prisma/client';
import { z } from 'zod';
import HttpException from '../../../models/http-exception.model';
import { HttpStatusCode } from '../../../models/http-status-code.model';
import type { EntityType, ExportRecord } from '../../shared/import-export/types';
import { getFieldsSchema, getFiltersSchema } from './validation.schemas';

type ValidationTarget = 'filters' | 'fields';

interface NormalizedValidationValue {
  filters: Record<string, unknown> | null;
  fields: Set<string> | null;
}

export interface NormalizedExportCreatePayload {
  filters: Prisma.InputJsonValue | null;
  fields: Prisma.InputJsonValue | null;
}

export class ExportValidationError extends Error {
  constructor(
    public target: ValidationTarget,
    message: string,
  ) {
    super(message);
    this.name = 'ExportValidationError';
  }
}

export function normalizeExportCreatePayload(
  entityType: EntityType,
  rawFilters: unknown,
  rawFields: unknown,
): NormalizedExportCreatePayload {
  const normalized = resolveExportRequestValidation(entityType, rawFilters, rawFields);
  return {
    filters: normalized.filters as Prisma.InputJsonValue | null,
    fields: normalized.fields ? Array.from(normalized.fields) as Prisma.InputJsonValue : null,
  };
}

export function resolveExportRequestValidation(
  entityType: EntityType,
  rawFilters: unknown,
  rawFields: unknown,
): NormalizedValidationValue {
  return normalizeExportValidationValue(entityType, rawFilters, rawFields, 'request');
}

export function resolveExportJobValidation(
  entityType: EntityType,
  rawFilters: Prisma.JsonValue | null | undefined,
  rawFields: Prisma.JsonValue | null | undefined,
): NormalizedValidationValue {
  return normalizeExportValidationValue(entityType, rawFilters, rawFields, 'job');
}

export function projectExportRecord(
  record: ExportRecord,
  fields: Set<string> | null,
): Record<string, unknown> {
  if (!fields) {
    return record as unknown as Record<string, unknown>;
  }

  const source = record as unknown as Record<string, unknown>;
  const projected: Record<string, unknown> = {};

  for (const field of fields) {
    if (field in source) {
      projected[field] = source[field];
    }
  }

  return projected;
}

function normalizeExportValidationValue(
  entityType: EntityType,
  rawFilters: unknown,
  rawFields: unknown,
  mode: 'request' | 'job',
): NormalizedValidationValue {
  const filters = parseFilters(entityType, rawFilters, mode);
  const fields = parseFields(entityType, rawFields, mode);
  return { filters, fields };
}

function parseFilters(
  entityType: EntityType,
  rawFilters: unknown,
  mode: 'request' | 'job',
): Record<string, unknown> | null {
  if (rawFilters === undefined || rawFilters === null) {
    return null;
  }

  const filtersInput = canonicalizeFilterKeys(entityType, rawFilters);
  const result = getFiltersSchema(entityType).safeParse(filtersInput);
  if (!result.success) {
    throwValidationError(mode, 'filters', result.error);
  }

  const normalized = result.data as Record<string, unknown>;
  return Object.keys(normalized).length ? normalized : null;
}

function parseFields(
  entityType: EntityType,
  rawFields: unknown,
  mode: 'request' | 'job',
): Set<string> | null {
  if (rawFields === undefined || rawFields === null) {
    return null;
  }

  const fieldsInput = canonicalizeFieldValues(entityType, rawFields);
  const result = getFieldsSchema(entityType).safeParse(fieldsInput);
  if (!result.success) {
    throwValidationError(mode, 'fields', result.error);
  }

  return new Set(result.data);
}

function throwValidationError(
  mode: 'request' | 'job',
  target: ValidationTarget,
  error: z.ZodError,
): never {
  const message = buildValidationMessage(target, error);

  if (mode === 'request') {
    throw new HttpException(HttpStatusCode.UNPROCESSABLE_ENTITY, {
      errors: { [target]: [message] },
    });
  }

  throw new ExportValidationError(target, message);
}

function buildValidationMessage(target: ValidationTarget, error: z.ZodError): string {
  const details = error.issues.map((issue) => {
    const path = issue.path.length ? issue.path.join('.') : target;
    return `${path}: ${issue.message}`;
  });
  return `Invalid ${target}: ${details.join('; ')}`;
}

function canonicalizeFilterKeys(entityType: EntityType, value: unknown): unknown {
  if (!isPlainObject(value)) {
    return value;
  }

  const normalized: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    normalized[normalizeFilterKey(entityType, key)] = item;
  }
  return normalized;
}

function canonicalizeFieldValues(entityType: EntityType, value: unknown): unknown {
  if (!Array.isArray(value)) {
    return value;
  }

  return value.map((entry) =>
    typeof entry === 'string' ? normalizeFieldKey(entityType, entry) : entry,
  );
}

function normalizeFilterKey(entityType: EntityType, key: string): string {
  const normalized = normalizeKey(key);

  if (entityType === 'users' && normalized === 'createdat') {
    return 'created_at';
  }
  if (entityType === 'articles' && normalized === 'authorid') {
    return 'author_id';
  }
  if (entityType === 'articles' && normalized === 'publishedat') {
    return 'published_at';
  }
  if (entityType === 'articles' && normalized === 'createdat') {
    return 'created_at';
  }
  if (entityType === 'comments' && normalized === 'articleid') {
    return 'article_id';
  }
  if (entityType === 'comments' && normalized === 'userid') {
    return 'user_id';
  }
  if (entityType === 'comments' && normalized === 'createdat') {
    return 'created_at';
  }

  return normalized;
}

function normalizeFieldKey(entityType: EntityType, key: string): string {
  const normalized = normalizeKey(key);

  if (entityType === 'users') {
    if (normalized === 'createdat') {
      return 'created_at';
    }
    if (normalized === 'updatedat') {
      return 'updated_at';
    }
  }

  if (entityType === 'articles') {
    if (normalized === 'authorid') {
      return 'author_id';
    }
    if (normalized === 'publishedat') {
      return 'published_at';
    }
  }

  if (entityType === 'comments') {
    if (normalized === 'articleid') {
      return 'article_id';
    }
    if (normalized === 'userid') {
      return 'user_id';
    }
    if (normalized === 'createdat') {
      return 'created_at';
    }
  }

  return normalized;
}

function normalizeKey(value: string): string {
  return value
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s-]+/g, '_')
    .toLowerCase();
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
