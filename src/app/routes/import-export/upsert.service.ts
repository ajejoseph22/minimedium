import bcrypt from 'bcryptjs';
import { randomUUID } from 'crypto';
import slugify from 'slugify';
import type { Prisma, PrismaClient } from '@prisma/client';
import prismaClient from '../../../prisma/prisma-client';
import {
  ArticleImportRecord,
  CommentImportRecord,
  CreateRecordErrorOptions,
  EntityType,
  ImportRecord,
  ProcessingErrorCode,
  ValidationErrorCode,
} from './types';
import { loadConfig } from './config';
import { sanitizeValue } from './utils';

const config = loadConfig();

export interface IndexedImportRecord<TRecord extends ImportRecord = ImportRecord> {
  record: TRecord;
  recordIndex: number;
}

export interface BatchUpsertOptions {
  jobId: string;
  batchSize?: number;
  prisma?: PrismaClient;
}

export interface BatchUpsertResult {
  attempted: number;
  succeeded: number;
  failed: number;
  errors: CreateRecordErrorOptions[];
}

interface RecordOperation {
  record: ImportRecord;
  recordIndex: number;
  execute: () => Prisma.PrismaPromise<unknown>;
}

class UpsertRecordError extends Error {
  constructor(
    public errorCode: ValidationErrorCode | ProcessingErrorCode,
    public field: string,
    public value: unknown,
    message: string,
  ) {
    super(message);
    this.name = 'UpsertRecordError';
  }
}

export async function upsertImportRecords(
  records: IndexedImportRecord[],
  entityType: EntityType,
  options: BatchUpsertOptions,
): Promise<BatchUpsertResult> {
  const prisma = options.prisma ?? prismaClient;
  const batchSize = options.batchSize ?? config.batchSize;
  const result: BatchUpsertResult = {
    attempted: records.length,
    succeeded: 0,
    failed: 0,
    errors: [],
  };

  for (const chunk of chunkArray(records, batchSize)) {
    const batchResult = await upsertBatch(prisma, chunk, entityType, options.jobId);
    result.succeeded += batchResult.succeeded;
    result.failed += batchResult.failed;
    result.errors.push(...batchResult.errors);
  }

  return result;
}

async function upsertBatch(
  prisma: PrismaClient,
  records: IndexedImportRecord[],
  entityType: EntityType,
  jobId: string,
): Promise<BatchUpsertResult> {
  if (!records.length) {
    return { attempted: 0, succeeded: 0, failed: 0, errors: [] };
  }

  if (entityType === 'articles') {
    await ensureTags(prisma, records.map((entry) => entry.record as ArticleImportRecord));
  }

  const operations = await buildOperations(prisma, records, entityType);

  try {
    await prisma.$transaction(operations.map((operation) => operation.execute()));
    return {
      attempted: records.length,
      succeeded: records.length,
      failed: 0,
      errors: [],
    };
  } catch (error) {
    return await fallbackPerRecord(operations, entityType, jobId, records.length);
  }
}

async function fallbackPerRecord(
  operations: RecordOperation[],
  entityType: EntityType,
  jobId: string,
  attempted: number,
): Promise<BatchUpsertResult> {
  const errors: CreateRecordErrorOptions[] = [];
  let succeeded = 0;

  for (const operation of operations) {
    try {
      await operation.execute();
      succeeded += 1;
    } catch (error) {
      errors.push(
        mapUpsertError({
          error,
          entityType,
          record: operation.record,
          jobId,
          recordIndex: operation.recordIndex,
        }),
      );
    }
  }

  return {
    attempted,
    succeeded,
    failed: attempted - succeeded,
    errors,
  };
}

async function buildOperations(
  prisma: PrismaClient,
  records: IndexedImportRecord[],
  entityType: EntityType,
): Promise<RecordOperation[]> {
  switch (entityType) {
    case 'users':
      return buildUserOperations(prisma, records);
    case 'articles':
      return buildArticleOperations(prisma, records);
    case 'comments':
      return buildCommentOperations(prisma, records);
    default:
      throw new Error(`Unsupported entity type: ${entityType}`);
  }
}

async function buildUserOperations(
  prisma: PrismaClient,
  records: IndexedImportRecord[],
): Promise<RecordOperation[]> {
  const operations: RecordOperation[] = [];
  let defaultPasswordHash: string | null = null;

  const getDefaultPasswordHash = async () => {
    if (!defaultPasswordHash) {
      defaultPasswordHash = await bcrypt.hash(randomUUID(), 10);
    }
    return defaultPasswordHash;
  };

  for (const entry of records) {
    const record = entry.record as any;
    const email = normalizeEmail(record.email);
    const createdAt = parseDate(record.created_at);
    const updatedAt = parseDate(record.updated_at);
    const canCreate = Boolean(email);

    const updateData: Prisma.UserUpdateInput = {
      ...(email ? { email } : {}),
      ...(record.name ? { name: record.name } : {}),
      ...(record.role ? { role: record.role } : {}),
      ...(record.active !== undefined ? { active: record.active } : {}),
      ...(updatedAt ? { updatedAt } : {}),
    };

    if (canCreate) {
      const username = deriveUsername(email, record.name);
      const password = await getDefaultPasswordHash();
      const createData: Prisma.UserCreateInput = {
        ...(record.id ? { id: record.id } : {}),
        email,
        username,
        password,
        ...(record.name ? { name: record.name } : {}),
        ...(record.role ? { role: record.role } : {}),
        ...(record.active !== undefined ? { active: record.active } : {}),
        ...(createdAt ? { createdAt } : {}),
        ...(updatedAt ? { updatedAt } : {}),
      };

      const where = record.id ? { id: record.id } : { email };
      operations.push({
        record: entry.record,
        recordIndex: entry.recordIndex,
        execute: () => prisma.user.upsert({ where, update: updateData, create: createData }),
      });
      continue;
    }

    if (!record.id) {
      operations.push({
        record: entry.record,
        recordIndex: entry.recordIndex,
        execute: () =>
          Promise.reject(
            new UpsertRecordError(
              ValidationErrorCode.MISSING_REQUIRED_FIELD,
              'id',
              null,
              'User record missing id for update',
            ),
          ) as Prisma.PrismaPromise<unknown>,
      });
      continue;
    }

    operations.push({
      record: entry.record,
      recordIndex: entry.recordIndex,
      execute: () => prisma.user.update({ where: { id: record.id }, data: updateData }),
    });
  }

  return operations;
}

async function buildArticleOperations(
  prisma: PrismaClient,
  records: IndexedImportRecord[],
): Promise<RecordOperation[]> {
  const operations: RecordOperation[] = [];

  for (const entry of records) {
    const record = entry.record as ArticleImportRecord;
    const slug = record.slug ? record.slug.trim().toLowerCase() : undefined;
    const tags = normalizeTags(record.tags);
    const tagsProvided = Array.isArray(record.tags);
    const status = normalizeArticleStatus(record.status, record.published_at);
    const publishedAt = record.published_at ? new Date(record.published_at) : null;
    const isDraft = record.status === 'draft';

    const updateData: Prisma.ArticleUpdateInput = {
      ...(slug ? { slug } : {}),
      ...(record.title ? { title: record.title.trim() } : {}),
      ...(record.body ? { body: record.body.trim() } : {}),
      ...(record.title || record.body
        ? { description: buildDescription(record.title, record.body) }
        : {}),
      ...(record.author_id
        ? { author: { connect: { id: record.author_id } } }
        : {}),
      ...(status ? { status } : {}),
      ...(isDraft
        ? { publishedAt: null }
        : record.published_at
        ? { publishedAt }
        : {}),
      ...(tagsProvided
        ? {
            tagList: {
              set: tags.map((tag) => ({ name: tag })),
            },
          }
        : {}),
    };

    if (slug) {
      const createData: Prisma.ArticleCreateInput = {
        ...(record.id ? { id: record.id } : {}),
        slug,
        title: record.title.trim(),
        description: buildDescription(record.title, record.body),
        body: record.body.trim(),
        ...(status ? { status } : {}),
        ...(isDraft
          ? { publishedAt: null }
          : record.published_at
          ? { publishedAt }
          : {}),
        author: { connect: { id: record.author_id } },
        ...(tags.length
          ? {
              tagList: {
                connect: tags.map((tag) => ({ name: tag })),
              },
            }
          : {}),
      };

      const where = record.id ? { id: record.id } : { slug };
      operations.push({
        record: entry.record,
        recordIndex: entry.recordIndex,
        execute: () => prisma.article.upsert({ where, update: updateData, create: createData }),
      });
      continue;
    }

    operations.push({
      record: entry.record,
      recordIndex: entry.recordIndex,
      execute: () => prisma.article.update({ where: { id: record.id }, data: updateData }),
    });
  }

  return operations;
}

async function buildCommentOperations(
  prisma: PrismaClient,
  records: IndexedImportRecord[],
): Promise<RecordOperation[]> {
  const operations: RecordOperation[] = [];

  for (const entry of records) {
    const record = entry.record as CommentImportRecord;
    const createdAt = parseDate(record.created_at);

    const updateData: Prisma.CommentUpdateInput = {
      ...(record.body ? { body: record.body.trim() } : {}),
      ...(record.article_id ? { article: { connect: { id: record.article_id } } } : {}),
      ...(record.user_id ? { user: { connect: { id: record.user_id } } } : {}),
    };

    const createData: Prisma.CommentCreateInput = {
      ...(record.id ? { id: record.id } : {}),
      body: record.body.trim(),
      article: { connect: { id: record.article_id } },
      user: { connect: { id: record.user_id } },
      ...(createdAt ? { createdAt } : {}),
    };

    operations.push({
      record: entry.record,
      recordIndex: entry.recordIndex,
      execute: () =>
        prisma.comment.upsert({
          where: { id: record.id },
          update: updateData,
          create: createData,
        }),
    });
  }

  return operations;
}

async function ensureTags(prisma: PrismaClient, records: ArticleImportRecord[]) {
  const tagNames = new Set<string>();
  for (const record of records) {
    for (const tag of normalizeTags(record.tags)) {
      tagNames.add(tag);
    }
  }

  const tags = Array.from(tagNames).map((name) => ({ name }));
  if (tags.length === 0) {
    return;
  }

  await prisma.tag.createMany({ data: tags, skipDuplicates: true });
}

function normalizeEmail(email?: string): string | null {
  if (!email) {
    return null;
  }
  return email.trim().toLowerCase();
}

function deriveUsername(email: string, name?: string): string {
  if (email) {
    return email;
  }

  const base = name ? slugify(name, { lower: true, strict: true }) : 'user';
  return `${base}-${randomUUID()}`;
}

function normalizeTags(tags?: string[]): string[] {
  if (!Array.isArray(tags)) {
    return [];
  }

  const normalized = tags
    .map((tag) => tag.trim())
    .filter((tag) => tag.length > 0);

  return Array.from(new Set(normalized));
}

function normalizeArticleStatus(status?: string, publishedAt?: string): string | undefined {
  if (status) {
    return status.trim().toLowerCase();
  }

  if (publishedAt) {
    return 'published';
  }

  return undefined;
}

function buildDescription(title?: string, body?: string): string {
  const fallback = title?.trim() || body?.trim() || 'Imported article';

  if (!body) {
    return fallback;
  }

  return fallback.length > 160 ? fallback.slice(0, 160) : fallback;
}

function parseDate(value?: string): Date | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }

  return parsed;
}

function chunkArray<T>(items: T[], size: number): T[][] {
  if (size <= 0) {
    return [items];
  }

  const chunks: T[][] = [];

  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }

  return chunks;
}

function mapUpsertError(params: {
  error: unknown;
  entityType: EntityType;
  record: ImportRecord;
  jobId: string;
  recordIndex: number;
}): CreateRecordErrorOptions {
  const { error, entityType, record, jobId, recordIndex } = params;

  if (error instanceof UpsertRecordError) {
    return buildError(
      jobId,
      recordIndex,
      error.errorCode,
      error.message,
      error.field,
      error.value,
    );
  }

  if (isPrismaKnownError(error)) {
    if (error.code === 'P2002') {
      const target = extractPrismaTarget(error.meta?.target);
      const field = target ? mapRecordField(entityType, target) : null;
      return buildError(
        jobId,
        recordIndex,
        ValidationErrorCode.DUPLICATE_VALUE,
        field ? `Duplicate value for ${field}` : 'Duplicate value violates unique constraint',
        field ?? 'record',
        field ? getRecordValue(record, field) : null,
      );
    }

    if (error.code === 'P2003') {
      const rawField = extractPrismaField(error.meta?.field_name);
      const field = rawField ? mapRecordField(entityType, rawField) : null;
      return buildError(
        jobId,
        recordIndex,
        ValidationErrorCode.INVALID_REFERENCE,
        field ? `Invalid reference for ${field}` : 'Invalid foreign key reference',
        field ?? 'record',
        field ? getRecordValue(record, field) : null,
      );
    }

    if (error.code === 'P2025') {
      const field = inferLookupField(entityType, record);
      return buildError(
        jobId,
        recordIndex,
        ValidationErrorCode.INVALID_REFERENCE,
        'Record not found for update',
        field,
        getRecordValue(record, field),
      );
    }
  }

  return buildError(
    jobId,
    recordIndex,
    ProcessingErrorCode.BATCH_FAILED,
    'Batch upsert failed for record',
    'record',
    null,
  );
}

function buildError(
  jobId: string,
  recordIndex: number,
  errorCode: ValidationErrorCode | ProcessingErrorCode,
  message: string,
  field: string,
  value: unknown,
): CreateRecordErrorOptions {
  return {
    jobId,
    recordIndex,
    errorCode,
    message,
    field,
    value: sanitizeValue(value),
  };
}



function isPrismaKnownError(error: unknown): error is Prisma.PrismaClientKnownRequestError {
  return Boolean(
    error &&
      typeof error === 'object' &&
      'code' in error &&
      'clientVersion' in error,
  );
}

function extractPrismaTarget(target: unknown): string | null {
  if (Array.isArray(target) && target.length > 0) {
    return String(target[0]);
  }
  if (typeof target === 'string') {
    return target;
  }
  return null;
}

function extractPrismaField(field: unknown): string | null {
  if (typeof field === 'string') {
    return field.replace(/^\w+\./, '');
  }
  return null;
}

function mapRecordField(entityType: EntityType, field: string): string {
  const normalized = field.replace(/^\w+\./, '');
  const snake = toSnakeCase(normalized);

  if (snake === 'tag_list') {
    return 'tags';
  }

  if (entityType === 'articles' && snake === 'author_id') {
    return 'author_id';
  }

  return snake;
}

function toSnakeCase(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[-\s]+/g, '_')
    .toLowerCase();
}

function inferLookupField(entityType: EntityType, record: ImportRecord): string {
  switch (entityType) {
    case 'users':
      return 'id' in record && record.id ? 'id' : 'email';
    case 'articles':
      return 'id' in record && record.id ? 'id' : 'slug';
    case 'comments':
      return 'id';
    default:
      return 'record';
  }
}

function getRecordValue(record: ImportRecord, field: string): unknown {
  const key = field as keyof typeof record;
  return record[key];
}
