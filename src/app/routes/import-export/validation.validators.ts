import prismaClient from '../../../prisma/prisma-client';
import { CreateRecordErrorOptions, ValidationErrorCode } from './types';
import type { PrismaClient } from '@prisma/client';

export interface ValidationCache {
  seenEmails: Set<string>;
  seenSlugs: Set<string>;
  emailLookup: Map<string, number | null>;
  slugLookup: Map<string, number | null>;
  userIdLookup: Map<number, boolean>;
  articleIdLookup: Map<number, boolean>;
}

export interface RecordValidationContext {
  jobId: string;
  recordIndex: number;
  prisma?: PrismaClient;
}

export function createValidationCache(): ValidationCache {
  return {
    seenEmails: new Set(),
    seenSlugs: new Set(),
    emailLookup: new Map(),
    slugLookup: new Map(),
    userIdLookup: new Map(),
    articleIdLookup: new Map(),
  };
}

export function addError(
  errors: CreateRecordErrorOptions[],
  context: RecordValidationContext,
  errorCode: ValidationErrorCode,
  message: string,
  field: string,
  value: unknown,
) {
  errors.push({
    jobId: context.jobId,
    recordIndex: context.recordIndex,
    errorCode,
    message,
    field,
    value: sanitizeValue(value),
  });
}

export async function validateUserEmailUniqueness(
  email: string,
  recordId: number | undefined,
  errors: CreateRecordErrorOptions[],
  context: RecordValidationContext,
  cache: ValidationCache,
) {
  if (cache.seenEmails.has(email)) {
    addError(errors, context, ValidationErrorCode.DUPLICATE_VALUE, 'Email is duplicated in this import batch', 'email', email);
    return;
  }
  cache.seenEmails.add(email);

  const existingId = await lookupUserIdByEmail(email, context, cache);
  if (existingId && (!recordId || existingId !== recordId)) {
    addError(errors, context, ValidationErrorCode.DUPLICATE_VALUE, 'Email is already in use', 'email', email);
  }
}

export async function validateArticleSlugUniqueness(
  slug: string,
  recordId: number | undefined,
  errors: CreateRecordErrorOptions[],
  context: RecordValidationContext,
  cache: ValidationCache,
) {
  if (cache.seenSlugs.has(slug)) {
    addError(errors, context, ValidationErrorCode.DUPLICATE_VALUE, 'Slug is duplicated in this import batch', 'slug', slug);
    return;
  }
  cache.seenSlugs.add(slug);

  const existingId = await lookupArticleIdBySlug(slug, context, cache);
  if (existingId && (!recordId || existingId !== recordId)) {
    addError(errors, context, ValidationErrorCode.DUPLICATE_VALUE, 'Slug is already in use', 'slug', slug);
  }
}

export async function validateUserExists(
  userId: number,
  errors: CreateRecordErrorOptions[],
  context: RecordValidationContext,
  cache: ValidationCache,
  field: string,
) {
  const exists = await lookupUserExists(userId, context, cache);
  if (!exists) {
    addError(errors, context, ValidationErrorCode.INVALID_REFERENCE, 'User does not exist', field, userId);
  }
}

export async function validateArticleExists(
  articleId: number,
  errors: CreateRecordErrorOptions[],
  context: RecordValidationContext,
  cache: ValidationCache,
  field: string,
) {
  const exists = await lookupArticleExists(articleId, context, cache);
  if (!exists) {
    addError(errors, context, ValidationErrorCode.INVALID_REFERENCE, 'Article does not exist', field, articleId);
  }
}

async function lookupUserIdByEmail(
  email: string,
  context: RecordValidationContext,
  cache: ValidationCache,
): Promise<number | null> {
  if (cache.emailLookup.has(email)) {
    return cache.emailLookup.get(email) ?? null;
  }
  const prisma = context.prisma ?? prismaClient;
  const existing = await prisma.user.findFirst({
    where: {
      email: {
        equals: email,
        mode: 'insensitive',
      },
    },
    select: { id: true },
  });
  const id = existing?.id ?? null;
  cache.emailLookup.set(email, id);
  return id;
}

async function lookupArticleIdBySlug(
  slug: string,
  context: RecordValidationContext,
  cache: ValidationCache,
): Promise<number | null> {
  if (cache.slugLookup.has(slug)) {
    return cache.slugLookup.get(slug) ?? null;
  }
  const prisma = context.prisma ?? prismaClient;
  const existing = await prisma.article.findUnique({
    where: { slug },
    select: { id: true },
  });
  const id = existing?.id ?? null;
  cache.slugLookup.set(slug, id);
  return id;
}

async function lookupUserExists(
  userId: number,
  context: RecordValidationContext,
  cache: ValidationCache,
): Promise<boolean> {
  if (cache.userIdLookup.has(userId)) {
    return cache.userIdLookup.get(userId) ?? false;
  }
  const prisma = context.prisma ?? prismaClient;
  const exists = (await prisma.user.count({ where: { id: userId } })) > 0;
  cache.userIdLookup.set(userId, exists);
  return exists;
}

async function lookupArticleExists(
  articleId: number,
  context: RecordValidationContext,
  cache: ValidationCache,
): Promise<boolean> {
  if (cache.articleIdLookup.has(articleId)) {
    return cache.articleIdLookup.get(articleId) ?? false;
  }
  const prisma = context.prisma ?? prismaClient;
  const exists = (await prisma.article.count({ where: { id: articleId } })) > 0;
  cache.articleIdLookup.set(articleId, exists);
  return exists;
}

function sanitizeValue(value: unknown): unknown {
  if (value === undefined || value === null) {
    return null;
  }

  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (Array.isArray(value)) {
    return value.slice(0, 10);
  }

  if (typeof value === 'object') {
    return '[object]';
  }

  return String(value);
}
