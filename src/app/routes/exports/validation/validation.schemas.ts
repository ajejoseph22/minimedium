import { z } from 'zod';
import type { EntityType } from '../../shared/import-export/types';

const positiveIntSchema = z.coerce.number().int().positive();
const nonEmptyStringSchema = z.string().trim().min(1);
const dateTimeStringSchema = z
  .string()
  .trim()
  .refine((value) => !Number.isNaN(Date.parse(value)), {
    message: 'must be a valid ISO date-time string',
  });
const dateRangeSchema = z
  .object({
    gt: dateTimeStringSchema.optional(),
    gte: dateTimeStringSchema.optional(),
    lt: dateTimeStringSchema.optional(),
    lte: dateTimeStringSchema.optional(),
  })
  .strict()
  .refine((value) => Object.keys(value).length > 0, {
    message: 'must contain at least one of gt, gte, lt, lte',
  });
const dateFilterSchema = z.union([dateTimeStringSchema, dateRangeSchema]);
const nullableDateFilterSchema = z.union([dateTimeStringSchema, dateRangeSchema, z.null()]);

export const userFiltersSchema = z
  .object({
    id: positiveIntSchema.optional(),
    email: nonEmptyStringSchema.optional(),
    role: nonEmptyStringSchema.optional(),
    name: nonEmptyStringSchema.optional(),
    active: z.coerce.boolean().optional(),
    created_at: dateFilterSchema.optional(),
  })
  .strict();

export const articleFiltersSchema = z
  .object({
    id: positiveIntSchema.optional(),
    slug: nonEmptyStringSchema.optional(),
    status: nonEmptyStringSchema.optional(),
    author_id: positiveIntSchema.optional(),
    published_at: nullableDateFilterSchema.optional(),
    created_at: dateFilterSchema.optional(),
  })
  .strict();

export const commentFiltersSchema = z
  .object({
    id: positiveIntSchema.optional(),
    article_id: positiveIntSchema.optional(),
    user_id: positiveIntSchema.optional(),
    created_at: dateFilterSchema.optional(),
  })
  .strict();

export const userFieldEnum = z.enum([
  'id',
  'email',
  'name',
  'role',
  'active',
  'created_at',
  'updated_at',
]);

export const articleFieldEnum = z.enum([
  'id',
  'slug',
  'title',
  'body',
  'author_id',
  'tags',
  'published_at',
  'status',
]);

export const commentFieldEnum = z.enum([
  'id',
  'article_id',
  'user_id',
  'body',
  'created_at',
]);

export function getFiltersSchema(entityType: EntityType) {
  switch (entityType) {
    case 'users':
      return userFiltersSchema;
    case 'articles':
      return articleFiltersSchema;
    case 'comments':
      return commentFiltersSchema;
    default:
      return z.never();
  }
}

export function getFieldsSchema(entityType: EntityType) {
  switch (entityType) {
    case 'users':
      return z.array(userFieldEnum).min(1);
    case 'articles':
      return z.array(articleFieldEnum).min(1);
    case 'comments':
      return z.array(commentFieldEnum).min(1);
    default:
      return z.never();
  }
}
