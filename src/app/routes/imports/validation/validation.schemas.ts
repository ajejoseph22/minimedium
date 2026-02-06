import { z } from 'zod';

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const KEBAB_CASE_REGEX = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

const DEFAULT_ALLOWED_USER_ROLES = new Set(['user', 'admin']);
const DEFAULT_ALLOWED_ARTICLE_STATUSES = new Set(['draft', 'published']);
export const ZodIssueCode = {
  invalid_type: 'invalid_type',
  invalid_value: 'invalid_value',
  too_small: 'too_small',
  too_big: 'too_big',
  invalid_format: 'invalid_format',
  custom: 'custom',
} as const;

export type ZodIssueCode = (typeof ZodIssueCode)[keyof typeof ZodIssueCode];

export function buildUserSchema(allowedRoles: Set<string> = DEFAULT_ALLOWED_USER_ROLES) {
  return z
    .object({
      id: z.number().int().positive().optional(),
      email: z
        .string()
        .trim()
        .min(1, { message: 'email is required' })
        .transform((value) => value.toLowerCase())
        .refine((value) => EMAIL_REGEX.test(value), {
          message: 'Email must be a valid email address',
        })
        .optional(),
      name: z
        .string()
        .trim()
        .min(1, { message: 'name is required' }),
      role: z
        .string()
        .trim()
        .transform((value) => value.toLowerCase())
        .refine((value) => allowedRoles.has(value), {
          message: 'Role is not allowed',
        })
        .optional(),
      active: z.boolean().optional(),
      created_at: isoDateSchema('created_at').optional(),
      updated_at: isoDateSchema('updated_at').optional(),
    })
    .passthrough()
    .superRefine((data, ctx) => {
      if (!data.id && !data.email) {
        ctx.addIssue({
          code: ZodIssueCode.custom,
          message: 'User must include id or email for upsert',
          path: ['id'],
        });
      }
    });
}

export function buildArticleSchema(allowedStatuses: Set<string> = DEFAULT_ALLOWED_ARTICLE_STATUSES) {
  return z
    .object({
      id: z.number().int().positive().optional(),
      slug: z
        .string()
        .trim()
        .transform((value) => value.toLowerCase())
        .refine((value) => KEBAB_CASE_REGEX.test(value), {
          message: 'Slug must be kebab-case',
        })
        .optional(),
      title: z
        .string()
        .trim()
        .min(1, { message: 'title is required' }),
      body: z
        .string()
        .trim()
        .min(1, { message: 'body is required' }),
      author_id: z
        .number()
        .int()
        .positive(),
      tags: z.array(z.string()).optional(),
      published_at: isoDateSchema('published_at').optional(),
      status: z
        .string()
        .trim()
        .transform((value) => value.toLowerCase())
        .refine((value) => allowedStatuses.has(value), {
          message: 'Status is not allowed',
        })
        .optional(),
    })
    .passthrough()
    .superRefine((data, ctx) => {
      if (!data.id && !data.slug) {
        ctx.addIssue({
          code: ZodIssueCode.custom,
          message: 'Article must include id or slug for upsert',
          path: ['id'],
        });
      }

      if (data.status === 'draft' && data.published_at) {
        ctx.addIssue({
          code: ZodIssueCode.custom,
          message: 'Draft articles must not include published_at',
          path: ['published_at'],
        });
      }
    });
}

export function buildCommentSchema() {
  return z
    .object({
      id: z
        .number()
        .int()
        .positive(),
      body: z
        .string()
        .trim()
        .min(1, { message: 'body is required' })
        .refine((value) => countWords(value) <= 500, {
          message: 'Comment body must be 500 words or fewer',
        }),
      article_id: z
        .number()
        .int()
        .positive(),
      user_id: z
        .number()
        .int()
        .positive(),
      created_at: isoDateSchema('created_at').optional(),
    })
    .passthrough();
}

export function isoDateSchema(field: string) {
  return z
    .string()
    .refine((value) => !Number.isNaN(Date.parse(value)), {
      message: `${field} must be an ISO date string`,
    });
}

function countWords(value: string): number {
  const trimmed = value.trim();

  if (!trimmed) {
    return 0;
  }

  return trimmed.split(/\s+/).length;
}
