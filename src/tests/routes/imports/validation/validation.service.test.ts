import prismaMock from '../../../prisma-mock';
import {
  validateArticleRecord,
  validateCommentRecord,
  validateUserRecord,
} from '../../../../app/routes/imports/validation/validation.service';
import { createValidationCache } from '../../../../app/routes/imports/validation/validation.validators';
import { ValidationErrorCode } from '../../../../app/routes/shared/import-export/types';

const prisma: any = prismaMock;

const baseContext = () => ({
  jobId: 'job-1',
  recordIndex: 0,
  prisma,
  cache: createValidationCache(),
});

describe('Validation Service', () => {
  describe('users', () => {
    it('should accept a valid user', async () => {
      prisma.user.findFirst.mockResolvedValue(null);

      const result = await validateUserRecord(
        {
          id: 10,
          email: 'Test@Example.com',
          name: 'Test User',
          role: 'user',
          active: true,
        },
        baseContext()
      );

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
      expect(result.record?.email).toBe('test@example.com');
    });

    it('should accept a user with id but no email', async () => {
      const result = await validateUserRecord(
        {
          id: 10,
          name: 'Test User',
          role: 'user',
          active: true,
        },
        baseContext()
      );

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject a user without id and email', async () => {
      const result = await validateUserRecord(
        {
          name: 'Test User',
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.MISSING_REQUIRED_FIELD);
    });

    it('should reject an invalid email', async () => {
      const result = await validateUserRecord(
        {
          id: 10,
          email: 'not-an-email',
          name: 'Test User',
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.INVALID_FIELD_FORMAT);
    });

    it('should reject an unsupported role', async () => {
      const result = await validateUserRecord(
        {
          id: 10,
          email: 'test@example.com',
          name: 'Test User',
          role: 'superadmin',
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.INVALID_ENUM_VALUE);
    });

    it('should reject non-boolean active values', async () => {
      const result = await validateUserRecord(
        {
          id: 10,
          email: 'test@example.com',
          name: 'Test User',
          active: 'yes',
        } as any,
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.INVALID_FIELD_TYPE);
    });

    it('should reject duplicate emails in a batch', async () => {
      prisma.user.findFirst.mockResolvedValue(null);
      const cache = createValidationCache();
      const context = { ...baseContext(), cache };

      const first = await validateUserRecord(
        {
          id: 10,
          email: 'test@example.com',
          name: 'Test User',
        },
        context
      );
      const second = await validateUserRecord(
        {
          id: 11,
          email: 'test@example.com',
          name: 'Other User',
        },
        { ...context, recordIndex: 1 }
      );

      expect(first.valid).toBe(true);
      expect(second.valid).toBe(false);
      expect(second.errors[0]?.errorCode).toBe(ValidationErrorCode.DUPLICATE_VALUE);
    });

    it('should reject an email already in the database if id doesn\'t match', async () => {
      prisma.user.findFirst.mockResolvedValue({ id: 99 });

      const result = await validateUserRecord(
        {
          email: 'test@example.com',
          name: 'Test User',
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.DUPLICATE_VALUE);
      expect(result.skip).toBe(true);
    });

    it('should allow email reuse for the same id', async () => {
      prisma.user.findFirst.mockResolvedValue({ id: 99 });

      const result = await validateUserRecord(
        {
          id: 99,
          email: 'test@example.com',
          name: 'Test User',
        },
        baseContext()
      );

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });
  });

  describe('articles', () => {
    it('should accept a valid article', async () => {
      prisma.user.count.mockResolvedValue(1);
      prisma.article.findUnique.mockResolvedValue(null);

      const result = await validateArticleRecord(
        {
          id: 1,
          slug: 'my-article',
          title: 'My Article',
          body: 'Body text',
          author_id: 10,
          status: 'draft',
        },
        baseContext()
      );

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject a non-kebab-case slug', async () => {
      const result = await validateArticleRecord(
        {
          id: 1,
          slug: 'Bad Slug',
          title: 'My Article',
          body: 'Body text',
          author_id: 10,
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.INVALID_FIELD_FORMAT);
    });

    it('should reject articles with missing author_id', async () => {
      prisma.user.count.mockResolvedValue(0);

      const result = await validateArticleRecord(
        {
          id: 1,
          slug: 'my-article',
          title: 'My Article',
          body: 'Body text',
          author_id: 10,
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors.some((error) => error.errorCode === ValidationErrorCode.INVALID_REFERENCE)).toBe(true);
      expect(result.skip).toBe(true);
    });

    it('should reject draft articles with published_at', async () => {
      prisma.user.count.mockResolvedValue(1);

      const result = await validateArticleRecord(
        {
          id: 1,
          slug: 'my-article',
          title: 'My Article',
          body: 'Body text',
          author_id: 10,
          status: 'draft',
          published_at: new Date().toISOString(),
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.INVALID_FIELD_FORMAT);
    });

    it('should reject duplicate slugs in a batch', async () => {
      prisma.user.count.mockResolvedValue(1);
      prisma.article.findUnique.mockResolvedValue(null);
      const cache = createValidationCache();
      const context = { ...baseContext(), cache };

      const first = await validateArticleRecord(
        {
          id: 1,
          slug: 'my-article',
          title: 'My Article',
          body: 'Body text',
          author_id: 10,
        },
        context
      );

      const second = await validateArticleRecord(
        {
          id: 2,
          slug: 'my-article',
          title: 'My Article 2',
          body: 'Body text',
          author_id: 10,
        },
        { ...context, recordIndex: 1 }
      );

      expect(first.valid).toBe(true);
      expect(second.valid).toBe(false);
      expect(second.errors[0]?.errorCode).toBe(ValidationErrorCode.DUPLICATE_VALUE);
    });

    it('should reject a slug already in the database', async () => {
      prisma.user.count.mockResolvedValue(1);
      prisma.article.findUnique.mockResolvedValue({ id: 99 });

      const result = await validateArticleRecord(
        {
          slug: 'my-article',
          title: 'My Article',
          body: 'Body text',
          author_id: 10,
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.DUPLICATE_VALUE);
    });
  });

  describe('comments', () => {
    it('should accept a valid comment', async () => {
      prisma.user.count.mockResolvedValue(1);
      prisma.article.count.mockResolvedValue(1);

      const result = await validateCommentRecord(
        {
          id: 1,
          body: 'Nice article',
          article_id: 10,
          user_id: 20,
        },
        baseContext()
      );

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject comments longer than 500 words', async () => {
      const longBody = new Array(502).fill('word').join(' ');

      const result = await validateCommentRecord(
        {
          id: 1,
          body: longBody,
          article_id: 10,
          user_id: 20,
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.FIELD_TOO_LONG);
    });

    it('should reject comments with missing foreign keys', async () => {
      prisma.user.count.mockResolvedValue(0);
      prisma.article.count.mockResolvedValue(0);

      const result = await validateCommentRecord(
        {
          id: 1,
          body: 'Nice article',
          article_id: 10,
          user_id: 20,
        },
        baseContext()
      );

      expect(result.valid).toBe(false);
      expect(result.errors.some((error) => error.errorCode === ValidationErrorCode.INVALID_REFERENCE)).toBe(true);
      expect(result.skip).toBe(true);
    });
  });
});
