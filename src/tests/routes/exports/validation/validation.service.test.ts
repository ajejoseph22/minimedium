import HttpException from '../../../../app/models/http-exception.model';
import { HttpStatusCode } from '../../../../app/models/http-status-code.model';
import {
  ExportValidationError,
  normalizeExportCreatePayload,
  resolveExportJobValidation,
} from '../../../../app/routes/exports/validation/validation.service';

describe('Export Validation Service', () => {
  it('should normalize aliases for filters and fields on create payload', () => {
    const normalized = normalizeExportCreatePayload(
      'articles',
      {
        authorId: '13',
        status: 'published',
        publishedAt: '2026-02-05T00:00:00Z',
        createdAt: { gte: '2026-01-01T00:00:00Z' },
      },
      ['id', 'authorId', 'title', 'publishedAt'],
    );

    expect(normalized.filters).toEqual({
      author_id: 13,
      status: 'published',
      published_at: '2026-02-05T00:00:00Z',
      created_at: { gte: '2026-01-01T00:00:00Z' },
    });
    expect(normalized.fields).toEqual(['id', 'author_id', 'title', 'published_at']);
  });

  it('should accept date range filters for users and comments createdAt', () => {
    const usersNormalized = normalizeExportCreatePayload(
      'users',
      { createdAt: { gte: '2026-01-01T00:00:00Z', lt: '2026-02-01T00:00:00Z' } },
      null,
    );
    const commentsNormalized = normalizeExportCreatePayload(
      'comments',
      { createdAt: { lte: '2026-02-05T00:00:00Z' } },
      null,
    );

    expect(usersNormalized.filters).toEqual({
      created_at: { gte: '2026-01-01T00:00:00Z', lt: '2026-02-01T00:00:00Z' },
    });
    expect(commentsNormalized.filters).toEqual({
      created_at: { lte: '2026-02-05T00:00:00Z' },
    });
  });

  it('should reject unknown filter keys on create payload', () => {
    expect(() =>
      normalizeExportCreatePayload('users', { unknown_filter: 'x' }, null),
    ).toThrow(HttpException);

    try {
      normalizeExportCreatePayload('users', { unknown_filter: 'x' }, null);
    } catch (error) {
      const typedError = error as HttpException;
      expect(typedError.errorCode).toBe(HttpStatusCode.UNPROCESSABLE_ENTITY);
      expect(typedError.message).toEqual(
        expect.objectContaining({
          errors: expect.objectContaining({
            filters: expect.any(Array),
          }),
        }),
      );
    }
  });

  it('should reject unknown field values on create payload', () => {
    expect(() =>
      normalizeExportCreatePayload('comments', null, ['id', 'non_existing']),
    ).toThrow(HttpException);
  });

  it('should throw ExportValidationError when persisted job config is invalid', () => {
    expect(() =>
      resolveExportJobValidation('articles', { bad_key: true }, ['id']),
    ).toThrow(ExportValidationError);
  });
});
