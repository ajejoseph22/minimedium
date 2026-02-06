import prismaMock from '../../prisma-mock';
import { upsertImportRecords } from '../../../app/routes/imports/upsert.service';
import { ValidationErrorCode } from '../../../app/routes/shared/import-export/types';


jest.mock('bcryptjs', () => ({
  hash: jest.fn().mockResolvedValue('hashed'),
}));

const prisma = prismaMock as unknown as any;

const baseOptions = (overrides: Partial<{ jobId: string; batchSize: number; prisma: any }> = {}) => ({
  jobId: 'job-1',
  batchSize: 2,
  prisma,
  ...overrides,
});

describe('Upsert Service', () => {
  it('should upsert users in a single batch', async () => {
    prisma.$transaction.mockResolvedValue([]);
    prisma.user.upsert.mockResolvedValue({ id: 1 });

    const records = [
      {
        record: { id: 1, email: 'user1@example.com', name: 'User 1', role: 'user', active: true },
        recordIndex: 0,
      },
      {
        record: { id: 2, email: 'user2@example.com', name: 'User 2', role: 'user', active: true },
        recordIndex: 1,
      },
    ];

    const result = await upsertImportRecords(records, 'users', baseOptions());

    expect(result.attempted).toBe(2);
    expect(result.succeeded).toBe(2);
    expect(result.failed).toBe(0);
    expect(result.errors).toHaveLength(0);
    expect(prisma.$transaction).toHaveBeenCalledTimes(1);
    expect(prisma.user.upsert).toHaveBeenCalledTimes(2);
  });

  it('should chunk batches based on batchSize', async () => {
    prisma.$transaction.mockResolvedValue([]);
    prisma.user.upsert.mockResolvedValue({ id: 1 });

    const records = [
      {
        record: { id: 1, email: 'user1@example.com', name: 'User 1', role: 'user', active: true },
        recordIndex: 0,
      },
      {
        record: { id: 2, email: 'user2@example.com', name: 'User 2', role: 'user', active: true },
        recordIndex: 1,
      },
    ];

    await upsertImportRecords(records, 'users', baseOptions({ batchSize: 1 }));

    expect(prisma.$transaction).toHaveBeenCalledTimes(2);
  });

  it('should fallback to individual processing on transaction failure, and capture duplicate errors', async () => {
    prisma.$transaction.mockRejectedValue(new Error('some error'));

    prisma.user.upsert
      .mockResolvedValueOnce({ id: 1 })
      .mockResolvedValueOnce({ id: 2 })
      .mockRejectedValueOnce({
        code: 'P2002',
        clientVersion: '4.16.1',
        meta: { target: ['email'] },
      })
      .mockResolvedValueOnce({ id: 2 });

    const records = [
      {
        record: { id: 1, email: 'user1@example.com', name: 'User 1', role: 'user', active: true },
        recordIndex: 0,
      },
      {
        record: { id: 2, email: 'user2@example.com', name: 'User 2', role: 'user', active: true },
        recordIndex: 1,
      },
    ];

    const result = await upsertImportRecords(records, 'users', baseOptions());

    expect(result.failed).toBe(1);
    expect(result.succeeded).toBe(1);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.DUPLICATE_VALUE);
    expect(result.errors[0]?.field).toBe('email');
    expect(result.errors[0]?.value).toBe('user1@example.com');
  });

  it('should return a per-record error when update is missing id', async () => {
    prisma.$transaction.mockImplementation(async (operations: Promise<unknown>[]) => {
      await Promise.allSettled(operations);
      throw new Error('some error');
    });

    const records = [
      {
        record: { name: 'Missing ID' },
        recordIndex: 0,
      },
    ];

    const result = await upsertImportRecords(records, 'users', baseOptions());

    expect(result.failed).toBe(1);
    expect(result.errors[0]?.errorCode).toBe(ValidationErrorCode.MISSING_REQUIRED_FIELD);
    expect(result.errors[0]?.field).toBe('id');
  });

  it('should ensure tags exist before article upsert', async () => {
    prisma.$transaction.mockResolvedValue([]);
    prisma.article.upsert.mockResolvedValue({ id: 1 });

    const records = [
      {
        record: {
          id: 1,
          slug: 'first-article',
          title: 'First',
          body: 'Body',
          author_id: 10,
          tags: ['news', 'tech'],
        },
        recordIndex: 0,
      },
      {
        record: {
          id: 2,
          slug: 'second-article',
          title: 'Second',
          body: 'Body',
          author_id: 10,
          tags: ['tech', 'product'],
        },
        recordIndex: 1,
      },
    ];

    await upsertImportRecords(records, 'articles', baseOptions());

    expect(prisma.tag.createMany).toHaveBeenCalledWith({
      data: [{ name: 'news' }, { name: 'tech' }, { name: 'product' }],
      skipDuplicates: true,
    });
  });

  it('should report foreign key errors using import field names', async () => {
    prisma.$transaction.mockRejectedValue(new Error('some error'));

    prisma.article.upsert
      .mockResolvedValueOnce({ id: 1 })
      .mockRejectedValueOnce({
        code: 'P2003',
        clientVersion: '4.16.1',
        meta: { field_name: 'authorId' },
      });

    const records = [
      {
        record: {
          id: 1,
          slug: 'mapped-field',
          title: 'Mapped',
          body: 'Body',
          author_id: 99,
        },
        recordIndex: 0,
      },
    ];

    const result = await upsertImportRecords(
      records,
      'articles',
      baseOptions()
    );

    expect(result.failed).toBe(1);
    expect(result.errors[0]?.field).toBe('author_id');
    expect(result.errors[0]?.value).toBe(99);
  });
});
