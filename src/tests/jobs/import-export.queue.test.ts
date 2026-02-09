const addMock = jest.fn();

jest.mock('bullmq', () => ({
  Queue: jest.fn(() => ({
    add: addMock,
  })),
}));

jest.mock('ioredis', () =>
  jest.fn(() => ({
    on: jest.fn(),
  })),
);

import { enqueueExportJob, enqueueImportJob } from '../../app/jobs/import-export.queue';

describe('import-export.queue', () => {
  beforeEach(() => {
    addMock.mockReset();
    addMock.mockResolvedValue({});
  });

  it('should enqueue import job with deterministic queue jobId', async () => {
    await enqueueImportJob({
      jobId: 'abc-123',
      resource: 'articles',
      format: 'ndjson',
    });

    expect(addMock).toHaveBeenCalledWith(
      'import',
      expect.objectContaining({
        jobId: 'abc-123',
        type: 'import',
      }),
      expect.objectContaining({
        jobId: 'import-abc-123',
      }),
    );
  });

  it('should enqueue export job with deterministic queue jobId', async () => {
    await enqueueExportJob({
      jobId: 'xyz-789',
      resource: 'users',
      format: 'json',
    });

    expect(addMock).toHaveBeenCalledWith(
      'export',
      expect.objectContaining({
        jobId: 'xyz-789',
        type: 'export',
      }),
      expect.objectContaining({
        jobId: 'export-xyz-789',
      }),
    );
  });
});
