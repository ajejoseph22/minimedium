import importExportController from '../../app/routes/import-export/import-export.controller';
import { streamExportRecords } from '../../app/routes/import-export';

jest.mock('../../app/routes/auth/auth', () => ({
  __esModule: true,
  default: { required: (_req, _res, next) => next() },
}));

jest.mock('../../app/routes/import-export/config', () => ({
  loadConfig: () => ({
    exportStreamMaxLimit: 2,
    batchSize: 1,
  }),
}));

jest.mock('../../app/routes/import-export/export.service', () => ({
  streamExportRecords: jest.fn(),
}));

type TestResponse = {
  statusCode: number;
  headers: Record<string, string>;
  headersSent: boolean;
  ended: boolean;
  status: (code: number) => TestResponse;
  setHeader: (key: string, value: string) => void;
  write: (chunk: string) => boolean;
  end: () => void;
};

function createTestResponse() {
  let body = '';
  let resolveEnd: (() => void) | null = null;
  const endPromise = new Promise<void>((resolve) => {
    resolveEnd = resolve;
  });

  const res: TestResponse = {
    statusCode: 200,
    headers: {},
    headersSent: false,
    ended: false,
    status(code: number) {
      res.statusCode = code;
      return res;
    },
    setHeader(key: string, value: string) {
      res.headers[key] = value;
    },
    write(chunk: string) {
      res.headersSent = true;
      body += chunk;
      return true;
    },
    end() {
      res.ended = true;
      resolveEnd?.();
    },
  };

  return { res, getBody: () => body, endPromise };
}

async function runRequest(query: Record<string, string>) {
  const req = {
    method: 'GET',
    url: '/v1/exports',
    query,
    on: jest.fn(),
  };
  const { res, getBody, endPromise } = createTestResponse();
  const next = jest.fn();
  (
    importExportController as unknown as (req, res, next: (error?: unknown) => void) => void
  )(req, res, next);
  await endPromise;
  return { res, body: getBody(), next };
}

describe('Export Controller', () => {
  it('should stream JSON with nextCursor when limit is reached', async () => {
    (streamExportRecords as jest.Mock).mockImplementation(async function* () {
      yield {
        id: 1,
        email: 'first@example.com',
        name: 'First',
        role: 'user',
        active: true,
        created_at: '2026-02-05T00:00:00Z',
        updated_at: '2026-02-05T00:00:00Z',
      };
      yield {
        id: 2,
        email: 'second@example.com',
        name: 'Second',
        role: 'user',
        active: true,
        created_at: '2026-02-05T00:00:00Z',
        updated_at: '2026-02-05T00:00:00Z',
      };
    });

    const { body, res, next } = await runRequest({ resource: 'users', format: 'json', limit: '2' });

    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(200);
    const parsed = JSON.parse(body);
    expect(parsed.data).toHaveLength(2);
    expect(parsed.nextCursor).toBe(2);
  });

  it('should stream NDJSON with a trailing cursor line', async () => {
    (streamExportRecords as jest.Mock).mockImplementation(async function* () {
      yield {
        id: 1,
        email: 'first@example.com',
        name: 'First',
        role: 'user',
        active: true,
        created_at: '2026-02-05T00:00:00Z',
        updated_at: '2026-02-05T00:00:00Z',
      };
    });

    const { body } = await runRequest({ resource: 'users', format: 'ndjson', limit: '1' });
    const lines = body.trim().split('\n');
    expect(lines).toHaveLength(2);
    const cursorLine = JSON.parse(lines[1] ?? '{}');
    expect(cursorLine._type).toBe('cursor');
    expect(cursorLine.nextCursor).toBe(1);
  });

  it('should reject limits above the max', async () => {
    const req = {
      method: 'GET',
      url: '/v1/exports',
      query: { resource: 'users', limit: '10' },
      on: jest.fn(),
    };
    const { res } = createTestResponse();
    const next = jest.fn();
    (
      importExportController as unknown as (req, res, next: (error?: unknown) => void) => void
    )(req, res, next);
    // Wait for next tick to allow controller to call next(error)
    await new Promise((resolve) => setImmediate(resolve));
    expect(next).toHaveBeenCalled();
    const error = next.mock.calls[0]?.[0];
    expect(error?.errorCode).toBe(422);
  });
});
