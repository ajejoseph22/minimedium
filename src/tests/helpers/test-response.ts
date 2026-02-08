export interface TestResponseHarness {
  res: any;
  done: Promise<void>;
  getJsonBody: () => unknown;
  getTextBody: () => string;
}

export function createTestResponse(): TestResponseHarness {
  let resolveDone: (() => void) | null = null;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });
  let jsonBody: unknown;
  let textBody = '';

  const res: any = {
    statusCode: 200,
    headersSent: false,
    headers: {} as Record<string, string>,
    status(code: number) {
      this.statusCode = code;
      return this;
    },
    json(payload: unknown) {
      this.headersSent = true;
      jsonBody = payload;
      resolveDone?.();
      return this;
    },
    setHeader(key: string, value: string) {
      this.headers[key] = value;
    },
    write(chunk: string) {
      this.headersSent = true;
      textBody += chunk;
      return true;
    },
    end() {
      resolveDone?.();
      return this;
    },
  };

  return { res, done, getJsonBody: () => jsonBody, getTextBody: () => textBody };
}
