export function createTestResponse(): {
  res: any;
  done: Promise<void>;
  getJsonBody: () => any;
  getTextBody: () => string;
} {
  let resolveDone = () => undefined;
  let isDone = false;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });
  const complete = () => {
    if (isDone) {
      return;
    }
    isDone = true;
    resolveDone();
  };
  let jsonBody: unknown;
  let textBody = '';

  const res = {
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
      complete();
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
    end(chunk?: string) {
      if (chunk) {
        textBody += chunk;
      }
      this.headersSent = true;
      complete();
      return this;
    },
  };

  return { res, done, getJsonBody: () => jsonBody, getTextBody: () => textBody };
}
