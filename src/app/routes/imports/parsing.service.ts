import { Readable } from 'stream';
import readline from 'readline';
import { parser as jsonParser } from 'stream-json';
import { streamArray } from 'stream-json/streamers/StreamArray';
import { FileErrorCode, ImportExportErrorCode, ProcessingErrorCode } from '../shared/import-export/types';

export interface ParseOptions {
  maxRecords: number;
}

export interface ParsedRecord<T = unknown> {
  record: T;
  index: number;
  lineNumber?: number;
}

export class ImportExportParseError extends Error {
  code: ImportExportErrorCode;
  details?: Record<string, unknown>;

  constructor(code: ImportExportErrorCode, message: string, details?: Record<string, unknown>) {
    super(message);
    this.name = 'ImportExportParseError';
    this.code = code;
    this.details = details;
  }
}

function enforceRecordLimit(recordCount: number, maxRecords: number) {
  if (recordCount > maxRecords) {
    throw new ImportExportParseError(
      FileErrorCode.TOO_MANY_RECORDS,
      `Import exceeds maximum record limit (${maxRecords}).`,
      { maxRecords, recordCount }
    );
  }
}

function normalizeErrorDetails(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return { message: error.message, name: error.name };
  }

  return { message: String(error) };
}

/**
 * Parse NDJSON input line-by-line using constant memory.
 */
export async function* parseNdjsonStream<T>(
  input: Readable,
  options: ParseOptions
): AsyncGenerator<ParsedRecord<T>> {
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  let recordCount = 0;
  let lineNumber = 0;

  try {
    for await (const line of rl) {
      lineNumber += 1;
      if (!line.trim()) {
        continue;
      }

      recordCount += 1;
      enforceRecordLimit(recordCount, options.maxRecords);

      try {
        const record = JSON.parse(line) as T;
        yield { record, index: recordCount - 1, lineNumber };
      } catch (error) {
        throw new ImportExportParseError(
          ProcessingErrorCode.PARSE_ERROR,
          `Invalid JSON at line ${lineNumber}.`,
          { lineNumber, ...normalizeErrorDetails(error) }
        );
      }
    }
  } catch (error) {
    if (error instanceof ImportExportParseError) {
      throw error;
    }

    throw new ImportExportParseError(
      ProcessingErrorCode.STREAM_ERROR,
      'Failed to read NDJSON stream.',
      normalizeErrorDetails(error)
    );
  } finally {
    rl.close();
  }
}

/**
 * Parse a JSON array stream using stream-json with constant memory.
 */
export async function* parseJsonArrayStream<T>(
  input: Readable,
  options: ParseOptions
): AsyncGenerator<ParsedRecord<T>> {
  const stream = input.pipe(jsonParser()).pipe(streamArray());
  let recordCount = 0;

  try {
    for await (const chunk of stream) {
      recordCount += 1;
      enforceRecordLimit(recordCount, options.maxRecords);
      yield { record: chunk.value as T, index: recordCount - 1 };
    }
  } catch (error) {
    if (error instanceof ImportExportParseError) {
      throw error;
    }

    throw new ImportExportParseError(
      ProcessingErrorCode.PARSE_ERROR,
      'Invalid JSON array input.',
      normalizeErrorDetails(error)
    );
  } finally {
    stream.destroy();
  }
}
