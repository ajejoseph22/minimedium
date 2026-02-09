import { Readable } from 'stream';
import { parseJsonArrayStream } from '../../../app/routes/imports/parsing.service';
import { ProcessingErrorCode } from '../../../app/routes/shared/import-export/types';

describe('parsing.service', () => {
  it('should parse valid json array records', async () => {
    const input = Readable.from(['[{"id":1},{"id":2}]']);
    const records: Array<{ id: number }> = [];

    for await (const parsed of parseJsonArrayStream<{ id: number }>(input, { maxRecords: 10 })) {
      records.push(parsed.record);
    }

    expect(records).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it('should throw parse error for malformed json array input', async () => {
    // Object + trailing object simulates json file content that is not a valid JSON array.
    const input = Readable.from(['{"id":1}\n{"id":2}']);

    const consume = async () => {
      for await (const _record of parseJsonArrayStream(input, { maxRecords: 10 })) {
        // no-op
      }
    };

    await expect(consume()).rejects.toEqual(
      expect.objectContaining({
        name: 'ImportExportParseError',
        code: ProcessingErrorCode.PARSE_ERROR,
      }),
    );
  });
});
