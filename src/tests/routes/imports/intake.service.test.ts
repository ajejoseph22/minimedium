import {
  ImportExportError,
  validateUploadedFile,
} from '../../../app/routes/imports/intake.service';
import { FileErrorCode } from '../../../app/routes/shared/import-export/types';
import { createUploadedFile } from '../../helpers/uploaded-file';

describe('Intake Service', () => {
  it('should accept ndjson upload when mimetype is generic octet-stream but extension is supported', async () => {
    await expect(
      validateUploadedFile(createUploadedFile({
        filename: 'upload-1.ndjson',
        path: '/tmp/upload-1.ndjson',
        originalname: 'articles-import.ndjson',
        mimetype: 'application/octet-stream',
        size: 128,
      }))
    ).resolves.toBeUndefined();
  });

  it('should reject unsupported upload format when both mimetype and extension are unsupported', async () => {
    await expect(
      validateUploadedFile(createUploadedFile({
        filename: 'upload-2.bin',
        path: '/tmp/upload-2.bin',
        originalname: 'payload.bin',
        mimetype: 'application/octet-stream',
        size: 128,
      }))
    ).rejects.toMatchObject<Partial<ImportExportError>>({
      code: FileErrorCode.UNSUPPORTED_FORMAT,
      message: 'Unsupported content type',
    });
  });

  it('should reject empty uploads', async () => {
    await expect(
      validateUploadedFile(createUploadedFile({
        filename: 'upload-3.ndjson',
        path: '/tmp/upload-3.ndjson',
        originalname: 'empty.ndjson',
        mimetype: 'application/x-ndjson',
        size: 0,
      }))
    ).rejects.toMatchObject<Partial<ImportExportError>>({
      code: FileErrorCode.EMPTY_FILE,
      message: 'Uploaded file is empty',
    });
  });
});
