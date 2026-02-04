-- CreateEnum
CREATE TYPE "JobStatus" AS ENUM ('queued', 'running', 'partial', 'succeeded', 'failed', 'cancelled');

-- CreateEnum
CREATE TYPE "ImportExportFormat" AS ENUM ('ndjson', 'json');

-- CreateEnum
CREATE TYPE "ImportExportResource" AS ENUM ('users', 'articles', 'comments');

-- CreateEnum
CREATE TYPE "ImportSourceType" AS ENUM ('upload', 'url');

-- CreateTable
CREATE TABLE "ImportJob" (
    "id" TEXT NOT NULL,
    "status" "JobStatus" NOT NULL DEFAULT 'queued',
    "resource" "ImportExportResource" NOT NULL,
    "format" "ImportExportFormat" NOT NULL,
    "sourceType" "ImportSourceType" NOT NULL,
    "sourceLocation" TEXT,
    "fileName" TEXT,
    "fileSize" INTEGER,
    "requestHash" TEXT,
    "idempotencyKey" TEXT,
    "totalRecords" INTEGER,
    "processedRecords" INTEGER NOT NULL DEFAULT 0,
    "successCount" INTEGER NOT NULL DEFAULT 0,
    "errorCount" INTEGER NOT NULL DEFAULT 0,
    "errorSummary" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "startedAt" TIMESTAMP(3),
    "finishedAt" TIMESTAMP(3),
    "createdById" INTEGER NOT NULL,

    CONSTRAINT "ImportJob_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ImportError" (
    "id" TEXT NOT NULL,
    "jobId" TEXT NOT NULL,
    "recordIndex" INTEGER NOT NULL,
    "recordId" TEXT,
    "errorCode" INTEGER NOT NULL,
    "errorName" TEXT NOT NULL,
    "message" TEXT NOT NULL,
    "field" TEXT,
    "value" JSONB,
    "details" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ImportError_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ExportJob" (
    "id" TEXT NOT NULL,
    "status" "JobStatus" NOT NULL DEFAULT 'queued',
    "resource" "ImportExportResource" NOT NULL,
    "format" "ImportExportFormat" NOT NULL,
    "filters" JSONB,
    "fields" JSONB,
    "totalRecords" INTEGER,
    "processedRecords" INTEGER NOT NULL DEFAULT 0,
    "outputLocation" TEXT,
    "downloadUrl" TEXT,
    "fileSize" INTEGER,
    "expiresAt" TIMESTAMP(3),
    "requestHash" TEXT,
    "idempotencyKey" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "startedAt" TIMESTAMP(3),
    "finishedAt" TIMESTAMP(3),
    "createdById" INTEGER NOT NULL,

    CONSTRAINT "ExportJob_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "ImportJob_createdById_idempotencyKey_resource_key" ON "ImportJob"("createdById", "idempotencyKey", "resource");

-- CreateIndex
CREATE UNIQUE INDEX "ExportJob_createdById_idempotencyKey_resource_key" ON "ExportJob"("createdById", "idempotencyKey", "resource");

-- AddForeignKey
ALTER TABLE "ImportJob" ADD CONSTRAINT "ImportJob_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ImportError" ADD CONSTRAINT "ImportError_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "ImportJob"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ExportJob" ADD CONSTRAINT "ExportJob_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;
