import ipaddr from 'ipaddr.js';
import { promises as fs } from 'fs';
import type { Prisma } from '@prisma/client';
import { Request } from 'express';
import HttpException from '../../../models/http-exception.model';
import { EntityType, FileFormat } from './types';

export function isLocalHostname(host: string): boolean {
  return (
    host === 'localhost' ||
    host.endsWith('.localhost') ||
    host.endsWith('.local')
  );
}

export function isPrivateIp(ip: string): boolean {
  if (!ipaddr.isValid(ip)) {
    return true;
  }

  const addr = ipaddr.parse(ip);
  if (isIPv6Address(addr) && addr.isIPv4MappedAddress()) {
    return isPrivateIp(addr.toIPv4Address().toString());
  }

  return addr.range() !== 'unicast';
}

function isIPv6Address(addr: ipaddr.IPv4 | ipaddr.IPv6): addr is ipaddr.IPv6 {
  return addr.kind() === 'ipv6';
}

export function sanitizeValue(value: unknown): unknown {
  if (value === undefined || value === null) {
    return null;
  }

  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.slice(0, 10);
  }

  if (typeof value === 'object') {
    return '[object]';
  }

  return String(value);
}

export async function pathExists(filePath: string): Promise<boolean> {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

export interface AuthenticatedRequest extends Request {
  auth?: { user?: { id?: number } };
}

export function requireUserId(req: AuthenticatedRequest): number {
  const userId = req.auth?.user?.id;
  if (typeof userId !== 'number') {
    throw new HttpException(401, {
      errors: { auth: ['missing authorization credentials'] },
    });
  }
  return userId;
}

export function getIdempotencyKey(req: Request): string | undefined {
  const key = req.get('Idempotency-Key')?.trim();
  return key && key.length > 0 ? key : undefined;
}

export function getQueryParamValue(value: unknown): string | undefined {
  if (typeof value === 'string') {
    return value;
  }
  if (Array.isArray(value) && value.length > 0) {
    return String(value[0]);
  }
  return undefined;
}

export function parseEntityType(value: string | undefined): EntityType {
  if (!value) {
    throw new HttpException(422, {
      errors: { resource: ['resource is required'] },
    });
  }
  const normalized = value.trim().toLowerCase();
  if (
    normalized === 'users' ||
    normalized === 'articles' ||
    normalized === 'comments'
  ) {
    return normalized as EntityType;
  }
  throw new HttpException(422, {
    errors: { resource: ['resource must be one of users, articles, comments'] },
  });
}

export function parseFormat(value?: string): FileFormat {
  if (!value) {
    return 'ndjson';
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === 'ndjson' || normalized === 'json') {
    return normalized as FileFormat;
  }
  throw new HttpException(422, {
    errors: { format: ['format must be json or ndjson'] },
  });
}

export function normalizeFormat(value: string | null): FileFormat {
  if (value === 'ndjson' || value === 'json') {
    return value;
  }
  return 'ndjson';
}

export function parseLimit(
  value: string | undefined,
  maxRecords: number
): number {
  if (!value) {
    return maxRecords;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new HttpException(422, {
      errors: { limit: ['limit must be a positive integer'] },
    });
  }
  if (parsed > maxRecords) {
    throw new HttpException(422, {
      errors: { limit: [`limit must be <= ${maxRecords}`] },
    });
  }
  return parsed;
}

export function parseCursor(value?: string): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new HttpException(422, {
      errors: { cursor: ['cursor must be a positive integer'] },
    });
  }
  return parsed;
}

export function parsePositiveInteger(
  value: string | undefined,
  defaultValue: number,
  maxValue: number,
  fieldName: string
): number {
  if (!value) {
    return defaultValue;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0 || parsed > maxValue) {
    throw new HttpException(422, {
      errors: {
        [fieldName]: [`${fieldName} must be a positive integer <= ${maxValue}`],
      },
    });
  }
  return parsed;
}

export function toJsonObject(
  value: Prisma.JsonValue | null | undefined
): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

export function normalizeJsonValue(
  value: unknown
): Prisma.InputJsonValue | null {
  if (!isObject(value) && !Array.isArray(value)) {
    return null;
  }

  return value as Prisma.InputJsonValue;
}

export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function round(value: number, digits: number): number {
  const factor = Math.pow(10, digits);
  return Math.round(value * factor) / factor;
}
