import pino, { Logger } from 'pino';

const nodeEnv = process.env.NODE_ENV || 'development';
const isProduction = nodeEnv === 'production';
const isTest = nodeEnv === 'test';
const prettyEnabled =
  process.env.LOG_PRETTY === 'true' || (!isProduction && !isTest && process.env.LOG_PRETTY !== 'false');

const rootLogger = pino({
  level: process.env.LOG_LEVEL || 'info',
  ...(prettyEnabled
    ? {
        transport: {
          target: 'pino-pretty',
          options: {
            colorize: true,
            translateTime: 'SYS:standard',
            ignore: 'pid,hostname',
            singleLine: true,
          },
        },
      }
    : {}),
});

export function createLogger(bindings?: Record<string, unknown>): Logger {
  if (!bindings) {
    return rootLogger;
  }

  return rootLogger.child(bindings);
}

export default rootLogger;
