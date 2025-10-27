import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import express from 'express';

dotenv.config();

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { cors: true, bodyParser: false });
  const globalPrefix = process.env.API_PREFIX || 'api';
  app.setGlobalPrefix(globalPrefix);

  app.enableCors({
    origin: ['http://localhost:7000','http://10.16.220.141:7000','http://10.16.220.141:81','http://localhost:5173'],
    credentials: true,
    exposedHeaders: ['set-cookie', 'authorization'],
    allowedHeaders: [
      'Content-Type',
      'Authorization',
      'X-Requested-With',
      'Accept',
      'Origin',
      'Cache-Control',
      'x-user-id',
      'x-user-name'
    ],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS','PATCH','HEAD'],
    preflightContinue: false,
    optionsSuccessStatus: 204,
  });

  const port = Number(process.env.PORT || 7000);
  // Configure body limits for large base64 payloads (PDFs)
  try {
    const expressApp = (app as any).getHttpAdapter().getInstance();
    expressApp.use(express.json({ limit: '100mb' }));
    expressApp.use(express.urlencoded({ extended: true, limit: '100mb' }));
    // Serve saved PDF templates statically; resolve relative to server/dist
    const staticRoot = path.resolve(__dirname, '..', '..', 'client', 'permis_templates');
    if (!fs.existsSync(staticRoot)) fs.mkdirSync(staticRoot, { recursive: true });
    expressApp.use('/static/permis_templates', express.static(staticRoot));
  } catch {}
  await app.listen(port);
  // eslint-disable-next-line no-console
  console.log(`Server listening on http://localhost:${port}/${globalPrefix}`);
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Failed to bootstrap server', err);
  process.exit(1);
});
