import { Module } from '@nestjs/common';
import { PermisController } from './permis.controller';
import { PermisService } from './permis.service';
import { AccessService } from './access.service';
import { DiagnosticsController } from './diagnostics.controller';
import { TemplatesController } from './templates.controller';
import { TemplatesService } from './templates.service';
import { DocumentController } from './document.controller';

@Module({
  controllers: [PermisController, DiagnosticsController, TemplatesController, DocumentController],
  providers: [PermisService, AccessService, TemplatesService]
})
export class PermisModule {}
