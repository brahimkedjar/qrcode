import { Controller, Get, Param } from '@nestjs/common';
import { AccessService } from './access.service';

@Controller('diagnostics')
export class DiagnosticsController {
  constructor(private readonly access: AccessService) {}

  @Get('access')
  async accessDiagnostics() {
    return this.access.diagnostics();
  }

  @Get('schema/:table')
  async schema(@Param('table') table: string) {
    return this.access.getTableColumns(table);
  }
}

