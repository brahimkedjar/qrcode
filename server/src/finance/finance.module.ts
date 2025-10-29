import { Module } from '@nestjs/common';
import { FinanceService } from './finance.service';
import { FinanceController } from './finance.controller';
import { AccessService } from '../permis/access.service';

@Module({
  providers: [FinanceService, AccessService],
  controllers: [FinanceController],
})
export class FinanceModule {}

