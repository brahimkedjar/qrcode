import { Module } from '@nestjs/common';
import { PermisModule } from './permis/permis.module';
import { FinanceModule } from './finance/finance.module';
import { HealthController } from './health.controller';
import { SyncController, SyncTablesController } from './sync.controller';
import { ArticleSetsController } from './article-sets/article-sets.controller';
import { ArticleSetsService } from './article-sets/article-sets.service';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';

@Module({
  imports: [PermisModule, FinanceModule],
  controllers: [HealthController, ArticleSetsController, AuthController, SyncController, SyncTablesController],
  providers: [ArticleSetsService, AuthService]
})
export class AppModule {}
