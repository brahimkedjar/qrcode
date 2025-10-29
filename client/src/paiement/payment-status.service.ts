// src/payments/payment-status.service.ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';

@Injectable()
export class PaymentStatusService implements OnModuleInit {
  constructor(private prisma: PrismaService) {}

  onModuleInit() {
    this.checkOverdueObligations();
  }

  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT)
  async checkOverdueObligations() {
    const now = new Date();
    const overdueObligations = await this.prisma.obligationFiscale.findMany({
      where: {
        date_echeance: { lt: now },
        statut: { not: 'Paye' }
      }
    });

    for (const obligation of overdueObligations) {
      await this.prisma.obligationFiscale.update({
        where: { id: obligation.id },
        data: { statut: 'En_retard' }
      });
    }
  }
}