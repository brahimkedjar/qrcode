// src/payments/payment.module.ts
import { Module } from '@nestjs/common';
import { PrismaService } from 'src/prisma/prisma.service';
import { PaymentController } from './payment.controller';
import { PaymentService } from './payment.service';
import { PaymentStatusService } from './payment-status.service';
import { ScheduleModule } from '@nestjs/schedule';

@Module({
  imports: [ScheduleModule.forRoot()],
  controllers: [PaymentController],
  providers: [PaymentService, PrismaService, PaymentStatusService]
})
export class PaymentModule {}