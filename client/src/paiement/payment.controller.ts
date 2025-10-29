// src/payments/payment.controller.ts

import { Controller, Get, Post, Body, Param, HttpException, HttpStatus, NotFoundException, HttpCode } from '@nestjs/common';
import { PaymentService } from './payment.service';
import { CreatePaymentDto } from './create-payment.dto';
import { ObligationResponseDto } from './obligation-response.dto';
import { PaymentResponseDto } from './payment-response.dto';
import { CreateRenewalObligationsDto } from './create-renewal-obligations.dto';

@Controller('payments')
export class PaymentController {
  constructor(private readonly paymentService: PaymentService) {}

// Backend NestJS
@Get('procedures/:id')
async getProcedure(@Param('id') id: number) {
  const procedure = await this.paymentService.getProcedureWithPermis(+id);
  return {
    ...procedure,
    permis: procedure!.permis?.[0] || null, // renvoie directement l'objet
  };
}

@Get('ts-paiement/obligation/:obligationId')
async getTsPaiementByObligation(@Param('obligationId') obligationId: string) {
  try {
    const tsPaiements = await this.paymentService.getTsPaiementByObligationId(parseInt(obligationId));
    return tsPaiements;
  } catch (error) {
    console.error('Error fetching TsPaiement by obligation:', error);
    return [];
  }
}
  // @Post()
  // create(@Body() createPaymentDto: CreatePaymentDto): Promise<PaymentResponseDto> {
  //   return this.paymentService.createPayment(createPaymentDto);
  // }

  @Get('obligations/:permisId')
  getObligations(@Param('permisId') permisId: string): Promise<ObligationResponseDto[]> {
    return this.paymentService.getObligationsForPermis(parseInt(permisId));
  }

  @Get('payments/:obligationId')
  getPayments(@Param('obligationId') obligationId: string): Promise<PaymentResponseDto[]> {
    return this.paymentService.getPaymentsForObligation(parseInt(obligationId));
  }

// payment.controller.ts
// payment.controller.ts

// payment.controller.ts

@Post('initialize/:permisId/:procedureId')
async createInitialObligations(
  @Param('permisId') permisId: string,
  @Param('procedureId') procedureId: string,
  @Body() body: { dateAttribution: string }
) {
  try {
    console.log('Creating initial obligations for permis:', permisId, 'procedure:', procedureId);
    
    const dateAttribution = new Date(body.dateAttribution);
    console.log('Parsed attribution date:', dateAttribution);
    
    const result = await this.paymentService.createInitialDemandeObligations(
      parseInt(permisId),
      parseInt(procedureId),
      dateAttribution
    );
    
    console.log('Initial obligations created successfully:', result);
    return result;
  } catch (error) {
    console.error('Error creating initial obligations:', error);
    throw new HttpException(error.message, HttpStatus.BAD_REQUEST);
  }
}

@Post('renewal/:permisId/:procedureId')
async createRenewalObligations(
  @Param('permisId') permisId: string,
  @Param('procedureId') procedureId: string,
  @Body() body: { 
    renewalStartDate: string;
    renewalDuration: number;
  }
) {
  try {
    console.log('Creating renewal obligations for permis:', permisId, 'procedure:', procedureId);
    
    const renewalStartDate = new Date(body.renewalStartDate);
    console.log('Parsed renewal start date:', renewalStartDate);
    
    const result = await this.paymentService.createRenewalObligations(
      parseInt(permisId),
      parseInt(procedureId),
      renewalStartDate,
      body.renewalDuration
    );
    
    console.log('Renewal obligations created successfully:', result);
    return result;
  } catch (error) {
    console.error('Error creating renewal obligations:', error);
    throw new HttpException(error.message, HttpStatus.BAD_REQUEST);
  }
}

  @Get('obligations')
async getAllObligations(): Promise<ObligationResponseDto[]> {
  return this.paymentService.getAllObligationsWithDetails();
}

@Get('stats')
getGlobalStats() {
  return this.paymentService.getGlobalPaymentSummary();
}

@Post('generate-receipt/:paymentId')
async generateReceipt(@Param('paymentId') paymentId: string) {
  try {
    const result = await this.paymentService.generatePaymentReceipt(parseInt(paymentId));
    return {
      success: true,
      pdfUrl: result.pdfUrl,
      paymentDetails: result.paymentDetails
    };
  } catch (error) {
    console.error('Receipt generation failed:', error);
    throw new HttpException(
      'Failed to generate receipt: ' + error.message,
      HttpStatus.INTERNAL_SERVER_ERROR
    );
  }
}

// payment.controller.ts
@Get('check-obligations/:permisId')
async checkObligationsPaid(@Param('permisId') permisId: string) {
  
  return this.paymentService.checkAllObligationsPaid(+permisId);
}

@Get('permis/:permisId')
async getPermis(@Param('permisId') permisId: string) {
  const permis = await this.paymentService.getPermisWithDetails(parseInt(permisId, 10));
  if (!permis) {
    throw new NotFoundException(`Permis ${permisId} non trouvé`);
  }
  return permis;
}




}