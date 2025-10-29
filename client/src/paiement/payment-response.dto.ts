// src/payments/dto/payment-response.dto.ts

export class PaymentResponseDto {
  id: number;
  amount: number;
  currency: string;
  paymentDate: Date;
  paymentMethod: string;
  receiptNumber: string;
  status: string;
  proofUrl?: string;
}