// src/payments/dto/create-payment.dto.ts

import { PaymentResponseDto } from "./payment-response.dto";

export class CreatePaymentDto {
  obligationId: number;
  amount: number;
  currency: string;
  paymentDate: string;
  paymentMethod: string;
  receiptNumber: string;
  proofUrl?: string;
  status?: string;
}

// src/payments/permis-obligations.dto.ts
export class PermisObligationDto {
  id: number;
  typePaiement: {
    id: number;
    libelle: string;
  };
  amount: number;
  fiscalYear: number;
  dueDate: string;
  status: string;
  payments: PaymentResponseDto[];
  permis: {
    code_permis: string;
    detenteur: {
      nom_societeFR: string;
      registreCommerce: {
        nif: string;
      };
    };
    typePermis: {
      lib_type: string;
      code_type: string;
    };
  };
}

// src/payments/payment-stats.dto.ts
export class PaymentStatsDto {
  totalObligations: number;
  paid: number;
  overdue: number;
  pending: number;
  totalAmount: number;
  paidAmount: number;
  overdueAmount: number;
}