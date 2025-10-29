import { PaymentResponseDto } from "./payment-response.dto";

export interface ObligationResponseDto {
  id: number;
  details_calcul?: string | null;
  typePaiement: {
    id: number;
    libelle: string;
  };
  amount: number;
  fiscalYear: number;
  dueDate: string;
  status: string;
  payments: PaymentResponseDto[];
  tsPaiements?: {
    id_tsPaiement: number;
    datePerDebut: string;
    datePerFin: string;
    surfaceMin: number;
    surfaceMax: number;
  }[];
  permis: {
    id: number;
    code_permis: string;
    detenteur?: {
      id?: number;
      nom_societeFR: string;
      registreCommerce?: {
        nif: string;
      };
    } | null;
  };
}