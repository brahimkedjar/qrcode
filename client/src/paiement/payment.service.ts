// payment.service.ts

import { HttpException, HttpStatus, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { CreatePaymentDto } from './create-payment.dto';
import { ObligationResponseDto } from './obligation-response.dto';
import { PaymentResponseDto } from './payment-response.dto';
import * as fs from 'fs';
import * as path from 'path';
import * as puppeteer from 'puppeteer';
import { EnumStatutPaiement } from '@prisma/client';
interface TaxeSuperficiaireCalculation {
  droitFixe: number;
  droitProportionnel: number;
  superficie: number;
  mois: number;
  taxeAnnuelle: number;
  taxeAPayer: number;
  periodeType: string;
}

function format1Decimal(value: number): number {
  return parseFloat(value.toFixed(1));
}

@Injectable()
export class PaymentService {
  constructor(private prisma: PrismaService) {}

async createInitialDemandeObligations(
  permisId: number,
  procedureId: number,
  dateAttribution: Date
) {
  console.log('=== START: createInitialDemandeObligations ===', { 
    permisId, 
    procedureId, 
    dateAttribution
  });

  // Validate input
  if (!permisId || !procedureId || isNaN(dateAttribution.getTime())) {
    throw new HttpException('Invalid input parameters', HttpStatus.BAD_REQUEST);
  }

  const permis = await this.prisma.permis.findUnique({
    where: { id: permisId },
    include: { typePermis: { include: { taxe: true } } },
  });

  if (!permis) {
    throw new HttpException('Permis not found', HttpStatus.NOT_FOUND);
  }

  // Check if initial obligations already exist
  const existingInitialObligations = await this.prisma.obligationFiscale.findMany({
    where: { 
      id_permis: permisId,
      // We can identify initial obligations by their earlier years or specific pattern
      annee_fiscale: {
        gte: dateAttribution.getFullYear(),
        lte: dateAttribution.getFullYear() + permis.typePermis.duree_initiale - 1
      }
    },
    include: { typePaiement: true },
  });

  console.log('Existing initial obligations:', existingInitialObligations.length);

  if (existingInitialObligations.length > 0) {
    console.log('Initial obligations already exist, updating if needed');
    
    // Update any surface tax obligations with missing details
    const obligationsWithMissingDetails = existingInitialObligations.filter(
      ob => !ob.details_calcul && ob.typePaiement.libelle === 'Taxe superficiaire'
    );
    
    if (obligationsWithMissingDetails.length > 0) {
      console.log('Updating initial obligations with missing details:', obligationsWithMissingDetails.length);
      for (const obligation of obligationsWithMissingDetails) {
        await this.updateSurfaceTaxObligationDetails(obligation.id, permisId, dateAttribution, false);
      }
    }
    return existingInitialObligations;
  }

  // Calculate fees for initial demande
  const [establishmentFee, attributionProduct] = await Promise.all([
    this.calculateEstablishmentFee(permisId, procedureId),
    this.calculateAttributionProduct(permisId, procedureId),
  ]);

  // Create non-surface tax obligations for initial demande
  const nonSurfaceObligations = [
    {
      id_typePaiement: 1, // Attribution product
      id_permis: permisId,
      annee_fiscale: dateAttribution.getFullYear(),
      montant_attendu: format1Decimal(attributionProduct),
      date_echeance: new Date(dateAttribution.getFullYear(), dateAttribution.getMonth() + 1, dateAttribution.getDate()),
      statut: EnumStatutPaiement.A_payer,
    },
    {
      id_typePaiement: 2, // Establishment fee
      id_permis: permisId,
      annee_fiscale: dateAttribution.getFullYear(),
      montant_attendu: format1Decimal(establishmentFee),
      date_echeance: new Date(dateAttribution.getFullYear(), dateAttribution.getMonth() + 1, dateAttribution.getDate()),
      statut: EnumStatutPaiement.A_payer,
    }
  ];

  // Create surface tax obligations for initial demande
  const surfaceTaxObligations = await this.createSurfaceTaxObligations(
    permisId, 
    dateAttribution,
    false, // isRenewal = false
    permis.typePermis.duree_initiale // Use initial duration
  );

  // Combine all obligations
  const obligations = [...nonSurfaceObligations, ...surfaceTaxObligations];

  console.log('Creating initial demande obligations:', obligations.length);

  // Create obligations in transaction
  const createdObligations = await this.prisma.$transaction(async (tx) => {
    // Create new obligations
    const created = await tx.obligationFiscale.createMany({
      data: obligations,
    });

    console.log('Initial obligations created:', created.count);

    // Fetch created surface tax obligations
    const createdSurfaceObligations = await tx.obligationFiscale.findMany({
      where: {
        id_permis: permisId,
        typePaiement: { libelle: 'Taxe superficiaire' },
      },
      include: {
        typePaiement: true,
        permis: { include: { typePermis: true } },
      },
    });

    console.log('Created surface tax obligations for initial demande:', createdSurfaceObligations.length);

    // Create TsPaiement records
    for (const obligation of createdSurfaceObligations) {
      console.log('Processing TsPaiement for initial obligation:', obligation.id);
      await this.createTsPaiementForObligation(
        obligation, 
        dateAttribution, 
        tx,
        false, // isRenewal = false
        permis.typePermis.duree_initiale
      );
    }

    return tx.obligationFiscale.findMany({
      where: { id_permis: permisId },
      include: {
        typePaiement: true,
        tsPaiements: true,
      },
    });
  });

  console.log('All initial obligations created:', createdObligations.length);
  return createdObligations;
}

// For renewal obligations
async createRenewalObligations(
  permisId: number,
  procedureId: number,
  renewalStartDate: Date,
  renewalDuration: number
) {
  console.log('=== START: createRenewalObligations ===', { 
    permisId, 
    procedureId, 
    renewalStartDate,
    renewalDuration
  });

  // Validate input
  if (!permisId || !procedureId || isNaN(renewalStartDate.getTime()) || !renewalDuration) {
    throw new HttpException('Invalid input parameters', HttpStatus.BAD_REQUEST);
  }

  const permis = await this.prisma.permis.findUnique({
    where: { id: permisId },
    include: { typePermis: { include: { taxe: true } } },
  });

  if (!permis) {
    throw new HttpException('Permis not found', HttpStatus.NOT_FOUND);
  }

  // Calculate renewal period
  const renewalStartYear = renewalStartDate.getFullYear();
  const renewalEndYear = renewalStartYear + Math.ceil(renewalDuration) - 1;

  // Check if renewal obligations already exist for this period
  const existingRenewalObligations = await this.prisma.obligationFiscale.findMany({
    where: { 
      id_permis: permisId,
      annee_fiscale: {
        gte: renewalStartYear,
        lte: renewalEndYear
      }
    },
    include: { typePaiement: true },
  });

  console.log('Existing renewal obligations:', existingRenewalObligations.length);

  if (existingRenewalObligations.length > 0) {
    console.log('Renewal obligations already exist for this period, updating if needed');
    
    // Update any surface tax obligations
    const surfaceTaxObligations = existingRenewalObligations.filter(
      ob => ob.typePaiement.libelle === 'Taxe superficiaire'
    );
    
    for (const obligation of surfaceTaxObligations) {
      await this.updateSurfaceTaxObligationDetails(
        obligation.id, 
        permisId, 
        renewalStartDate,
        true, // isRenewal = true
        renewalDuration
      );
    }
    return existingRenewalObligations;
  }

  // Calculate establishment fee for renewal
  const establishmentFee = await this.calculateEstablishmentFee(permisId, procedureId);

  // Create non-surface tax obligations for renewal
  const nonSurfaceObligations = [{
    id_typePaiement: 2, // Establishment fee
    id_permis: permisId,
    annee_fiscale: renewalStartYear,
    montant_attendu: format1Decimal(establishmentFee),
    date_echeance: new Date(renewalStartYear, renewalStartDate.getMonth() + 1, renewalStartDate.getDate()),
    statut: EnumStatutPaiement.A_payer,
  }];

  // Create surface tax obligations for renewal
  const surfaceTaxObligations = await this.createSurfaceTaxObligations(
    permisId, 
    renewalStartDate,
    true, // isRenewal = true
    renewalDuration
  );

  // Combine all obligations
  const obligations = [...nonSurfaceObligations, ...surfaceTaxObligations];

  console.log('Creating renewal obligations:', obligations.length);

  // Create obligations in transaction
  const createdObligations = await this.prisma.$transaction(async (tx) => {
    // Create new obligations
    const created = await tx.obligationFiscale.createMany({
      data: obligations,
    });

    console.log('Renewal obligations created:', created.count);

    // Fetch created surface tax obligations
    const createdSurfaceObligations = await tx.obligationFiscale.findMany({
      where: {
        id_permis: permisId,
        typePaiement: { libelle: 'Taxe superficiaire' },
        annee_fiscale: {
          gte: renewalStartYear,
          lte: renewalEndYear
        }
      },
      include: {
        typePaiement: true,
        permis: { include: { typePermis: true } },
      },
    });

    console.log('Created surface tax obligations for renewal:', createdSurfaceObligations.length);

    // Create TsPaiement records
    for (const obligation of createdSurfaceObligations) {
      console.log('Processing TsPaiement for renewal obligation:', obligation.id);
      await this.createTsPaiementForObligation(
        obligation, 
        renewalStartDate, 
        tx,
        true, // isRenewal = true
        renewalDuration
      );
    }

    return tx.obligationFiscale.findMany({
      where: { 
        id_permis: permisId,
        annee_fiscale: {
          gte: renewalStartYear,
          lte: renewalEndYear
        }
      },
      include: {
        typePaiement: true,
        tsPaiements: true,
      },
    });
  });

  console.log('All renewal obligations created:', createdObligations.length);
  return createdObligations;
}


  async createTsPaiementForObligation(
  obligation: any, 
  dateAttribution: Date, 
  tx: any,
  isRenewal: boolean = false,
  renewalDuration?: number
): Promise<void> {
  console.log('=== START: createTsPaiementForObligation ===', { 
    obligationId: obligation.id, 
    dateAttribution,
    isRenewal,
    renewalDuration 
  });

  try {
    // Validate obligation
    if (!obligation) {
      throw new Error(`Obligation ${obligation.id} is undefined`);
    }

    if (obligation.typePaiement.libelle !== 'Taxe superficiaire') {
      console.log('Not a surface tax obligation, skipping TsPaiement creation');
      return;
    }

    // Check if TsPaiement already exists for this obligation
    const existingTsPaiement = await tx.tsPaiement.findFirst({
      where: { id_obligation: obligation.id },
    });

    if (existingTsPaiement) {
      console.log(`TsPaiement already exists for obligation ${obligation.id}`);
      return;
    }

    const obligationYear = obligation.annee_fiscale;
    const attributionYear = dateAttribution.getFullYear();
    const attributionMonth = dateAttribution.getMonth() + 1;
    
    // For renewals, use the renewal duration, otherwise use initial duration
    const permitDuration = isRenewal && renewalDuration 
      ? renewalDuration 
      : obligation.permis.typePermis.duree_initiale;

    console.log('Parameters:', { obligationYear, attributionYear, permitDuration, isRenewal });

    let datePerDebut: Date;
    let datePerFin: Date;

    if (isRenewal) {
      // For renewals, the period starts from the attribution date
      datePerDebut = new Date(dateAttribution);
      
      if (obligationYear === attributionYear) {
        // First year of renewal: From attribution date to end of year
        datePerFin = new Date(Date.UTC(obligationYear, 11, 31, 23, 59, 59, 999));
      } else {
        // Subsequent years: Full calendar year
        datePerDebut = new Date(Date.UTC(obligationYear, 0, 1));
        datePerFin = new Date(Date.UTC(obligationYear, 11, 31, 23, 59, 59, 999));
      }
    } else {
      // For initial permits
      if (obligationYear === attributionYear) {
        // First year: From attribution date to December 31st
        datePerDebut = new Date(dateAttribution);
        datePerFin = new Date(Date.UTC(obligationYear, 11, 31, 23, 59, 59, 999));
      } else if (obligationYear < attributionYear + permitDuration - 1) {
        // Middle years: January 1st to December 31st
        datePerDebut = new Date(Date.UTC(obligationYear, 0, 1));
        datePerFin = new Date(Date.UTC(obligationYear, 11, 31, 23, 59, 59, 999));
      } else {
        // Final year: January 1st to attribution date + duration - 1 day
        datePerDebut = new Date(Date.UTC(obligationYear, 0, 1));
        datePerFin = new Date(dateAttribution);
        datePerFin.setUTCFullYear(datePerFin.getUTCFullYear() + permitDuration);
        datePerFin.setUTCDate(datePerFin.getUTCDate() - 1);
        datePerFin.setUTCHours(23, 59, 59, 999);
      }
    }
    // Get superficie from obligation details or permis
    let superficie = 0;
    try {
      const calculationDetails = JSON.parse(obligation.details_calcul || '{}');
      superficie = calculationDetails.superficie || obligation.permis.superficie || 0;
    } catch (error) {
      console.warn('Failed to parse details_calcul, using permis superficie:', error);
      superficie = obligation.permis.superficie || 0;
    }

    console.log('TsPaiement data:', {
      obligationId: obligation.id,
      datePerDebut: datePerDebut.toISOString(),
      datePerFin: datePerFin.toISOString(),
      superficie,
      isRenewal
    });

    // Create TsPaiement record
    await tx.tsPaiement.create({
      data: {
        datePerDebut,
        datePerFin,
        surfaceMin: superficie,
        surfaceMax: superficie,
        id_obligation: obligation.id,
      },
    });

    console.log(`TsPaiement created successfully for obligation ${obligation.id}`);
  } catch (error) {
    console.error('Error creating TsPaiement:', error);
    throw new HttpException(
      `Failed to create TsPaiement for obligation ${obligation.id}: ${error.message}`,
      HttpStatus.INTERNAL_SERVER_ERROR
    );
  }

  console.log('=== END: createTsPaiementForObligation ===');
}

  async updateSurfaceTaxObligationDetails(
  obligationId: number, 
  permisId: number, 
  dateAttribution: Date,
  isRenewal: boolean = false,
  renewalDuration?: number
) {
  const obligation = await this.prisma.obligationFiscale.findUnique({
    where: { id: obligationId },
    include: {
      permis: { include: { typePermis: { include: { taxe: true } } } },
      typePaiement: true,
    },
  });

  if (!obligation || obligation.typePaiement.libelle !== 'Taxe superficiaire') {
    return;
  }

  const attributionYear = dateAttribution.getFullYear();
  const attributionMonth = dateAttribution.getMonth() + 1;
  const obligationYear = obligation.annee_fiscale;
  
  let periodeType = this.getPeriodeType(obligation.permis.nombre_renouvellements);
  let numberOfMonths = 12;

  if (isRenewal) {
    // For renewals, use appropriate period type
    periodeType = obligation.permis.nombre_renouvellements === 0 
      ? 'premier_renouvellement' 
      : 'autre_renouvellement';
    
    if (obligationYear === attributionYear) {
      // First year of renewal: calculate partial months
      numberOfMonths = 12 - attributionMonth + 1;
    } else if (renewalDuration && obligationYear === attributionYear + Math.ceil(renewalDuration) - 1) {
      // Last year of renewal: calculate partial months if needed
      const renewalEndDate = new Date(dateAttribution);
      renewalEndDate.setFullYear(dateAttribution.getFullYear() + renewalDuration);
      
      const startOfYear = new Date(obligationYear, 0, 1);
      const diffTime = renewalEndDate.getTime() - startOfYear.getTime();
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
      numberOfMonths = diffDays / 30.44; // Average month length
      numberOfMonths = Math.ceil(numberOfMonths); // Round up to whole months
    }
  } else if (obligation.annee_fiscale === attributionYear) {
    // For initial obligations in the first year
    numberOfMonths = 12 - attributionMonth + 1;
  }

  const taxCalculation = this.calculateSurfaceTaxForPeriod(
    obligation.permis.typePermis.taxe,
    obligation.permis.superficie || 0,
    periodeType,
    numberOfMonths
  );

  await this.prisma.obligationFiscale.update({
    where: { id: obligationId },
    data: {
      montant_attendu: format1Decimal(taxCalculation.taxeAPayer),
      details_calcul: JSON.stringify({
        ...taxCalculation,
        isRenewal,
        renewalDuration
      }),
    },
  });
}

 async createSurfaceTaxObligations(
  permisId: number, 
  dateAttribution: Date, 
  isRenewal: boolean = false,
  renewalDuration?: number
) {
  console.log('=== START: createSurfaceTaxObligations ===', { 
    permisId, 
    dateAttribution,
    isRenewal,
    renewalDuration 
  });

  const permis = await this.prisma.permis.findUnique({
    where: { id: permisId },
    include: {
      typePermis: { include: { taxe: true } },
    },
  });

  if (!permis) {
    throw new HttpException('Permis not found', HttpStatus.NOT_FOUND);
  }

  const requiresSurfaceTax = ['PEM', 'PXM', 'PEC', 'PXC'].includes(permis.typePermis.code_type);
  if (!requiresSurfaceTax) {
    console.log('No surface tax required for permit type:', permis.typePermis.code_type);
    return [];
  }

  const attributionYear = dateAttribution.getFullYear();
  const attributionMonth = dateAttribution.getMonth() + 1; // JavaScript months are 0-indexed
  
  // For renewals, use the provided renewal duration instead of initial duration
  const dureeTotale = isRenewal && renewalDuration 
    ? renewalDuration 
    : permis.typePermis.duree_initiale;
  
  // For renewals, use the appropriate period type based on nombre_renouvellements
  const periodeType = isRenewal 
    ? permis.nombre_renouvellements === 0 ? 'premier_renouvellement' : 'autre_renouvellement'
    : this.getPeriodeType(permis.nombre_renouvellements);

  const obligations: any[] = [];

  console.log('Creating surface tax obligations for:', {
    isRenewal,
    dureeTotale,
    periodeType,
    attributionYear,
    attributionMonth,
    nombre_renouvellements: permis.nombre_renouvellements
  });

  for (let yearOffset = 0; yearOffset < dureeTotale; yearOffset++) {
    const currentObligationYear = attributionYear + yearOffset;
    let numberOfMonths = 12;
    
    if (yearOffset === 0) {
      // First year calculation - different for initial vs renewal
      if (isRenewal) {
        // For renewals: from attribution date to end of year
        numberOfMonths = 12 - attributionMonth + 1;
      } else {
        // For initial permits: from attribution date to end of year
        // Calculate exact months including partial months
        const attributionDate = new Date(dateAttribution);
        const endOfYear = new Date(attributionYear, 11, 31); // December 31st
        
        // Calculate difference in months (including partial months)
        const diffTime = Math.abs(endOfYear.getTime() - attributionDate.getTime());
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        numberOfMonths = diffDays / 30.44; // Average month length
        numberOfMonths = Math.ceil(numberOfMonths * 10) / 10; // Round to 1 decimal
      }
    } else if (yearOffset === dureeTotale - 1 && !isRenewal) {
      // Last year of initial permit: from start of year to attribution date + duration - 1 day
      const finalYearStart = new Date(currentObligationYear, 0, 1);
      const permitEndDate = new Date(dateAttribution);
      permitEndDate.setFullYear(permitEndDate.getFullYear() + dureeTotale);
      permitEndDate.setDate(permitEndDate.getDate() - 1); // -1 day to get the last day
        
      const diffTime = Math.abs(permitEndDate.getTime() - finalYearStart.getTime());
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
      numberOfMonths = diffDays / 30.44; // Average month length
      numberOfMonths = Math.ceil(numberOfMonths * 10) / 10; // Round to 1 decimal
    }

    console.log(`Year ${currentObligationYear}: ${numberOfMonths} months`);

    const taxCalculation = this.calculateSurfaceTaxForPeriod(
      permis.typePermis.taxe,
      permis.superficie || 0,
      periodeType,
      numberOfMonths
    );

    const calculationDetails = JSON.stringify({
      droitFixe: Number(taxCalculation.droitFixe),
      droitProportionnel: Number(taxCalculation.droitProportionnel),
      superficie: Number(taxCalculation.superficie),
      mois: Number(taxCalculation.mois),
      taxeAnnuelle: Number(taxCalculation.taxeAnnuelle),
      taxeAPayer: Number(taxCalculation.taxeAPayer),
      periodeType: taxCalculation.periodeType,
      isRenewal: isRenewal,
      renewalDuration: renewalDuration
    });

    obligations.push({
      id_typePaiement: 3,
      id_permis: permisId,
      annee_fiscale: currentObligationYear,
      montant_attendu: format1Decimal(taxCalculation.taxeAPayer),
      date_echeance: new Date(currentObligationYear, 11, 31),
      statut: EnumStatutPaiement.A_payer,
      details_calcul: calculationDetails,
    });
  }

  console.log('Surface tax obligations to create:', obligations.length);
  return obligations;
}

  private getPeriodeType(nombreRenouvellements: number): 'initial' | 'premier_renouvellement' | 'autre_renouvellement' {
    if (nombreRenouvellements === 0) {
      return 'initial';
    } else if (nombreRenouvellements === 1) {
      return 'premier_renouvellement';
    } else {
      return 'autre_renouvellement';
    }
  }

  private calculateSurfaceTaxForPeriod(
    taxe: any,
    superficie: number,
    periodeType: 'initial' | 'premier_renouvellement' | 'autre_renouvellement',
    numberOfMonths: number
  ): TaxeSuperficiaireCalculation {
    let droitProportionnel = 0;

    switch (periodeType) {
      case 'initial':
        droitProportionnel = taxe.periode_initiale;
        break;
      case 'premier_renouvellement':
        droitProportionnel = taxe.premier_renouv;
        break;
      case 'autre_renouvellement':
        droitProportionnel = taxe.autre_renouv;
        break;
    }

    const taxeAnnuelle = taxe.droit_fixe + (droitProportionnel * superficie);
    const taxeMensuelle = taxeAnnuelle / 12;
    const taxeAPayer = taxeMensuelle * numberOfMonths;

    return {
      droitFixe: taxe.droit_fixe,
      droitProportionnel,
      superficie,
      mois: numberOfMonths,
      taxeAnnuelle,
      taxeAPayer,
      periodeType,
    };
  }

  async calculateEstablishmentFee(permisId: number, procedureId: number) {
    const permis = await this.prisma.permis.findUnique({
      where: { id: permisId },
      include: { typePermis: true },
    });

    if (!permis) throw new HttpException('Permis not found', HttpStatus.NOT_FOUND);

    const demande = await this.prisma.demande.findFirst({
      where: { id_proc: procedureId },
      include: { typeProcedure: true },
    });

    if (!demande) throw new HttpException('Demande not found for this procedure', HttpStatus.NOT_FOUND);

    const barem = await this.prisma.baremProduitetDroit.findFirst({
      where: {
        typePermisId: permis.typePermis.id,
        typeProcedureId: demande.typeProcedure!.id,
      },
    });

    if (!barem) {
      throw new HttpException(
        `Barem not found for TypePermis ${permis.typePermis.id} and TypeProcedure ${demande.typeProcedure!.id}`,
        HttpStatus.NOT_FOUND
      );
    }

    return barem.montant_droit_etab;
  }

  async calculateAttributionProduct(permisId: number, procedureId: number) {
    const permis = await this.prisma.permis.findUnique({
      where: { id: permisId },
      include: { typePermis: true },
    });

    if (!permis) throw new HttpException('Permis not found', HttpStatus.NOT_FOUND);

    const demande = await this.prisma.demande.findFirst({
      where: { id_proc: procedureId },
      include: { typeProcedure: true },
    });

    if (!demande) throw new HttpException('Demande not found for this procedure', HttpStatus.NOT_FOUND);

    const barem = await this.prisma.baremProduitetDroit.findFirst({
      where: {
        typePermisId: permis.typePermis.id,
        typeProcedureId: demande.typeProcedure!.id,
      },
    });

    if (!barem) {
      throw new HttpException(
        `Barem not found for TypePermis ${permis.typePermis.id} and TypeProcedure ${demande.typeProcedure!.id}`,
        HttpStatus.NOT_FOUND
      );
    }

    return barem.produit_attribution;
  }

  async getProcedureWithPermis(procedureId: number) {
    return this.prisma.procedure.findUnique({
      where: { id_proc: procedureId },
      include: {
        permis: {
          include: {
            typePermis: true,
          },
        },
      },
    });
  }

  async getObligationsForPermis(permisId: number): Promise<ObligationResponseDto[]> {
    const obligations = await this.prisma.obligationFiscale.findMany({
      where: { id_permis: permisId },
      include: {
        typePaiement: true,
        paiements: true,
        tsPaiements: true,
      },
      orderBy: { date_echeance: 'asc' },
    });

    return obligations.map(obligation => this.mapToObligationResponseDto(obligation));
  }

  async getPaymentsForObligation(obligationId: number): Promise<PaymentResponseDto[]> {
    const payments = await this.prisma.paiement.findMany({
      where: { id_obligation: obligationId },
      orderBy: { date_paiement: 'desc' },
    });

    return payments.map(payment => this.mapToPaymentResponseDto(payment));
  }

  async getTsPaiementByObligationId(obligationId: number) {
    return this.prisma.tsPaiement.findMany({
      where: { id_obligation: obligationId },
      include: {
        obligation: {
          include: {
            typePaiement: true,
          },
        },
      },
      orderBy: { datePerDebut: 'asc' },
    });
  }

  async getAllObligationsWithDetails(): Promise<ObligationResponseDto[]> {
    const obligations = await this.prisma.obligationFiscale.findMany({
      include: {
        typePaiement: true,
        paiements: true,
        tsPaiements: true,
        permis: {
          include: {
            detenteur: { include: { registreCommerce: true } },
            typePermis: true,
          },
        },
      },
      orderBy: { date_echeance: 'asc' },
    });

    return obligations.map(obligation => this.mapToObligationResponseDto(obligation));
  }

  async getGlobalPaymentSummary() {
    const obligations = await this.prisma.obligationFiscale.findMany({
      include: { paiements: true, typePaiement: true },
    });

    const now = new Date();
    const summary = {
      totalDue: 0,
      totalPaid: 0,
      overdueAmount: 0,
      pendingCount: 0,
      totalObligations: obligations.length,
      paidObligations: 0,
      overdueObligations: 0,
    };

    obligations.forEach(obligation => {
      const totalPaid = obligation.paiements.reduce(
        (sum, payment) => sum + payment.montant_paye,
        0
      );

      summary.totalDue += obligation.montant_attendu;
      summary.totalPaid += totalPaid;

      if (totalPaid >= obligation.montant_attendu) {
        summary.paidObligations++;
      } else if (new Date(obligation.date_echeance) < now) {
        summary.overdueObligations++;
        summary.overdueAmount += (obligation.montant_attendu - totalPaid);
      } else {
        summary.pendingCount++;
      }
    });

    return summary;
  }

  private mapToPaymentResponseDto(payment: any): PaymentResponseDto {
    return {
      id: payment.id,
      amount: payment.montant_paye,
      currency: payment.devise,
      paymentDate: payment.date_paiement,
      paymentMethod: payment.mode_paiement,
      receiptNumber: payment.num_quittance,
      status: payment.etat_paiement,
      proofUrl: payment.justificatif_url,
    };
  }

  private mapToObligationResponseDto(obligation: any): ObligationResponseDto {
    return {
      id: obligation.id,
      typePaiement: {
        id: obligation.typePaiement.id,
        libelle: obligation.typePaiement.libelle,
      },
      amount: obligation.montant_attendu,
      fiscalYear: obligation.annee_fiscale,
      dueDate: obligation.date_echeance.toISOString(),
      status: obligation.statut,
      payments: obligation.paiements?.map((p: any) => this.mapToPaymentResponseDto(p)) || [],
      details_calcul: obligation.details_calcul,
      tsPaiements: obligation.tsPaiements?.map((ts: any) => ({
        id_tsPaiement: ts.id_tsPaiement,
        datePerDebut: ts.datePerDebut.toISOString(),
        datePerFin: ts.datePerFin.toISOString(),
        surfaceMin: ts.surfaceMin,
        surfaceMax: ts.surfaceMax,
      })) || [],
      permis: {
        id: obligation.permis?.id,
        code_permis: obligation.permis?.code_permis || '',
        detenteur: obligation.permis?.detenteur
          ? {
              id: obligation.permis.detenteur.id_detenteur,
              nom_societeFR: obligation.permis.detenteur.nom_societeFR,
              registreCommerce: obligation.permis.detenteur.registreCommerce
                ? { nif: obligation.permis.detenteur.registreCommerce.nif }
                : undefined,
            }
          : null,
      },
    };
  }

  async getPermisWithDetails(permisId: number) {
    return this.prisma.permis.findUnique({
      where: { id: permisId },
      include: {
        typePermis: { include: { taxe: true } },
        detenteur: true,
        commune: true,
        statut: true,
        procedures: {
          include: {
            demandes: {
              include: {
                typeProcedure: true,
                typePermis: true,
                expertMinier: true,
                wilaya: true,
                daira: true,
                commune: true,
              },
            },
            ProcedureEtape: true,
            ProcedurePhase: true,
          },
        },
        ObligationFiscale: {
          include: {
            typePaiement: true,
            tsPaiements: true,
          },
        },
        CahierCharge: true,
        RapportActivite: true,
      },
    });
  }
  async checkAllObligationsPaid(permisId: number): Promise<{
  isPaid: boolean;
  missing: { libelle: string; montantRestant: number }[];
}> {
  const obligations = await this.getObligationsForPermis(permisId);

  const missing = obligations
    .filter(o => o.status !== 'Payé')
    .map(o => {
      const montantPayé = o.payments.reduce((sum, p) => sum + p.amount, 0);
      return {
        libelle: o.typePaiement.libelle,
        montantRestant: o.amount - montantPayé,
      };
    });

  return {
    isPaid: missing.length === 0,
    missing,
  };
}

async generatePaymentReceipt(paymentId: number) {
  try {

    const payment = await this.prisma.paiement.findUnique({
      where: { id: paymentId },
      include: {
        obligation: {
          include: {
            typePaiement: true,
            permis: {
              include: {
                detenteur: true,
              },
            },
            paiements: true, 
          },
        },
      },
    });

    if (!payment) {
      throw new NotFoundException(`Payment with ID ${paymentId} not found`);
    }

    const obligation = payment.obligation;

    const receiptsDir = path.resolve(process.cwd(), 'server/public/receipts');
    if (!fs.existsSync(receiptsDir)) {
      fs.mkdirSync(receiptsDir, { recursive: true });
    }

    const outputPath = path.join(receiptsDir, `${paymentId}.pdf`);

    const htmlContent = this.buildReceiptHtml({
      obligation,
      payment, // current payment
      payments: obligation?.paiements || [] 
    });

    
try {
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox'],
  });

  const page = await browser.newPage();
  await page.setContent(htmlContent, { waitUntil: 'networkidle0' });

  await page.pdf({
    path: outputPath,
    format: 'A4',
    printBackground: true,
  });

  await browser.close();
} catch (error) {
  console.error('🛑 Puppeteer PDF generation failed:', error);
  throw error; // Optional: re-throw to let NestJS return 500
}

    return {
      pdfUrl: `http://localhost:3001/receipts/${paymentId}.pdf`,
      paymentDetails: payment,
    };
  } catch (error: any) {
    throw new Error(`Failed to generate receipt: ${error.message}`);
  }
}

    private buildReceiptHtml(data: {
  obligation: any;
  payment: any;
  payments: any[];
}): string {
  const { obligation, payment, payments } = data;

  const detenteurName = obligation?.permis?.detenteur?.nom_societeFR || 'N/A';
  const totalPaid = payments.reduce((sum, p) => sum + (p.montant_paye || 0), 0);
  const type = obligation?.typePaiement?.libelle || 'N/A';

  const paymentRows = payments.map(p => `
    <tr>
      <td>${new Date(p.date_paiement).toLocaleDateString()}</td>
      <td>${(p.montant_paye || 0).toLocaleString()} DZD</td>
      <td>${p.mode_paiement || 'N/A'}</td>
      <td>${p.num_quittance || 'N/A'}</td>
      <td>Validé</td>
    </tr>
  `).join('');

  return `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <style>
        body { font-family: Arial; padding: 40px; }
        h1 { text-align: center; color: #4CAF50; }
        .label { font-weight: bold; color: #555; }
        .section { margin-bottom: 16px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 8px; border: 1px solid #ccc; text-align: center; }
        .footer { margin-top: 40px; text-align: center; font-size: 12px; color: #888; }
      </style>
    </head>
    <body>
      <h1>Reçu de Paiement</h1>

      <div class="section"><span class="label">Détenteur:</span> ${detenteurName}</div>
      <div class="section"><span class="label">Type de droit:</span> ${type}</div>
      <div class="section"><span class="label">Montant total payé:</span> ${totalPaid.toLocaleString()} DZD</div>

      <h2>Historique des Paiements</h2>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Montant</th>
            <th>Méthode</th>
            <th>Quittance</th>
            <th>Statut</th>
          </tr>
        </thead>
        <tbody>
          ${paymentRows}
        </tbody>
      </table>

      <div class="footer">Ce reçu a été généré automatiquement. Merci de votre paiement.</div>
    </body>
    </html>
  `;
}


private async updateObligationStatus(obligationId: number) {
  const obligation = await this.prisma.obligationFiscale.findUnique({
    where: { id: obligationId },
    include: { paiements: true }
  });

  if (!obligation) return;

  const totalPaid = obligation.paiements.reduce(
    (sum, payment) => sum + payment.montant_paye, 0
  );

  let newStatus = obligation.statut;
  
  if (totalPaid >= obligation.montant_attendu) {
    newStatus = 'Paye';
  } else if (new Date() > obligation.date_echeance) {
    newStatus = 'En_retard';
  } else {
    newStatus = EnumStatutPaiement.Partiellement_paye;
  }

  await this.prisma.obligationFiscale.update({
    where: { id: obligationId },
    data: { statut: newStatus }
  });
}


/*async updatePaymentStatus(paymentId: number, status: string) {
  return this.prisma.paiement.update({
    where: { id: paymentId },
    data: { etat_paiement: status },
  });
}*/

/*async getPaymentSummary(permisId: number) {
  const obligations = await this.prisma.obligationFiscale.findMany({
    where: { id_permis: permisId },
    include: { paiements: true }
  });

  const now = new Date();
  const summary = {
    totalObligations: obligations.length,
    paid: 0,
    overdue: 0,
    pending: 0,
    totalAmount: 0,
    paidAmount: 0,
    overdueAmount: 0
  };

  obligations.forEach(obligation => {
    const totalPaid = obligation.paiements.reduce(
      (sum, payment) => sum + payment.montant_paye, 0
    );
    
    summary.totalAmount += obligation.montant_attendu;
    summary.paidAmount += totalPaid;

    if (totalPaid >= obligation.montant_attendu) {
      summary.paid++;
    } else if (obligation.date_echeance < now) {
      summary.overdue++;
      summary.overdueAmount += (obligation.montant_attendu - totalPaid);
    } else {
      summary.pending++;
    }
  });

  return summary;
}
*/
////////////////////////////////////////////////////////////////////////////////////////////////////////////


}
