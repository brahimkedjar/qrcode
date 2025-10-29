// create-renewal-obligations.dto.ts

import { IsInt, IsNotEmpty, IsDateString, IsNumber, Min } from "class-validator";

export class CreateRenewalObligationsDto {
  @IsInt()
  @IsNotEmpty()
  procedureId: number;

  @IsDateString()
  @IsNotEmpty()
  renewalStartDate: string;

  @IsNumber()
  @Min(0.5) // Minimum 6 months
  @IsNotEmpty()
  renewalDuration: number; // in years
}