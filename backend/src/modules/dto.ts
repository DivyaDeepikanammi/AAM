import { IsEmail, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class LoginDto {
  @IsEmail()
  email!: string;

  @IsString()
  password!: string;
}

export class RegisterDto extends LoginDto {
  @IsString()
  name!: string;

  @IsString()
  role!: 'MOM' | 'ADMIN';
}

export class ForgotPasswordDto {
  @IsEmail()
  email!: string;
}

export class ResetPasswordDto {
  @IsString()
  token!: string;

  @IsString()
  password!: string;
}

export class StatusDto {
  @IsString()
  status!: 'PENDING' | 'DONE' | 'MISSED' | 'TAKEN';
}

export class HealthLogDto {
  @IsInt()
  @Min(1)
  @Max(10)
  painLevel!: number;

  @IsInt()
  @Min(0)
  waterGlasses!: number;

  @IsOptional()
  walkingDone?: boolean;

  @IsOptional()
  @IsString()
  notes?: string;
}
