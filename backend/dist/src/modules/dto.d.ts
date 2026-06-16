export declare class LoginDto {
    email: string;
    password: string;
}
export declare class RegisterDto extends LoginDto {
    name: string;
    role: 'MOM' | 'ADMIN';
}
export declare class ForgotPasswordDto {
    email: string;
}
export declare class ResetPasswordDto {
    token: string;
    password: string;
}
export declare class StatusDto {
    status: 'PENDING' | 'DONE' | 'MISSED' | 'TAKEN';
}
export declare class HealthLogDto {
    painLevel: number;
    waterGlasses: number;
    walkingDone?: boolean;
    notes?: string;
}
