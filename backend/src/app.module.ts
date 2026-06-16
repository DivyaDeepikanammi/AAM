import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { AamController } from './modules/aam.controller';
import { AamService } from './modules/aam.service';
import { AuthController } from './modules/auth.controller';
import { AuthService } from './modules/auth.service';
import { PrismaService } from './prisma.service';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), JwtModule.register({})],
  controllers: [AuthController, AamController],
  providers: [PrismaService, AuthService, AamService],
})
export class AppModule {}

