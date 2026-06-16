import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { AamService } from './aam.service';
import { HealthLogDto } from './dto';

@Controller()
export class AamController {
  constructor(private readonly aam: AamService) {}

  @Get('dashboard')
  dashboard() {
    return this.aam.dashboard();
  }

  @Get('compliance')
  compliance() {
    return this.aam.compliance();
  }

  @Get('pain-trends')
  painTrends() {
    return this.aam.painTrends();
  }

  @Get('meal-plans/:range')
  mealPlans(@Param('range') range: 'today' | 'weekly' | 'monthly' | 'rotation') {
    return this.aam.mealPlans(range);
  }

  @Get('recipes')
  recipes() {
    return this.aam.recipes();
  }

  @Get('vegetables')
  vegetables() {
    return this.aam.vegetables();
  }

  @Get('medicines')
  medicines() {
    return this.aam.medicines();
  }

  @Get('reminders')
  reminders() {
    return this.aam.reminders();
  }

  @Get('groceries')
  groceries(@Query('period') period: 'weekly' | 'monthly' = 'weekly') {
    return this.aam.groceries(period);
  }

  @Get('alerts')
  alerts() {
    return this.aam.alerts();
  }

  @Get('monthly-health')
  monthlyHealth() {
    return this.aam.monthlyHealth();
  }

  @Post('health-logs')
  addHealthLog(@Body() dto: HealthLogDto) {
    return this.aam.addHealthLog(dto);
  }

  @Get('reports')
  reports() {
    return this.aam.reports();
  }

  @Get('admin/users')
  users() {
    return this.aam.users();
  }
}

