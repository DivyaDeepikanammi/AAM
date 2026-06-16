import { Injectable } from '@nestjs/common';
import { Status } from '@prisma/client';
import { PrismaService } from '../prisma.service';
import { HealthLogDto } from './dto';

@Injectable()
export class AamService {
  constructor(private readonly prisma: PrismaService) {}

  async dashboard() {
    const [meals, medicines, pain, monthlyHealth, notifications, stat] = await Promise.all([
      this.prisma.meal.findMany({ orderBy: [{ type: 'asc' }] }),
      this.prisma.medicine.findMany({ where: { active: true }, include: { logs: true } }),
      this.prisma.painLog.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.monthlyHealthCheck.findFirst({ orderBy: { month: 'desc' } }),
      this.prisma.notification.findMany({ orderBy: { createdAt: 'desc' }, take: 8 }),
      this.prisma.stat.findFirst({ orderBy: { date: 'desc' } }),
    ]);
    const cards = [
      ...meals.map((meal) => ({ id: meal.id, label: this.title(meal.type), detail: meal.menuItem, status: Status.PENDING })),
      ...medicines.map((medicine) => ({
        id: medicine.id,
        label: medicine.name,
        detail: `${medicine.dosage} - ${medicine.time} - ${medicine.foodRule}`,
        status: medicine.logs[0]?.status ?? Status.PENDING,
      })),
      { id: 'walking', label: 'Walking', detail: '20 minute easy walk', status: Status.PENDING },
      { id: 'water', label: 'Water Intake', detail: '6 of 8 glasses', status: Status.PENDING },
      { id: 'pain', label: 'Pain Level', detail: `${pain?.level ?? 0} / 10`, status: (pain?.level ?? 0) > 7 ? Status.MISSED : Status.PENDING },
    ];
    return {
      cards,
      completion: stat?.dailyCompletion ?? this.completion(cards),
      todayMeal: meals,
      reminders: await this.prisma.reminder.findMany({ where: { enabled: true }, orderBy: { time: 'asc' } }),
      monthlyHealthSummary: monthlyHealth,
      notifications,
    };
  }

  async compliance() {
    const stat = await this.prisma.stat.findFirst({ orderBy: { date: 'desc' } });
    return {
      daily: {
        meals: stat?.mealsCompleted ?? 33,
        medicines: stat?.tabletsCompleted ?? 33,
        walking: stat?.walkingCompletion ?? 60,
        water: stat?.waterCompletion ?? 75,
      },
      weekly: stat?.painTrend ?? [4, 5, 6, 3, 8, 5, 4],
      labels: ['Meals', 'Medicines', 'Walking', 'Water'],
    };
  }

  async painTrends() {
    const logs = await this.prisma.painLog.findMany({ orderBy: { createdAt: 'asc' }, take: 30 });
    return logs.map((log) => ({
      date: log.createdAt,
      level: log.level,
      label: log.level <= 3 ? 'Mild' : log.level <= 6 ? 'Moderate' : 'Severe',
      notes: log.notes,
    }));
  }

  mealPlans(range: 'today' | 'weekly' | 'monthly' | 'rotation') {
    if (range === 'weekly') {
      return this.prisma.weeklyMealPlan.findMany({ orderBy: { createdAt: 'asc' } });
    }
    return this.prisma.meal.findMany({ orderBy: [{ type: 'asc' }] });
  }

  recipes() {
    return this.prisma.recipe.findMany({ orderBy: [{ vegetable: 'asc' }, { name: 'asc' }] });
  }

  vegetables() {
    return this.prisma.recipe.groupBy({ by: ['vegetable'], _count: true, orderBy: { vegetable: 'asc' } });
  }

  medicines() {
    return this.prisma.medicine.findMany({ include: { logs: true }, orderBy: { time: 'asc' } });
  }

  reminders() {
    return this.prisma.reminder.findMany({ orderBy: { time: 'asc' } });
  }

  async groceries(period: 'weekly' | 'monthly') {
    const recipes = await this.prisma.recipe.findMany({ take: period === 'weekly' ? 8 : 24 });
    return {
      period,
      items: {
        vegetables: [...new Set(recipes.map((recipe) => recipe.vegetable))],
        eggs: period === 'weekly' ? '14 eggs' : '60 eggs',
        chicken: period === 'weekly' ? '1 kg' : '4 kg',
        paneer: period === 'weekly' ? '500 g' : '2 kg',
      },
    };
  }

  async alerts() {
    const pain = await this.prisma.painLog.findFirst({ orderBy: { createdAt: 'desc' } });
    return [
      { type: 'missed-medicine', severity: 'high', message: 'Night tablet missed.' },
      { type: 'activity', severity: 'medium', message: 'No activity today after lunch.' },
      ...(pain && pain.level > 7 ? [{ type: 'pain', severity: 'high', message: 'Pain level above 7.' }] : []),
      { type: 'monthly-health', severity: 'medium', message: 'Monthly sugar check pending.' },
    ];
  }

  monthlyHealth() {
    return this.prisma.monthlyHealthCheck.findMany({ orderBy: { month: 'asc' } });
  }

  async addHealthLog(dto: HealthLogDto) {
    const mom = await this.prisma.user.findFirst({ where: { role: 'MOM' } });
    return this.prisma.painLog.create({
      data: {
        userId: mom!.id,
        level: dto.painLevel,
        notes: dto.notes,
      },
    });
  }

  reports() {
    return this.prisma.stat.findFirst({ orderBy: { date: 'desc' } });
  }

  users() {
    return this.prisma.user.findMany({ select: { id: true, email: true, name: true, role: true, createdAt: true } });
  }

  private title(mealType: string) {
    return mealType.charAt(0) + mealType.slice(1).toLowerCase();
  }

  private completion(cards: { status: Status }[]) {
    return Math.round((cards.filter((card) => card.status === Status.DONE || card.status === Status.TAKEN).length / Math.max(cards.length, 1)) * 100);
  }
}
