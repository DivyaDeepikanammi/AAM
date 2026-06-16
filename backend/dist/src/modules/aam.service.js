"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AamService = void 0;
const common_1 = require("@nestjs/common");
const client_1 = require("@prisma/client");
const prisma_service_1 = require("../prisma.service");
let AamService = class AamService {
    prisma;
    constructor(prisma) {
        this.prisma = prisma;
    }
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
            ...meals.map((meal) => ({ id: meal.id, label: this.title(meal.type), detail: meal.menuItem, status: client_1.Status.PENDING })),
            ...medicines.map((medicine) => ({
                id: medicine.id,
                label: medicine.name,
                detail: `${medicine.dosage} - ${medicine.time} - ${medicine.foodRule}`,
                status: medicine.logs[0]?.status ?? client_1.Status.PENDING,
            })),
            { id: 'walking', label: 'Walking', detail: '20 minute easy walk', status: client_1.Status.PENDING },
            { id: 'water', label: 'Water Intake', detail: '6 of 8 glasses', status: client_1.Status.PENDING },
            { id: 'pain', label: 'Pain Level', detail: `${pain?.level ?? 0} / 10`, status: (pain?.level ?? 0) > 7 ? client_1.Status.MISSED : client_1.Status.PENDING },
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
    mealPlans(range) {
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
    async groceries(period) {
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
    async addHealthLog(dto) {
        const mom = await this.prisma.user.findFirst({ where: { role: 'MOM' } });
        return this.prisma.painLog.create({
            data: {
                userId: mom.id,
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
    title(mealType) {
        return mealType.charAt(0) + mealType.slice(1).toLowerCase();
    }
    completion(cards) {
        return Math.round((cards.filter((card) => card.status === client_1.Status.DONE || card.status === client_1.Status.TAKEN).length / Math.max(cards.length, 1)) * 100);
    }
};
exports.AamService = AamService;
exports.AamService = AamService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [prisma_service_1.PrismaService])
], AamService);
//# sourceMappingURL=aam.service.js.map