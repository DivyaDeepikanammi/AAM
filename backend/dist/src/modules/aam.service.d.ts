import { PrismaService } from '../prisma.service';
import { HealthLogDto } from './dto';
export declare class AamService {
    private readonly prisma;
    constructor(prisma: PrismaService);
    dashboard(): Promise<{
        cards: {
            id: string;
            label: string;
            detail: string;
            status: import(".prisma/client").$Enums.Status;
        }[];
        completion: number;
        todayMeal: {
            id: string;
            createdAt: Date;
            type: import(".prisma/client").$Enums.MealType;
            recipeId: string | null;
            date: Date;
            menuItem: string;
            curryName: string | null;
            portion: string;
            alternative: string | null;
        }[];
        reminders: {
            id: string;
            createdAt: Date;
            type: import(".prisma/client").$Enums.ReminderType;
            time: string;
            title: string;
            enabled: boolean;
        }[];
        monthlyHealthSummary: {
            id: string;
            createdAt: Date;
            notes: string | null;
            userId: string;
            month: Date;
            fastingSugar: number | null;
            ppSugar: number | null;
            hba1c: number | null;
            bp: string | null;
            weight: number | null;
        } | null;
        notifications: {
            id: string;
            createdAt: Date;
            userId: string;
            title: string;
            body: string;
            read: boolean;
        }[];
    }>;
    compliance(): Promise<{
        daily: {
            meals: number;
            medicines: number;
            walking: number;
            water: number;
        };
        weekly: string | number | boolean | import("@prisma/client/runtime/library").JsonObject | import("@prisma/client/runtime/library").JsonArray;
        labels: string[];
    }>;
    painTrends(): Promise<{
        date: Date;
        level: number;
        label: string;
        notes: string | null;
    }[]>;
    mealPlans(range: 'today' | 'weekly' | 'monthly' | 'rotation'): import(".prisma/client").Prisma.PrismaPromise<{
        id: string;
        createdAt: Date;
        type: import(".prisma/client").$Enums.MealType;
        recipeId: string | null;
        date: Date;
        menuItem: string;
        curryName: string | null;
        portion: string;
        alternative: string | null;
    }[]> | import(".prisma/client").Prisma.PrismaPromise<{
        id: string;
        createdAt: Date;
        day: string;
        breakfast: string;
        lunch: string;
        dinner: string;
        curry: string;
        alternativeCurry: string;
    }[]>;
    recipes(): import(".prisma/client").Prisma.PrismaPromise<{
        id: string;
        name: string;
        createdAt: Date;
        vegetable: string;
        type: string;
        ingredients: string;
        steps: import("@prisma/client/runtime/library").JsonValue;
        oilSuggestion: string;
        saltCaution: string;
        diabetesNote: string;
        servingSize: string;
        lastUsedAt: Date | null;
    }[]>;
    vegetables(): import(".prisma/client").Prisma.GetRecipeGroupByPayload<{
        by: "vegetable"[];
        _count: true;
        orderBy: {
            vegetable: "asc";
        };
    }>;
    medicines(): import(".prisma/client").Prisma.PrismaPromise<({
        logs: {
            id: string;
            createdAt: Date;
            userId: string;
            medicineId: string;
            status: import(".prisma/client").$Enums.Status;
            completedAt: Date | null;
        }[];
    } & {
        id: string;
        name: string;
        dosage: string;
        time: string;
        foodRule: string;
        active: boolean;
    })[]>;
    reminders(): import(".prisma/client").Prisma.PrismaPromise<{
        id: string;
        createdAt: Date;
        type: import(".prisma/client").$Enums.ReminderType;
        time: string;
        title: string;
        enabled: boolean;
    }[]>;
    groceries(period: 'weekly' | 'monthly'): Promise<{
        period: "weekly" | "monthly";
        items: {
            vegetables: string[];
            eggs: string;
            chicken: string;
            paneer: string;
        };
    }>;
    alerts(): Promise<{
        type: string;
        severity: string;
        message: string;
    }[]>;
    monthlyHealth(): import(".prisma/client").Prisma.PrismaPromise<{
        id: string;
        createdAt: Date;
        notes: string | null;
        userId: string;
        month: Date;
        fastingSugar: number | null;
        ppSugar: number | null;
        hba1c: number | null;
        bp: string | null;
        weight: number | null;
    }[]>;
    addHealthLog(dto: HealthLogDto): Promise<{
        id: string;
        createdAt: Date;
        level: number;
        notes: string | null;
        userId: string;
    }>;
    reports(): import(".prisma/client").Prisma.Prisma__StatClient<{
        id: string;
        createdAt: Date;
        date: Date;
        dailyCompletion: number;
        weeklyCompletion: number;
        mealsCompleted: number;
        tabletsCompleted: number;
        missedTablets: number;
        missedMeals: number;
        waterCompletion: number;
        walkingCompletion: number;
        painTrend: import("@prisma/client/runtime/library").JsonValue;
    } | null, null, import("@prisma/client/runtime/library").DefaultArgs, import(".prisma/client").Prisma.PrismaClientOptions>;
    users(): import(".prisma/client").Prisma.PrismaPromise<{
        id: string;
        email: string;
        name: string;
        role: import(".prisma/client").$Enums.Role;
        createdAt: Date;
    }[]>;
    private title;
    private completion;
}
