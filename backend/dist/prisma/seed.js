"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("@prisma/client");
const bcrypt = require("bcryptjs");
const prisma = new client_1.PrismaClient();
const recipeCatalog = {
    BENDAKAYA: ['Bendakaya Pulusu', 'Bendakaya Fry', 'Bendakaya Tomato Curry', 'Bendakaya Peanut Curry', 'Bendakaya Egg Curry'],
    VANKAYA: ['Vankaya Tamarind Iguru', 'Vankaya Pulusu', 'Vankaya Rajma Curry', 'Vankaya Gummadi Vadialu Pulusu', 'Vankaya Tomato Curry', 'Stuffed Vankaya Curry'],
    DONDAKAYA: ['Dondakaya Gravy Curry', 'Dondakaya Fry', 'Dondakaya Nuvvula Curry', 'Dondakaya Tomato Curry', 'Dondakaya Peanut Curry'],
    BEERAKAYA: ['Beerakaya Pappu', 'Beerakaya Tomato Curry', 'Beerakaya Pachadi', 'Beerakaya Milk Curry'],
    'SORAKAYA / ANAPAKAYA': ['Sorakaya Pappu', 'Sorakaya Curry', 'Sorakaya Pulusu'],
    POTLAKAYA: ['Potlakaya Pappu', 'Potlakaya Curry', 'Potlakaya Milk Curry', 'Potlakaya Fry'],
    KAKARAKAYA: ['Kakarakaya Pulusu', 'Kakarakaya Onion Fry', 'Stuffed Kakarakaya', 'Kakarakaya Tomato Curry'],
    CAULIFLOWER: ['Cauliflower Instant Avakaya', 'Cauliflower Lemon Pickle'],
    CABBAGE: ['Cabbage Pappu Curry', 'Cabbage Moong Dal Curry'],
    CAPSICUM: ['Capsicum Paneer Rolls', 'Capsicum Chicken Rolls'],
    GUMMADIKAYA: ['Pumpkin Pulusu'],
    MUNAGAKAYA: ['Munagakaya Pulusu', 'Munagakaya Tomato Curry', 'Munagakaya Sambar'],
    BEANS: ['Beans Curry', 'Beans Carrot Curry'],
    CHIKKUDUKAYA: ['Chikkudukaya Curry', 'Chikkudukaya Tomato Curry'],
    GORUCHIKKUDUKAYA: ['Cluster Beans Curry', 'Cluster Beans Tomato Curry'],
    PALAKURA: ['Palakura Pappu'],
    THOTAKURA: ['Thotakura Pappu', 'Thotakura Curry', 'Thotakura Egg Curry'],
    GONGURA: ['Gongura Pappu', 'Gongura Chicken', 'Gongura Pachadi'],
    'METHI AAKU': ['Methi Dal', 'Methi Curry', 'Methi Egg Curry'],
    MUNAGAAKU: ['Munagaaku Pappu', 'Munagaaku Egg Curry', 'Munagaaku Fry'],
    'PONNAGANTI AAKU': ['Ponnaganti Curry', 'Ponnaganti Pappu'],
    CARROT: ['Carrot Curry', 'Carrot Peas Curry', 'Carrot Instant Avakaya'],
    BEETROOT: ['Beetroot Curry', 'Beetroot-Carrot Curry', 'Beetroot-Carrot Avakaya'],
    RAJMA: ['Rajma Curry', 'Rajma Pulusu'],
    CHANA: ['Chana Curry', 'Chana Masala'],
    PANEER: ['Paneer Curry', 'Paneer Capsicum Curry', 'Paneer Roll'],
    EGG: ['Egg Curry', 'Egg Pulusu', 'Egg Tomato Curry', 'Egg Bhurji'],
    CHICKEN: ['Andhra Chicken Curry', 'Chicken Pulusu', 'Gongura Chicken', 'Pepper Chicken'],
};
const weeklyPlan = [
    ['Monday', 'Idli + Egg', 'Bendakaya Pulusu', 'Egg Curry', 'Bendakaya Pulusu', 'Used last week. Suggested alternative: Bendakaya Fry'],
    ['Tuesday', 'Dosa + Egg', 'Vankaya Tamarind Iguru', 'Palakura Pappu', 'Vankaya Tamarind Iguru', 'Vankaya Tomato Curry'],
    ['Wednesday', 'Oats + Egg', 'Beerakaya Pappu', 'Paneer Capsicum Curry', 'Beerakaya Pappu', 'Beerakaya Tomato Curry'],
    ['Thursday', 'Upma + Egg', 'Dondakaya Fry', 'Gongura Pappu', 'Dondakaya Fry', 'Dondakaya Peanut Curry'],
    ['Friday', 'Pesarattu', 'Thotakura Pappu', 'Chicken Pulusu', 'Thotakura Pappu', 'Thotakura Egg Curry'],
    ['Saturday', 'Idli + Egg', 'Beans Carrot Curry', 'Rajma Pulusu', 'Beans Carrot Curry', 'Beans Curry'],
    ['Sunday', 'Dosa + Egg', 'Andhra Chicken Curry', 'Sorakaya Pappu', 'Andhra Chicken Curry', 'Pepper Chicken'],
];
function today() {
    const date = new Date();
    date.setHours(0, 0, 0, 0);
    return date;
}
function recipeType(name) {
    return ['Pulusu', 'Fry', 'Curry', 'Pappu', 'Pickle', 'Roll', 'Iguru', 'Pachadi', 'Dal', 'Avakaya'].find((type) => name.includes(type)) ?? 'Curry';
}
async function main() {
    await prisma.$transaction([
        prisma.medicineLog.deleteMany(),
        prisma.mealLog.deleteMany(),
        prisma.meal.deleteMany(),
        prisma.weeklyMealPlan.deleteMany(),
        prisma.medicine.deleteMany(),
        prisma.reminder.deleteMany(),
        prisma.notification.deleteMany(),
        prisma.painLog.deleteMany(),
        prisma.monthlyHealthCheck.deleteMany(),
        prisma.stat.deleteMany(),
        prisma.recipe.deleteMany(),
        prisma.user.deleteMany(),
    ]);
    const passwordHash = await bcrypt.hash('Aam@1234', 12);
    const mom = await prisma.user.create({ data: { email: 'mom@aam.local', name: 'Mom', role: 'MOM', passwordHash } });
    const admin = await prisma.user.create({ data: { email: 'admin@aam.local', name: 'Admin', role: 'ADMIN', passwordHash } });
    for (const [vegetable, names] of Object.entries(recipeCatalog)) {
        for (const name of names) {
            await prisma.recipe.create({
                data: {
                    name,
                    vegetable,
                    type: recipeType(name),
                    ingredients: `${vegetable.toLowerCase()}, onion, tomato, turmeric, chilli powder, coriander, curry leaves`,
                    steps: ['Wash and cut vegetables.', 'Cook with light oil and spices.', 'Simmer until soft.', 'Serve warm in a small portion.'],
                    oilSuggestion: 'Use 1 to 2 teaspoons oil.',
                    saltCaution: 'Keep salt low.',
                    diabetesNote: 'Pair with measured rice or chapati and curd salad.',
                    servingSize: '1 small bowl',
                },
            });
        }
    }
    for (const [day, breakfast, lunch, dinner, curry, alternativeCurry] of weeklyPlan) {
        await prisma.weeklyMealPlan.create({ data: { day, breakfast, lunch, dinner, curry, alternativeCurry } });
    }
    const bendakaya = await prisma.recipe.findFirst({ where: { name: 'Bendakaya Pulusu' } });
    const egg = await prisma.recipe.findFirst({ where: { name: 'Egg Curry' } });
    await prisma.meal.createMany({
        data: [
            { date: today(), type: client_1.MealType.BREAKFAST, menuItem: 'Idli + Egg', curryName: null, portion: '2 idlis + 1 egg', alternative: 'Dosa + Egg' },
            { date: today(), type: client_1.MealType.LUNCH, menuItem: '1 cup rice + Bendakaya Pulusu + carrot beetroot curd salad', curryName: 'Bendakaya Pulusu', portion: '1 cup rice', alternative: 'Bendakaya Fry + Rasam', recipeId: bendakaya?.id },
            { date: today(), type: client_1.MealType.DINNER, menuItem: '2 chapatis + Egg Curry', curryName: 'Egg Curry', portion: '2 chapatis', alternative: 'Egg Bhurji + cucumber curd', recipeId: egg?.id },
        ],
    });
    const medicines = await prisma.medicine.createManyAndReturn({
        data: [
            { name: 'Morning Tablet', dosage: '1 tablet', time: '8:30 AM', foodRule: 'After food' },
            { name: 'Afternoon Tablet', dosage: '1 tablet', time: '2:00 PM', foodRule: 'After food' },
            { name: 'Night Tablet', dosage: '1 tablet', time: '8:30 PM', foodRule: 'After food' },
        ],
    });
    await prisma.medicineLog.createMany({
        data: medicines.map((medicine, index) => ({
            userId: mom.id,
            medicineId: medicine.id,
            status: index === 0 ? 'TAKEN' : index === 2 ? 'MISSED' : 'PENDING',
        })),
    });
    await prisma.reminder.createMany({
        data: [
            { type: client_1.ReminderType.BREAKFAST, title: 'Breakfast', time: '8:00 AM' },
            { type: client_1.ReminderType.MORNING_TABLET, title: 'Morning tablet', time: '8:30 AM' },
            { type: client_1.ReminderType.LUNCH, title: 'Lunch', time: '1:00 PM' },
            { type: client_1.ReminderType.AFTERNOON_TABLET, title: 'Afternoon tablet', time: '2:00 PM' },
            { type: client_1.ReminderType.WALKING, title: 'Walking', time: '6:00 PM' },
            { type: client_1.ReminderType.DINNER, title: 'Dinner', time: '7:30 PM' },
            { type: client_1.ReminderType.NIGHT_TABLET, title: 'Night tablet', time: '8:30 PM' },
            { type: client_1.ReminderType.WATER, title: 'Water', time: 'All day' },
            { type: client_1.ReminderType.MONTHLY_SUGAR_CHECK, title: 'Monthly sugar check', time: 'Monthly' },
        ],
    });
    await prisma.painLog.create({ data: { userId: mom.id, level: 8, notes: 'Pain level above 7' } });
    await prisma.monthlyHealthCheck.create({
        data: { userId: mom.id, month: today(), fastingSugar: 130, ppSugar: 182, hba1c: 7.1, bp: '124/82', weight: 67, notes: 'Monthly sugar check pending' },
    });
    await prisma.stat.create({
        data: {
            date: today(),
            dailyCompletion: 25,
            weeklyCompletion: 78,
            mealsCompleted: 33,
            tabletsCompleted: 33,
            missedTablets: 1,
            missedMeals: 0,
            waterCompletion: 75,
            walkingCompletion: 60,
            painTrend: [4, 5, 6, 3, 8, 5, 4],
        },
    });
    await prisma.notification.createMany({
        data: [
            { userId: mom.id, title: 'Night tablet missed', body: 'Night tablet missed.' },
            { userId: admin.id, title: 'Monthly sugar check pending', body: 'Monthly sugar check pending.' },
            { userId: admin.id, title: 'Pain level above 7', body: 'Pain level above 7.' },
        ],
    });
}
main()
    .then(async () => prisma.$disconnect())
    .catch(async (error) => {
    console.error(error);
    await prisma.$disconnect();
    process.exit(1);
});
//# sourceMappingURL=seed.js.map