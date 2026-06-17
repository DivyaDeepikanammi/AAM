import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit, computed, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { AamService } from './aam.service';
import { ChecklistItem, MealItem, MedicineItem, MonthlyHealth, NutritionInfo, RecipeItem, Stats, TaskStatus, WeeklyPlanItem } from './app-types';

type Role = 'mom' | 'admin';
type ViewKey = 'mom' | 'admin' | 'today' | 'weekly' | 'recipes' | 'reminders' | 'stats' | 'notifications';

@Component({
  selector: 'aam-root',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatButtonModule,
    MatCardModule,
    MatChipsModule,
    MatFormFieldModule,
    MatIconModule,
    MatInputModule,
    MatProgressBarModule,
    MatSlideToggleModule,
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss',
})
export class AppComponent implements OnInit, OnDestroy {
  readonly active = signal<ViewKey>('mom');
  readonly role = signal<Role>('mom');
  readonly today = new Date();
  readonly search = signal('');
  readonly menuSearch = signal('');
  readonly replacingMeal = signal<string | null>(null);
  readonly selectedRecipe = signal<any | null>(null);
  readonly adminMenuOpen = signal(false);
  readonly checklist = signal<ChecklistItem[]>([]);
  readonly recipes = signal<RecipeItem[]>([]);
  todayMeal: MealItem[] = [];
  weeklyPlan: WeeklyPlanItem[] = [];
  medicines: MedicineItem[] = [];
  monthlyHealth: MonthlyHealth = {};
  stats: Stats = {
    daily: 0,
    weekly: 0,
    meals: 0,
    tablets: 0,
    missedTablets: 0,
    missedMeals: 0,
    water: 0,
    walking: 0,
    painTrend: [],
  };
  notifications: string[] = [];
  private stopChecklistListener?: () => void;
  private notificationTimer?: ReturnType<typeof setInterval>;
  private readonly notified = new Set<string>();
  private readonly windows: Record<string, { start: number; end: number; label: string }> = {
    breakfast: { start: 7 * 60, end: 10 * 60, label: '7 AM - 10 AM' },
    lunch: { start: 12 * 60, end: 14 * 60, label: '12 PM - 2 PM' },
    dinner: { start: 19 * 60, end: 21 * 60, label: '7 PM - 9 PM' },
    'morning-tablet': { start: 8 * 60, end: 10 * 60, label: '8 AM - 10 AM' },
    'afternoon-tablet': { start: 13 * 60, end: 15 * 60, label: '1 PM - 3 PM' },
    'night-tablet': { start: 20 * 60, end: 22 * 60, label: '8 PM - 10 PM' },
    walking: { start: 17 * 60, end: 20 * 60, label: '5 PM - 8 PM' },
    water: { start: 7 * 60, end: 22 * 60, label: '7 AM - 10 PM' },
  };

  readonly completion = computed(() => {
    const items = this.checklist();
    return Math.round((items.filter((item) => item.status === 'Done').length / Math.max(items.length, 1)) * 100);
  });

  readonly missedItems = computed(() => this.checklist().filter((item) => item.status === 'Missed'));
  readonly dashboardItems = computed(() => {
    const order = ['breakfast', 'lunch', 'dinner', 'morning-tablet', 'afternoon-tablet', 'night-tablet', 'walking', 'water'];
    return order
      .map((id) => this.checklist().find((item) => item.id === id || item.label.toLowerCase().replace(/\s+/g, '-') === id))
      .filter((item): item is ChecklistItem => Boolean(item));
  });
  readonly filteredRecipes = computed(() => {
    const query = this.search().toLowerCase().trim();
    return this.recipes().filter((item) => `${item.vegetable} ${item.name} ${item.type}`.toLowerCase().includes(query));
  });
  readonly menuMatches = computed(() => {
    const query = this.menuSearch().toLowerCase().trim();
    const source = query ? this.recipes().filter((recipe) => `${recipe.name} ${recipe.vegetable}`.toLowerCase().includes(query)) : this.recipes();
    return source.slice(0, 8);
  });
  public readonly timeAlerts = computed(() => this.checklist()
    .filter((item) => item.status === 'Pending' && this.isExpired(item))
    .map((item) => `${item.label} not marked during ${this.windowLabel(item)}`));
  readonly adminCards = computed(() => [
    { label: 'Today completion', value: `${this.completion()}%`, tone: 'blue' },
    ...this.checklist()
      .filter((item) => ['Breakfast', 'Lunch', 'Dinner', 'Morning Tablet', 'Afternoon Tablet', 'Night Tablet', 'Walking'].includes(item.label))
      .map((item) => ({ label: item.label, value: item.status, tone: this.tone(item.status) })),
    { label: 'Pain level', value: '8 / 10', tone: 'rose' },
    { label: 'Missed items', value: `${this.missedItems().length}`, tone: 'yellow' },
  ]);

  readonly recipeGroups = computed(() => {
    const query = this.search().toLowerCase();
    const grouped = new Map<string, RecipeItem[]>();
    for (const recipe of this.recipes().filter((item) => `${item.vegetable} ${item.name}`.toLowerCase().includes(query))) {
      grouped.set(recipe.vegetable, [...(grouped.get(recipe.vegetable) ?? []), recipe]);
    }
    return [...grouped.entries()].map(([vegetable, recipes]) => ({ vegetable, recipes }));
  });

  momNav = [
    { key: 'mom' as ViewKey, label: 'Dashboard', icon: 'home' },
    { key: 'today' as ViewKey, label: "Today's Meal", icon: 'restaurant' },
    { key: 'weekly' as ViewKey, label: 'Weekly Plan', icon: 'calendar_view_week' },
    { key: 'recipes' as ViewKey, label: 'Recipes', icon: 'menu_book' },
    { key: 'reminders' as ViewKey, label: 'Reminders', icon: 'notifications_active' },
  ];

  adminNav = [
    { key: 'admin' as ViewKey, label: 'Dashboard', icon: 'space_dashboard' },
    { key: 'stats' as ViewKey, label: 'Weekly Stats', icon: 'bar_chart' },
    { key: 'notifications' as ViewKey, label: 'Notifications', icon: 'notifications' },
  ];

  constructor(private readonly aam: AamService) {}

  ngOnInit() {
    this.aam.loginAs('MOM');
    this.aam.loadState().subscribe((state) => {
      this.applyChecklist(state.checklist);
      this.todayMeal = state.todayMeal;
      this.weeklyPlan = state.weeklyPlan;
      this.recipes.set(state.recipes);
      this.medicines = state.medicines;
      this.monthlyHealth = state.monthlyHealth;
      this.stats = state.stats;
      this.notifications = state.notifications;
      this.setupNotifications();
    });
    this.stopChecklistListener = this.aam.listenChecklist((items) => this.applyChecklist(items));
  }

  ngOnDestroy() {
    this.stopChecklistListener?.();
    if (this.notificationTimer) clearInterval(this.notificationTimer);
  }

  currentNav() {
    return this.role() === 'admin' ? this.adminNav : this.momNav;
  }

  switchRole(nextRole: Role) {
    this.role.set(nextRole);
    this.aam.loginAs(nextRole === 'mom' ? 'MOM' : 'ADMIN');
    this.active.set(nextRole === 'mom' ? 'mom' : 'admin');
    this.adminMenuOpen.set(false);
  }

  markDone(item: ChecklistItem) {
    if (!this.canMarkNow(item)) return;
    this.setStatus(item, 'Done');
  }

  setStatus(item: ChecklistItem, status: TaskStatus) {
    const time = new Date().toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    this.checklist.update((items) => items.map((entry) => entry.id === item.id ? {
      ...entry,
      status,
      completedAt: status === 'Done' ? time : undefined,
      missedReason: status === 'Done' ? undefined : entry.missedReason,
    } : entry));
    this.aam.saveChecklist(this.checklist()).subscribe();
  }

  setMissedReason(item: ChecklistItem, missedReason: string) {
    this.checklist.update((items) => items.map((entry) => entry.id === item.id ? { ...entry, missedReason } : entry));
    this.aam.saveChecklist(this.checklist()).subscribe();
  }

  statusClass(status: string) {
    return status.toLowerCase();
  }

  bar(value: number) {
    return `${Math.max(5, value)}%`;
  }

  todayTotals() {
    return this.todayMeal.reduce((total, meal) => this.addNutrition(total, this.mealNutrition(meal)), this.emptyNutrition());
  }

  rotatedWeeklyPlan() {
    if (!this.weeklyPlan.length) return [];
    const week = this.weekNumber(this.today);
    return this.weeklyPlan.map((day, index, list) => {
      const lunchSource = list[(index + week) % list.length];
      const dinnerSource = list[(index + week + 2) % list.length];
      const breakfastSource = list[(index + week + 4) % list.length];
      return {
        ...day,
        breakfast: breakfastSource.breakfast,
        lunch: lunchSource.lunch,
        dinner: dinnerSource.dinner,
        curry: lunchSource.curry,
        alternative: `This week swap: ${day.alternative}`,
      };
    });
  }

  graphHeight(value: number, max = 100) {
    return `${Math.max(8, Math.min(100, (value / max) * 100))}%`;
  }

  public buttonText(item: ChecklistItem) {
    if (item.status === 'Done') return 'Done';
    if (this.isBeforeWindow(item)) return this.windowLabel(item);
    if (this.isExpired(item)) return 'Missed';
    return 'Mark Done';
  }

  canMarkNow(item: ChecklistItem) {
    const window = this.windows[item.id];
    if (!window) return true;
    const now = this.minutesNow();
    return now >= window.start && now <= window.end;
  }

  public windowLabel(item: ChecklistItem) {
    return this.windows[item.id]?.label ?? item.time;
  }

  taskTone(item: ChecklistItem, index: number) {
    if (item.status === 'Done') return 'done';
    if (item.status === 'Missed') return 'missed';
    return ['meal-one', 'meal-two', 'meal-three', 'tablet-one', 'tablet-two', 'tablet-three', 'walk-tone', 'water-tone'][index] ?? 'pending';
  }

  openRecipeForMeal(meal: MealItem) {
    const recipe = this.findRecipeForMeal(meal);
    this.selectedRecipe.set(recipe ?? null);
    this.search.set(recipe?.name ?? meal.curry ?? meal.menu);
    this.active.set('recipes');
  }

  startMenuChange(meal: MealItem) {
    this.replacingMeal.set(meal.meal);
    this.menuSearch.set(meal.curry || meal.menu);
  }

  chooseMenu(meal: MealItem, recipe: RecipeItem) {
    this.todayMeal = this.todayMeal.map((entry) => entry.meal === meal.meal ? {
      ...entry,
      menu: this.menuForMeal(entry.meal, recipe.name),
      curry: recipe.name,
      alternative: entry.menu,
      nutrition: this.recipeNutrition(recipe, entry.meal),
    } : entry);
    this.replacingMeal.set(null);
    this.menuSearch.set('');
    this.aam.saveTodayMeal(this.todayMeal).subscribe();
  }

  mealNutrition(meal: MealItem): NutritionInfo {
    return meal.nutrition ?? this.estimateNutrition(`${meal.meal} ${meal.menu} ${meal.curry} ${meal.portion}`);
  }

  macroList(meal: MealItem) {
    const nutrition = this.mealNutrition(meal);
    return [
      { label: 'Carbs', value: `${nutrition.carbs}g` },
      { label: 'Protein', value: `${nutrition.protein}g` },
      { label: 'Fibre', value: `${nutrition.fibre}g` },
      { label: 'Fat', value: `${nutrition.fat}g` },
    ];
  }

  private applyChecklist(items: ChecklistItem[]) {
    const next = this.enforceTimeWindows(items);
    this.checklist.set(next);
    if (JSON.stringify(next) !== JSON.stringify(items)) {
      this.aam.saveChecklist(next).subscribe();
    }
  }

  private setupNotifications() {
    if (!('Notification' in window)) return;
    if (Notification.permission === 'default') Notification.requestPermission();
    if (this.notificationTimer) clearInterval(this.notificationTimer);
    this.notificationTimer = setInterval(() => this.sendDueNotifications(), 60000);
    this.sendDueNotifications();
  }

  private sendDueNotifications() {
    if (!('Notification' in window) || Notification.permission !== 'granted') return;
    for (const item of this.checklist()) {
      if (item.status !== 'Pending' || !this.isExpired(item) || this.notified.has(item.id)) continue;
      this.notified.add(item.id);
      new Notification(`AAM reminder: ${item.label}`, { body: `Please mark it done or add a missed reason. Time: ${this.windowLabel(item)}` });
    }
  }

  private findRecipeForMeal(meal: MealItem) {
    const haystack = `${meal.curry} ${meal.menu}`.toLowerCase();
    return this.recipes().find((recipe) => haystack.includes(recipe.name.toLowerCase()))
      ?? this.recipes().find((recipe) => haystack.includes(recipe.vegetable.toLowerCase()))
      ?? this.recipes().find((recipe) => recipe.name.toLowerCase().includes((meal.curry || '').toLowerCase()));
  }

  private menuForMeal(mealName: string, curry: string) {
    const name = mealName.toLowerCase();
    if (name.includes('breakfast')) return `${curry} + 1 egg or curd`;
    if (name.includes('dinner')) return `2 chapatis + ${curry}`;
    return `1 cup rice + ${curry} + curd salad`;
  }

  private recipeNutrition(recipe: RecipeItem, mealName: string) {
    return this.estimateNutrition(`${mealName} ${recipe.name} ${recipe.vegetable} ${recipe.ingredients}`);
  }

  private estimateNutrition(text: string): NutritionInfo {
    const value = text.toLowerCase();
    const nutrition = this.emptyNutrition();
    if (value.includes('rice')) this.addInPlace(nutrition, { calories: 205, carbs: 45, protein: 4, fibre: 1, fat: 1 });
    if (value.includes('chapati')) this.addInPlace(nutrition, { calories: 220, carbs: 36, protein: 7, fibre: 6, fat: 6 });
    if (value.includes('idli')) this.addInPlace(nutrition, { calories: 130, carbs: 28, protein: 4, fibre: 2, fat: 1 });
    if (value.includes('dosa') || value.includes('pesarattu')) this.addInPlace(nutrition, { calories: 180, carbs: 28, protein: 6, fibre: 3, fat: 5 });
    if (value.includes('oats') || value.includes('upma')) this.addInPlace(nutrition, { calories: 190, carbs: 30, protein: 6, fibre: 5, fat: 5 });
    if (value.includes('egg')) this.addInPlace(nutrition, { calories: 78, carbs: 1, protein: 6, fibre: 0, fat: 5 });
    if (value.includes('chicken')) this.addInPlace(nutrition, { calories: 180, carbs: 2, protein: 26, fibre: 1, fat: 7 });
    if (value.includes('paneer')) this.addInPlace(nutrition, { calories: 210, carbs: 6, protein: 13, fibre: 1, fat: 15 });
    if (value.includes('dal') || value.includes('pappu') || value.includes('rajma') || value.includes('chana')) this.addInPlace(nutrition, { calories: 160, carbs: 24, protein: 10, fibre: 7, fat: 3 });
    if (nutrition.calories === 0) this.addInPlace(nutrition, { calories: 120, carbs: 16, protein: 4, fibre: 5, fat: 4 });
    if (value.includes('curry') || value.includes('pulusu') || value.includes('fry') || value.includes('vegetable')) this.addInPlace(nutrition, { calories: 90, carbs: 12, protein: 3, fibre: 5, fat: 4 });
    return nutrition;
  }

  private emptyNutrition(): NutritionInfo {
    return { calories: 0, carbs: 0, protein: 0, fibre: 0, fat: 0 };
  }

  private addNutrition(a: NutritionInfo, b: NutritionInfo): NutritionInfo {
    return { calories: a.calories + b.calories, carbs: a.carbs + b.carbs, protein: a.protein + b.protein, fibre: a.fibre + b.fibre, fat: a.fat + b.fat };
  }

  private addInPlace(target: NutritionInfo, add: NutritionInfo) {
    target.calories += add.calories;
    target.carbs += add.carbs;
    target.protein += add.protein;
    target.fibre += add.fibre;
    target.fat += add.fat;
  }

  private weekNumber(date: Date) {
    const first = new Date(date.getFullYear(), 0, 1);
    return Math.ceil((((date.getTime() - first.getTime()) / 86400000) + first.getDay() + 1) / 7);
  }

  private enforceTimeWindows(items: ChecklistItem[]) {
    return items.map((item) => {
      const window = this.windows[item.id];
      if (!window) return item;
      if (item.status === 'Done') {
        return this.completedWithinWindow(item, window) ? item : { ...item, status: 'Missed' as TaskStatus, completedAt: undefined };
      }
      return this.isExpired(item) ? { ...item, status: 'Missed' as TaskStatus, completedAt: undefined } : item;
    });
  }

  private isBeforeWindow(item: ChecklistItem) {
    const window = this.windows[item.id];
    return Boolean(window && this.minutesNow() < window.start);
  }

  private isExpired(item: ChecklistItem) {
    const window = this.windows[item.id];
    return Boolean(window && this.minutesNow() > window.end);
  }

  private completedWithinWindow(item: ChecklistItem, window: { start: number; end: number }) {
    if (!item.completedAt) return false;
    const minutes = this.parseTime(item.completedAt);
    return minutes >= window.start && minutes <= window.end;
  }

  private parseTime(value: string) {
    const match = value.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
    if (!match) return -1;
    let hour = Number(match[1]);
    const minute = Number(match[2]);
    const period = match[3].toUpperCase();
    if (period === 'PM' && hour !== 12) hour += 12;
    if (period === 'AM' && hour === 12) hour = 0;
    return hour * 60 + minute;
  }

  private minutesNow() {
    const now = new Date();
    return now.getHours() * 60 + now.getMinutes();
  }

  private tone(status: string) {
    return status === 'Done' || status === 'Taken' ? 'lime' : status === 'Missed' ? 'rose' : 'yellow';
  }
}
