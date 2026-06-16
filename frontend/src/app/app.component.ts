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
import { ChecklistItem, MealItem, MedicineItem, MonthlyHealth, RecipeItem, Stats, TaskStatus, WeeklyPlanItem } from './app-types';

type Role = 'mom' | 'admin';
type ViewKey = 'login' | 'register' | 'mom' | 'admin' | 'today' | 'weekly' | 'recipes' | 'reminders' | 'stats' | 'notifications';

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
  readonly active = signal<ViewKey>('login');
  readonly role = signal<Role | null>(null);
  readonly today = new Date();
  readonly search = signal('');
  readonly selectedRecipe = signal<any | null>(null);
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
    return Math.round((items.filter((item) => item.status === 'Done').length / items.length) * 100);
  });

  readonly missedItems = computed(() => this.checklist().filter((item) => item.status === 'Missed'));
  readonly timeAlerts = computed(() => this.checklist()
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
    this.aam.loadState().subscribe((state) => {
      this.applyChecklist(state.checklist);
      this.todayMeal = state.todayMeal;
      this.weeklyPlan = state.weeklyPlan;
      this.recipes.set(state.recipes);
      this.medicines = state.medicines;
      this.monthlyHealth = state.monthlyHealth;
      this.stats = state.stats;
      this.notifications = state.notifications;
    });
    this.stopChecklistListener = this.aam.listenChecklist((items) => this.applyChecklist(items));
  }

  ngOnDestroy() {
    this.stopChecklistListener?.();
  }

  currentNav() {
    return this.role() === 'admin' ? this.adminNav : this.momNav;
  }

  loginAs(nextRole: Role) {
    this.role.set(nextRole);
    this.aam.loginAs(nextRole === 'mom' ? 'MOM' : 'ADMIN');
    this.active.set(nextRole === 'mom' ? 'mom' : 'admin');
  }

  markDone(item: ChecklistItem) {
    if (!this.canMarkNow(item)) {
      window.alert(`${item.label} can be marked only during ${this.windowLabel(item)}.`);
      return;
    }
    this.setStatus(item, 'Done');
  }

  setStatus(item: ChecklistItem, status: TaskStatus) {
    const time = new Date().toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    this.checklist.update((items) => items.map((entry) => entry.id === item.id ? {
      ...entry,
      status,
      completedAt: status === 'Done' ? time : undefined,
    } : entry));
    this.aam.saveChecklist(this.checklist()).subscribe();
  }

  statusClass(status: string) {
    return status.toLowerCase();
  }

  bar(value: number) {
    return `${Math.max(5, value)}%`;
  }

  buttonText(item: ChecklistItem) {
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

  windowLabel(item: ChecklistItem) {
    return this.windows[item.id]?.label ?? item.time;
  }

  private applyChecklist(items: ChecklistItem[]) {
    const next = this.enforceTimeWindows(items);
    this.checklist.set(next);
    if (JSON.stringify(next) !== JSON.stringify(items)) {
      this.aam.saveChecklist(next).subscribe();
    }
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
