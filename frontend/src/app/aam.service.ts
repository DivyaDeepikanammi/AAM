import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone, signal } from '@angular/core';
import { catchError, map, Observable, of } from 'rxjs';
import { ChecklistItem, MealItem, MedicineItem, MonthlyHealth, RecipeItem, Stats, WeeklyPlanItem } from './app-types';

const FIREBASE_DB_URL = 'https://aamdb-840c9-default-rtdb.firebaseio.com';

export interface AamFirebaseState {
  checklist: ChecklistItem[];
  todayMeal: MealItem[];
  weeklyPlan: WeeklyPlanItem[];
  recipes: RecipeItem[];
  medicines: MedicineItem[];
  monthlyHealth: MonthlyHealth;
  stats: Stats;
  notifications: string[];
  updatedAt?: string;
}

@Injectable({ providedIn: 'root' })
export class AamService {
  readonly user = signal({ name: 'Admin', role: 'ADMIN' as 'MOM' | 'ADMIN' });

  constructor(
    private readonly http: HttpClient,
    private readonly zone: NgZone,
  ) {}

  firebaseUrl(path: string) {
    return `${FIREBASE_DB_URL}/${path}.json`;
  }

  loadState(): Observable<AamFirebaseState> {
    return this.http.get<Partial<AamFirebaseState> | null>(this.firebaseUrl('aam')).pipe(
      map((state) => this.normalizeState(state)),
      catchError(() => of(this.emptyState())),
    );
  }

  saveChecklist(checklist: ChecklistItem[]) {
    return this.http.put(this.firebaseUrl('aam/checklist'), checklist).pipe(
      catchError(() => of(null)),
    );
  }

  saveTodayMeal(todayMeal: MealItem[]) {
    return this.http.put(this.firebaseUrl('aam/todayMeal'), todayMeal).pipe(
      catchError(() => of(null)),
    );
  }

  saveWeeklyPlan(weeklyPlan: WeeklyPlanItem[]) {
    return this.http.put(this.firebaseUrl('aam/weeklyPlan'), weeklyPlan).pipe(
      catchError(() => of(null)),
    );
  }

  saveMonthlyHealth(monthlyHealth: MonthlyHealth) {
    return this.http.put(this.firebaseUrl('aam/monthlyHealth'), monthlyHealth).pipe(
      catchError(() => of(null)),
    );
  }

  saveMedicines(medicines: MedicineItem[]) {
    return this.http.put(this.firebaseUrl('aam/medicines'), medicines).pipe(
      catchError(() => of(null)),
    );
  }

  savePushToken(token: string) {
    const key = btoa(token).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
    return this.http.put(this.firebaseUrl(`aam/pushTokens/${key}`), {
      token,
      user: this.user().role,
      updatedAt: new Date().toISOString(),
    }).pipe(
      catchError(() => of(null)),
    );
  }

  listenChecklist(onValue: (items: ChecklistItem[]) => void) {
    const source = new EventSource(this.firebaseUrl('aam/checklist'));
    const handleEvent = (event: MessageEvent<string>) => {
      if (!event.data) return;
      const payload = JSON.parse(event.data) as { data?: ChecklistItem[] | null };
      if (Array.isArray(payload.data)) {
        this.zone.run(() => onValue(payload.data!));
      }
    };
    source.onmessage = handleEvent;
    source.addEventListener('put', handleEvent as EventListener);
    source.addEventListener('patch', handleEvent as EventListener);
    source.onerror = () => source.close();
    return () => source.close();
  }

  loginAs(role: 'MOM' | 'ADMIN') {
    this.user.set({ name: role === 'MOM' ? 'Mom' : 'Admin', role });
  }

  private normalizeState(state: Partial<AamFirebaseState> | null): AamFirebaseState {
    const empty = this.emptyState();
    return {
      checklist: state?.checklist ?? empty.checklist,
      todayMeal: state?.todayMeal ?? empty.todayMeal,
      weeklyPlan: state?.weeklyPlan ?? empty.weeklyPlan,
      recipes: state?.recipes ?? empty.recipes,
      medicines: state?.medicines ?? empty.medicines,
      monthlyHealth: state?.monthlyHealth ?? empty.monthlyHealth,
      stats: state?.stats ?? empty.stats,
      notifications: state?.notifications ?? empty.notifications,
      updatedAt: state?.updatedAt,
    };
  }

  private emptyState(): AamFirebaseState {
    return {
      checklist: [],
      todayMeal: [],
      weeklyPlan: [],
      recipes: [],
      medicines: [],
      monthlyHealth: {},
      stats: {
        daily: 0,
        weekly: 0,
        meals: 0,
        tablets: 0,
        missedTablets: 0,
        missedMeals: 0,
        water: 0,
        walking: 0,
        painTrend: [],
      },
      notifications: [],
    };
  }
}
