export type TaskStatus = 'Pending' | 'Done' | 'Missed';

export interface ChecklistItem {
  id: string;
  label: string;
  detail: string;
  time: string;
  status: TaskStatus;
  completedAt?: string;
  missedReason?: string;
}

export interface MealItem {
  meal: string;
  menu: string;
  curry: string;
  portion: string;
  alternative: string;
  nutrition?: NutritionInfo;
}

export interface NutritionInfo {
  calories: number;
  carbs: number;
  protein: number;
  fibre: number;
  fat: number;
}

export interface WeeklyPlanItem {
  day: string;
  breakfast: string;
  lunch: string;
  dinner: string;
  curry: string;
  alternative: string;
}

export interface MedicineItem {
  name: string;
  dosage: string;
  time: string;
  food: string;
  status: string;
}

export interface MonthlyHealth {
  fastingSugar?: number;
  ppSugar?: number;
  hba1c?: number;
  bp?: string;
  weight?: number;
  notes?: string;
}

export interface Stats {
  daily: number;
  weekly: number;
  meals: number;
  tablets: number;
  missedTablets: number;
  missedMeals: number;
  water: number;
  walking: number;
  painTrend: number[];
}

export interface RecipeItem {
  name: string;
  vegetable: string;
  type: string;
  ingredients: string;
  steps: string[];
  oil: string;
  salt: string;
  diabetes: string;
  serving: string;
}
