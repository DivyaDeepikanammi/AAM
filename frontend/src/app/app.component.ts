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
import { AamService } from './aam.service';
import { ChecklistItem, MealItem, MedicineItem, MonthlyHealth, NutritionInfo, RecipeItem, Stats, TaskStatus, WeeklyPlanItem } from './app-types';
import { firebaseConfig, firebaseVapidKey, hasFirebaseMessagingConfig } from './firebase-config';

type Role = 'mom' | 'admin';
type ViewKey = 'mom' | 'admin' | 'today' | 'weekly' | 'recipes' | 'reminders' | 'stats' | 'notifications';
type MealName = 'Breakfast' | 'Lunch' | 'Dinner';
type AlertPermission = NotificationPermission | 'unsupported';

interface MealOption {
  meal: MealName;
  name: string;
  menu: string;
  curry: string;
  portion: string;
  protein: number;
  note?: string;
}

type MealOptionTuple = [string, string, string, string, number];
type LocalRecipe = RecipeItem & { protein?: string; fiber?: string; prepTime?: string; cookTime?: string };

const localRecipe = (
  name: string,
  vegetable: string,
  type: string,
  ingredients: string[],
  steps: string[],
  protein = '',
  diabetes = 'Diabetes friendly when portion controlled.',
  serving = '1 serving',
  fiber = '',
): LocalRecipe => ({
  name,
  vegetable,
  type,
  ingredients: ingredients.join(', '),
  steps,
  oil: 'Use minimal oil.',
  salt: 'Keep salt low.',
  diabetes,
  serving,
  protein,
  fiber,
});

const LOCAL_RECIPES: LocalRecipe[] = [
  localRecipe('Idli + Egg', 'Breakfast', 'Breakfast', ['Idli batter', '1 egg'], ['Pour batter into idli moulds.', 'Steam for 10-12 mins.', 'Boil egg for 8-10 mins.', 'Serve hot.'], '~10g', 'Yes', '2 idlis + 1 egg', 'Low'),
  localRecipe('Idli + Peanut Chutney + Egg', 'Breakfast', 'Breakfast', ['2 idlis', '2 tbsp roasted peanuts', 'Garlic', 'Dry chillies', '1 egg'], ['Steam idlis.', 'Grind peanuts, garlic and chilli.', 'Boil egg.', 'Serve together.'], '~12g'),
  localRecipe('Pesarattu', 'Breakfast', 'Breakfast', ['Green gram', 'Ginger', 'Green chilli', 'Cumin', 'Salt'], ['Soak green gram overnight.', 'Grind with ginger, chilli and cumin.', 'Make dosa on tawa.'], '~14g', 'Excellent', '2 pesarattu', 'High'),
  localRecipe('Pesarattu + Egg', 'Breakfast', 'Breakfast', ['Pesarattu batter', '1 egg', 'Onion', 'Coriander'], ['Make pesarattu.', 'Prepare omelette separately.', 'Serve together.'], '~18g'),
  localRecipe('Oats + Milk + Flaxseeds', 'Breakfast', 'Breakfast', ['1/2 cup oats', '1 cup milk', '1 tsp flaxseeds'], ['Boil milk.', 'Add oats.', 'Cook 5 mins.', 'Add flaxseeds.'], '~8g', 'Good', '1 bowl', 'High'),
  localRecipe('Oats + Egg', 'Breakfast', 'Breakfast', ['Oats', 'Milk or water', 'Egg'], ['Prepare oats.', 'Boil egg.', 'Serve together.'], '~14g', 'Good', '1 oats bowl + egg', 'Good'),
  localRecipe('Vegetable Oats', 'Breakfast', 'Breakfast', ['Oats', 'Carrot', 'Beans', 'Onion'], ['Saute vegetables.', 'Add oats.', 'Add water.', 'Cook until soft.'], '~8g', 'Good', '1 bowl', 'High'),
  localRecipe('Upma + Egg', 'Breakfast', 'Breakfast', ['Rava', 'Onion', 'Curry leaves', 'Mustard', 'Egg'], ['Prepare upma.', 'Boil egg separately.', 'Serve.'], '~10g'),
  localRecipe('Vegetable Upma', 'Breakfast', 'Breakfast', ['Rava', 'Carrot', 'Beans', 'Onion', 'Peas'], ['Roast rava.', 'Cook vegetables.', 'Add rava and water.', 'Cook until fluffy.'], '~7g', 'Good', '1 small bowl', 'Good'),
  localRecipe('Ragi Malt + Egg', 'Breakfast', 'Breakfast', ['Ragi flour', 'Water', 'Milk optional', 'Egg'], ['Mix ragi flour in water.', 'Cook until thick.', 'Boil egg.', 'Serve.'], '~12g'),
  localRecipe('Ragi Dosa + Peanut Chutney', 'Breakfast', 'Breakfast', ['Ragi flour', 'Rice flour', 'Onion', 'Peanut chutney'], ['Prepare batter.', 'Make dosa.', 'Serve with chutney.'], '~10g', 'Good', '2 dosas', 'High'),
  localRecipe('Poha + Peanuts', 'Breakfast', 'Breakfast', ['Poha', 'Peanuts', 'Onion', 'Curry leaves'], ['Wash poha.', 'Saute peanuts and onion.', 'Add poha.', 'Mix well.'], '~7g'),
  localRecipe('Poha + Egg', 'Breakfast', 'Breakfast', ['Poha', 'Onion', 'Egg'], ['Make poha.', 'Boil egg.', 'Serve together.'], '~12g'),
  localRecipe('Wheat Dosa + Egg', 'Breakfast', 'Breakfast', ['Wheat flour', 'Water', 'Onion', 'Egg'], ['Prepare batter.', 'Make dosa.', 'Serve with egg.'], '~12g'),
  localRecipe('Moong Dal Chilla', 'Breakfast', 'Breakfast', ['Moong dal', 'Ginger', 'Green chilli', 'Cumin'], ['Soak dal.', 'Grind batter.', 'Cook like dosa.'], '~15g', 'Good', '2 chillas', 'High'),
  localRecipe('Moong Dal Chilla + Paneer', 'Breakfast', 'Breakfast', ['Moong dal batter', 'Paneer stuffing'], ['Make chilla.', 'Add paneer stuffing.', 'Fold and serve.'], '~18g'),
  localRecipe('Boiled Chana Breakfast', 'Breakfast', 'Breakfast', ['Boiled chana', 'Onion', 'Coriander', 'Lemon'], ['Mix all ingredients.', 'Serve fresh.'], '~12g', 'Good', '1 small bowl', 'High'),
  localRecipe('Curd + Roasted Chana', 'Breakfast', 'Breakfast', ['Curd', 'Roasted chana'], ['Add chana into curd.', 'Mix and serve.'], '~10g', 'Good', '1 bowl', 'Good'),
  localRecipe('Bendakaya Pulusu', 'Bendakaya', 'Lunch', ['250g bendakaya', 'Tamarind', 'Onion', 'Tomato', 'Turmeric', 'Chilli powder', 'Salt', '1 tsp oil'], ['Fry bendakaya lightly.', 'Add onion and tomato.', 'Add tamarind extract.', 'Add spices.', 'Simmer 15 mins.'], '', 'Yes', '1 bowl'),
  localRecipe('Bendakaya Peanut Curry', 'Bendakaya', 'Lunch', ['Bendakaya', 'Roasted peanuts', 'Onion', 'Garlic'], ['Roast peanuts and grind.', 'Fry bendakaya.', 'Add peanut powder.', 'Cook 5 mins.']),
  localRecipe('Vankaya Tamarind Iguru', 'Vankaya', 'Lunch', ['Brinjal', 'Tamarind', 'Onion', 'Chilli powder', 'Garlic'], ['Cook brinjal.', 'Add tamarind extract.', 'Reduce until thick.']),
  localRecipe('Vankaya Rajma Curry', 'Vankaya', 'Lunch', ['Brinjal', 'Cooked rajma', 'Onion', 'Tomato'], ['Cook rajma separately.', 'Prepare brinjal curry.', 'Mix rajma.', 'Simmer.'], 'High'),
  localRecipe('Dondakaya Gravy Curry', 'Dondakaya', 'Lunch', ['Dondakaya', 'Onion', 'Tomato', 'Garlic'], ['Saute vegetables.', 'Add tomato gravy.', 'Cook until soft.']),
  localRecipe('Dondakaya Nuvvula Curry', 'Dondakaya', 'Lunch', ['Dondakaya', 'Sesame seeds', 'Garlic'], ['Roast sesame.', 'Grind.', 'Add to cooked dondakaya.']),
  localRecipe('Beerakaya Pappu', 'Beerakaya', 'Lunch/Dinner', ['Beerakaya', 'Kandipappu', 'Tomato'], ['Pressure cook dal.', 'Add cooked beerakaya.', 'Temper and serve.'], 'Medium'),
  localRecipe('Sorakaya Pappu', 'Sorakaya', 'Lunch/Dinner', ['Sorakaya', 'Kandipappu', 'Tomato'], ['Pressure cook.', 'Mash.', 'Temper.'], '~10g'),
  localRecipe('Kakarakaya Pulusu', 'Kakarakaya', 'Lunch', ['Bitter gourd', 'Tamarind', 'Onion'], ['Fry bitter gourd.', 'Add tamarind extract.', 'Cook until thick.']),
  localRecipe('Cabbage Pappu Curry', 'Cabbage', 'Lunch/Dinner', ['Cabbage', 'Dal', 'Tomato'], ['Cook dal.', 'Add cabbage.', 'Temper and serve.'], '~10g'),
  localRecipe('Capsicum Paneer Rolls', 'Capsicum', 'Lunch/Dinner', ['Chapati', 'Capsicum', 'Paneer'], ['Saute capsicum.', 'Add paneer.', 'Roll in chapati.'], '~18g'),
  localRecipe('Pumpkin Pulusu', 'Pumpkin', 'Lunch', ['Pumpkin', 'Tamarind', 'Onion'], ['Cook pumpkin.', 'Add tamarind.', 'Simmer.']),
  localRecipe('Munagakaya Pulusu', 'Munagakaya', 'Lunch', ['Drumsticks', 'Tamarind', 'Onion'], ['Cook drumsticks.', 'Add tamarind.', 'Simmer.']),
  localRecipe('Beans Carrot Curry', 'Beans', 'Lunch', ['Beans', 'Carrot', 'Onion'], ['Chop vegetables.', 'Saute.', 'Cook until soft.']),
  localRecipe('Palakura Pappu', 'Palakura', 'Lunch/Dinner', ['Spinach', 'Dal', 'Tomato', 'Garlic', 'Mustard', 'Cumin'], ['Pressure cook dal and spinach.', 'Mash lightly.', 'Temper with garlic and mustard.', 'Serve hot.'], '~10g', 'Good', '1 bowl', 'Good'),
  localRecipe('Thotakura Pappu', 'Thotakura', 'Lunch/Dinner', ['Thotakura', 'Toor dal', 'Tomato'], ['Pressure cook together.', 'Mash.', 'Temper.'], '~10g'),
  localRecipe('Gongura Chicken', 'Gongura', 'Lunch', ['Chicken', 'Gongura', 'Onion'], ['Cook chicken.', 'Add gongura paste.', 'Simmer.'], 'High'),
  localRecipe('Methi Dal', 'Methi', 'Lunch/Dinner', ['Methi leaves', 'Dal', 'Garlic'], ['Cook dal.', 'Add methi.', 'Temper.'], '~10g', 'Good', '1 bowl', 'Good'),
  localRecipe('Munagaaku Pappu', 'Munagaaku', 'Lunch/Dinner', ['Drumstick leaves', 'Dal'], ['Cook together.', 'Mash.', 'Temper.'], '~10g'),
  localRecipe('Ponnaganti Pappu', 'Ponnaganti', 'Dinner', ['Ponnaganti leaves', 'Dal'], ['Cook together.', 'Mash.', 'Temper.'], '~10g'),
  localRecipe('Rajma Curry', 'Rajma', 'Lunch/Dinner', ['Rajma', 'Onion', 'Tomato'], ['Soak overnight.', 'Pressure cook.', 'Prepare curry.'], 'High'),
  localRecipe('Chana Masala', 'Chana', 'Lunch/Dinner', ['Chana', 'Onion', 'Tomato'], ['Soak overnight.', 'Pressure cook.', 'Prepare masala.'], 'High'),
  localRecipe('Paneer Curry', 'Paneer', 'Lunch/Dinner', ['Paneer', 'Onion', 'Tomato'], ['Saute onion and tomato.', 'Add paneer.', 'Simmer.'], '~18g'),
  localRecipe('Andhra Chicken Curry', 'Chicken', 'Lunch', ['Chicken', 'Onion', 'Tomato', 'Ginger garlic'], ['Marinate chicken.', 'Cook masala.', 'Add chicken.', 'Simmer.'], 'High'),
  localRecipe('Chicken Pulusu', 'Chicken', 'Lunch', ['Chicken', 'Tamarind', 'Onion'], ['Cook chicken.', 'Add tamarind.', 'Simmer.'], 'High'),
  localRecipe('Egg Curry', 'Egg', 'Dinner', ['2 eggs', 'Onion', 'Tomato', 'Turmeric'], ['Boil eggs.', 'Prepare light onion tomato gravy.', 'Add eggs.', 'Simmer.'], '~14g'),
  localRecipe('Egg Tomato Curry', 'Egg', 'Dinner', ['Eggs', 'Tomato', 'Onion'], ['Make tomato gravy.', 'Add boiled eggs.', 'Simmer.'], '~14g'),
  localRecipe('Egg Bhurji', 'Egg', 'Dinner', ['Eggs', 'Onion', 'Tomato', 'Coriander'], ['Saute onion and tomato.', 'Add beaten eggs.', 'Scramble.'], '~14g'),
  localRecipe('Paneer Capsicum Curry', 'Paneer', 'Dinner', ['Paneer', 'Capsicum', 'Onion'], ['Saute vegetables.', 'Add paneer.', 'Cook lightly.'], '~18g'),
  localRecipe('Cabbage Moong Dal Curry', 'Cabbage', 'Dinner', ['Cabbage', 'Moong dal'], ['Cook together.', 'Temper lightly.'], '~10g'),
  localRecipe('Carrot Peas Curry', 'Carrot', 'Dinner', ['Carrot', 'Peas', 'Onion'], ['Saute vegetables.', 'Cook until soft.'], '~6g'),
  localRecipe('Beetroot Curry', 'Beetroot', 'Dinner', ['Beetroot', 'Onion'], ['Cook beetroot.', 'Temper lightly.']),
  localRecipe('Potlakaya Curry', 'Potlakaya', 'Dinner', ['Snake gourd', 'Onion'], ['Saute.', 'Cook until soft.']),
  localRecipe('Sorakaya Curry', 'Sorakaya', 'Dinner', ['Bottle gourd', 'Onion'], ['Cook until soft.', 'Use light seasoning.']),
  localRecipe('Munagakaya Sambar', 'Munagakaya', 'Dinner', ['Drumsticks', 'Dal', 'Tamarind'], ['Cook dal.', 'Add drumsticks.', 'Add tamarind.'], '~8g'),
];

const BREAKFAST_OPTIONS: MealOption[] = ([
  ['Idli + Egg', 'Idli + Egg', 'Idli', '2 idlis + 1 egg', 10],
  ['Idli + Peanut Chutney + Egg', 'Idli + peanut chutney + egg', 'Peanut chutney', '2 idlis + chutney + 1 egg', 12],
  ['Pesarattu', 'Pesarattu', 'Pesarattu', '2 pesarattu', 14],
  ['Pesarattu + Egg', 'Pesarattu + egg omelette', 'Pesarattu', '1 pesarattu + 1 egg omelette', 18],
  ['Oats + Milk + Flaxseeds', 'Oats cooked in milk with flaxseeds', 'Oats', '1/2 cup oats + milk + 1 tsp flaxseeds', 8],
  ['Oats + Egg', 'Oats bowl + boiled egg', 'Oats', 'Oats bowl + 1 egg', 14],
  ['Vegetable Oats', 'Oats with carrot, beans and onion', 'Vegetable oats', '1 bowl', 8],
  ['Upma + Egg', 'Small bowl upma + egg', 'Upma', 'Small bowl upma + 1 egg', 10],
  ['Vegetable Upma', 'Upma with carrot, beans and peas', 'Vegetable upma', '1 small bowl', 7],
  ['Ragi Malt + Egg', 'Unsweetened ragi malt + egg', 'Ragi malt', '1 glass ragi malt + 1 egg', 12],
  ['Ragi Dosa + Peanut Chutney', 'Ragi dosa + peanut chutney', 'Ragi dosa', '2 ragi dosas + chutney', 10],
  ['Poha + Peanuts', 'Poha with roasted peanuts', 'Poha', '1 small bowl', 7],
  ['Poha + Egg', 'Poha + boiled egg', 'Poha', '1 small bowl + 1 egg', 12],
  ['Wheat Dosa + Egg', 'Wheat dosa + egg', 'Wheat dosa', '2 wheat dosas + 1 egg', 12],
  ['Moong Dal Chilla', 'Moong dal chilla', 'Moong dal chilla', '2 chillas', 15],
  ['Moong Dal Chilla + Paneer', 'Moong dal chilla with paneer stuffing', 'Moong dal chilla', 'Chilla + small paneer stuffing', 18],
  ['Boiled Chana Breakfast', 'Boiled chana with onion, coriander and lemon', 'Boiled chana', '1 small bowl', 12],
  ['Curd + Roasted Chana', 'Curd bowl + roasted chana', 'Curd and chana', '1 bowl curd + roasted chana', 10],
] as MealOptionTuple[]).map(([name, menu, curry, portion, protein]) => ({ meal: 'Breakfast', name, menu, curry, portion, protein }));

const LUNCH_OPTIONS: MealOption[] = ([
  ['Bendakaya Pulusu + Egg', 'Rice + Bendakaya Pulusu + boiled egg + salad + curd', 'Bendakaya Pulusu', '1 to 1.25 cups rice', 15],
  ['Vankaya Tamarind Iguru + Egg Curry', 'Rice + Vankaya Tamarind Iguru + egg curry + salad + curd', 'Vankaya Tamarind Iguru', '1 to 1.25 cups rice', 18],
  ['Dondakaya Gravy + Rajma', 'Rice + Dondakaya Gravy Curry + rajma curry + salad + curd', 'Dondakaya Gravy Curry', '1 to 1.25 cups rice', 15],
  ['Beerakaya Pappu', 'Rice + Beerakaya Pappu + salad + curd', 'Beerakaya Pappu', '1 to 1.25 cups rice', 12],
  ['Sorakaya Pappu + Egg', 'Rice + Sorakaya Pappu + boiled egg + salad + curd', 'Sorakaya Pappu', '1 to 1.25 cups rice', 15],
  ['Potlakaya Curry + Chana', 'Rice + Potlakaya Curry + chana masala + salad + curd', 'Potlakaya Curry', '1 to 1.25 cups rice', 15],
  ['Kakarakaya Pulusu + Egg Bhurji', 'Rice + Kakarakaya Pulusu + egg bhurji + salad + curd', 'Kakarakaya Pulusu', '1 to 1.25 cups rice', 18],
  ['Cabbage Pappu Curry', 'Rice + Cabbage Pappu Curry + salad + curd', 'Cabbage Pappu Curry', '1 to 1.25 cups rice', 12],
  ['Capsicum Paneer Rolls', 'Rice + Capsicum Paneer Rolls + salad + curd', 'Capsicum Paneer Rolls', '1 to 1.25 cups rice', 18],
  ['Pumpkin Pulusu + Egg Curry', 'Rice + Pumpkin Pulusu + egg curry + salad + curd', 'Pumpkin Pulusu', '1 to 1.25 cups rice', 18],
  ['Munagakaya Pulusu + Egg', 'Rice + Munagakaya Pulusu + boiled egg + salad + curd', 'Munagakaya Pulusu', '1 to 1.25 cups rice', 15],
  ['Beans Carrot + Chicken', 'Rice + Beans Carrot Curry + chicken curry + salad + curd', 'Beans Carrot Curry', '1 to 1.25 cups rice', 25],
  ['Chikkudukaya + Egg Curry', 'Rice + Chikkudukaya Curry + egg curry + salad + curd', 'Chikkudukaya Curry', '1 to 1.25 cups rice', 18],
  ['Cluster Beans + Rajma', 'Rice + Cluster Beans Curry + rajma curry + salad + curd', 'Cluster Beans Curry', '1 to 1.25 cups rice', 15],
  ['Palakura Pappu', 'Rice + Palakura Pappu + salad + curd', 'Palakura Pappu', '1 to 1.25 cups rice', 12],
  ['Thotakura Pappu + Egg Curry', 'Rice + Thotakura Pappu + egg curry + salad + curd', 'Thotakura Pappu', '1 to 1.25 cups rice', 18],
  ['Gongura Chicken', 'Rice + Gongura Chicken + salad + curd', 'Gongura Chicken', '1 to 1.25 cups rice', 25],
  ['Methi Dal', 'Rice + Methi Dal + salad + curd', 'Methi Dal', '1 to 1.25 cups rice', 12],
  ['Munagaaku Pappu + Egg', 'Rice + Munagaaku Pappu + boiled egg + salad + curd', 'Munagaaku Pappu', '1 to 1.25 cups rice', 15],
  ['Ponnaganti Curry + Chana', 'Rice + Ponnaganti Curry + chana masala + salad + curd', 'Ponnaganti Curry', '1 to 1.25 cups rice', 15],
  ['Carrot Peas + Paneer', 'Rice + Carrot Peas Curry + paneer curry + salad + curd', 'Carrot Peas Curry', '1 to 1.25 cups rice', 18],
  ['Beetroot + Egg Bhurji', 'Rice + Beetroot Curry + egg bhurji + salad + curd', 'Beetroot Curry', '1 to 1.25 cups rice', 18],
  ['Rajma Curry', 'Rice + Rajma Curry + salad + curd', 'Rajma Curry', '1 to 1.25 cups rice', 15],
  ['Chana Masala', 'Rice + Chana Masala + salad + curd', 'Chana Masala', '1 to 1.25 cups rice', 15],
  ['Paneer Curry', 'Rice + Paneer Curry + salad + curd', 'Paneer Curry', '1 to 1.25 cups rice', 20],
  ['Andhra Chicken Curry', 'Rice + Andhra Chicken Curry + salad + curd', 'Andhra Chicken Curry', '1 to 1.25 cups rice', 25],
  ['Chicken Pulusu', 'Rice + Chicken Pulusu + salad + curd', 'Chicken Pulusu', '1 to 1.25 cups rice', 25],
  ['Gongura Chicken', 'Rice + Gongura Chicken + salad + curd', 'Gongura Chicken', '1 to 1.25 cups rice', 25],
] as MealOptionTuple[]).map(([name, menu, curry, portion, protein]) => ({ meal: 'Lunch', name, menu, curry, portion, protein, note: 'Always include salad and curd. Rice max 1.25 cups.' }));

const DINNER_OPTIONS: MealOption[] = ([
  ['Chapati + Palakura Pappu', '2 chapatis + Palakura Pappu', 'Palakura Pappu', '2 chapatis', 10],
  ['Chapati + Thotakura Pappu', '2 chapatis + Thotakura Pappu', 'Thotakura Pappu', '2 chapatis', 10],
  ['Chapati + Beerakaya Pappu', '2 chapatis + Beerakaya Pappu', 'Beerakaya Pappu', '2 chapatis', 10],
  ['Chapati + Sorakaya + Egg', '2 chapatis + Sorakaya Curry + boiled egg', 'Sorakaya Curry', '2 chapatis + 1 egg', 12],
  ['Chapati + Potlakaya + Egg Bhurji', '2 chapatis + Potlakaya Curry + egg bhurji', 'Potlakaya Curry', '2 chapatis', 15],
  ['Chapati + Methi Dal', '2 chapatis + Methi Dal', 'Methi Dal', '2 chapatis', 10],
  ['Chapati + Munagaaku Pappu', '2 chapatis + Munagaaku Pappu', 'Munagaaku Pappu', '2 chapatis', 10],
  ['Chapati + Ponnaganti Pappu', '2 chapatis + Ponnaganti Pappu', 'Ponnaganti Pappu', '2 chapatis', 10],
  ['Chapati + Egg Curry', '2 chapatis + Egg Curry', 'Egg Curry', '2 chapatis', 14],
  ['Chapati + Egg Tomato Curry', '2 chapatis + Egg Tomato Curry', 'Egg Tomato Curry', '2 chapatis', 14],
  ['Chapati + Egg Bhurji', '2 chapatis + Egg Bhurji', 'Egg Bhurji', '2 chapatis', 14],
  ['Chapati + Paneer Curry', '2 chapatis + Paneer Curry', 'Paneer Curry', '2 chapatis', 18],
  ['Chapati + Paneer Capsicum', '2 chapatis + Paneer Capsicum Curry', 'Paneer Capsicum Curry', '2 chapatis', 18],
  ['Chapati + Capsicum Paneer Rolls', '2 chapatis + Capsicum Paneer Rolls', 'Capsicum Paneer Rolls', '2 chapatis', 18],
  ['Chapati + Rajma Curry', '2 chapatis + Rajma Curry', 'Rajma Curry', '2 chapatis', 12],
  ['Chapati + Chana Masala', '2 chapatis + Chana Masala', 'Chana Masala', '2 chapatis', 12],
  ['Rice + Palakura Pappu', '0.75 cup rice + Palakura Pappu', 'Palakura Pappu', '0.75 cup rice', 10],
  ['Rice + Thotakura Pappu', '0.75 cup rice + Thotakura Pappu', 'Thotakura Pappu', '0.75 cup rice', 10],
  ['Rice + Beerakaya Pappu', '0.75 cup rice + Beerakaya Pappu', 'Beerakaya Pappu', '0.75 cup rice', 10],
  ['Rice + Sorakaya Pappu', '0.75 cup rice + Sorakaya Pappu', 'Sorakaya Pappu', '0.75 cup rice', 10],
  ['Rice + Methi Dal', '0.75 cup rice + Methi Dal', 'Methi Dal', '0.75 cup rice', 10],
  ['Rice + Munagakaya Sambar', '0.75 cup rice + Munagakaya Sambar', 'Munagakaya Sambar', '0.75 cup rice', 8],
  ['Rice + Gummadikaya Pulusu + Egg', '0.75 cup rice + Gummadikaya Pulusu + boiled egg', 'Gummadikaya Pulusu', '0.75 cup rice + 1 egg', 12],
  ['Chapati + Cabbage Pappu', '2 chapatis + Cabbage Pappu Curry', 'Cabbage Pappu Curry', '2 chapatis', 10],
  ['Chapati + Cabbage Moong Dal', '2 chapatis + Cabbage Moong Dal Curry', 'Cabbage Moong Dal Curry', '2 chapatis', 10],
  ['Chapati + Carrot Peas + Paneer', '2 chapatis + Carrot Peas Curry + paneer', 'Carrot Peas Curry', '2 chapatis', 18],
  ['Chapati + Beetroot + Egg', '2 chapatis + Beetroot Curry + egg', 'Beetroot Curry', '2 chapatis + egg', 14],
  ['Chapati + Dondakaya Tomato + Egg', '2 chapatis + Dondakaya Tomato Curry + egg', 'Dondakaya Tomato Curry', '2 chapatis + egg', 14],
] as MealOptionTuple[]).map(([name, menu, curry, portion, protein]) => ({ meal: 'Dinner', name, menu, curry, portion, protein, note: 'Keep dinner lighter than lunch.' }));

const MEAL_OPTIONS = [...BREAKFAST_OPTIONS, ...LUNCH_OPTIONS, ...DINNER_OPTIONS];

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
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss',
})
export class AppComponent implements OnInit, OnDestroy {
  readonly active = signal<ViewKey>('mom');
  readonly role = signal<Role>('mom');
  readonly showSplash = signal(true);
  readonly today = new Date();
  readonly search = signal('');
  readonly menuSearch = signal('');
  readonly replacingMeal = signal<string | null>(null);
  readonly selectedRecipe = signal<any | null>(null);
  readonly reasonItem = signal<ChecklistItem | null>(null);
  readonly sugarOpen = signal(false);
  readonly nutritionMeal = signal<MealItem | null>(null);
  readonly weeklyRecipeDay = signal<WeeklyPlanItem | null>(null);
  readonly mealRecipe = signal<LocalRecipe | RecipeItem | null>(null);
  readonly profileMenuOpen = signal(false);
  readonly alertPermission = signal<AlertPermission>('default');
  readonly alertToast = signal<{ title: string; body: string } | null>(null);
  readonly pushStatus = signal('Push alerts not connected');
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
  private alertToastTimer?: ReturnType<typeof setTimeout>;
  private pushListenerStarted = false;
  private alertsReadyAt = Date.now() + 120000;
  private readonly notified = new Set<string>();
  private readonly progressResetKey = 'aam-progress-reset-2026-06-17-v3';
  private readonly windows: Record<string, { start: number; end: number; label: string }> = {
    breakfast: { start: 7 * 60, end: 10 * 60, label: '7 AM - 10 AM' },
    lunch: { start: 12 * 60, end: 14 * 60, label: '12 PM - 2 PM' },
    dinner: { start: 19 * 60, end: 21 * 60, label: '7 PM - 9 PM' },
    'morning-tablet': { start: 8 * 60, end: 10 * 60, label: '8 AM - 10 AM' },
    'afternoon-tablet': { start: 13 * 60, end: 15 * 60, label: '1 PM - 3 PM' },
    'night-tablet': { start: 20 * 60, end: 22 * 60, label: '8 PM - 10 PM' },
    walking: { start: 17 * 60, end: 22 * 60, label: '5 PM - 10 PM' },
    water: { start: 7 * 60, end: 22 * 60, label: '7 AM - 10 PM' },
  };

  readonly completion = computed(() => {
    const items = this.checklist();
    return Math.round((items.filter((item) => item.status === 'Done').length / Math.max(items.length, 1)) * 100);
  });

  readonly missedItems = computed(() => this.checklist().filter((item) => item.status === 'Missed'));
  readonly dashboardItems = computed(() => {
    const order = ['breakfast', 'lunch', 'dinner', 'walking', 'water'];
    return order
      .map((id) => this.checklist().find((item) => item.id === id || item.label.toLowerCase().replace(/\s+/g, '-') === id))
      .filter((item): item is ChecklistItem => Boolean(item));
  });
  readonly homeStats = computed(() => {
    const items = this.checklist();
    const meals = this.percentFor(['breakfast', 'lunch', 'dinner'], items);
    const tablets = this.percentFor(['morning-tablet', 'afternoon-tablet', 'night-tablet'], items);
    const water = this.percentFor(['water'], items);
    const walking = this.percentFor(['walking'], items);
    return { meals, tablets, water, walking };
  });
  readonly filteredRecipes = computed(() => {
    const query = this.search().toLowerCase().trim();
    return this.allRecipes().filter((item) => `${item.vegetable} ${item.name} ${item.type}`.toLowerCase().includes(query));
  });
  readonly menuMatches = computed(() => {
    const meal = this.replacingMeal();
    const source = MEAL_OPTIONS.filter((option) => option.meal === meal);
    return source;
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
    for (const recipe of this.allRecipes().filter((item) => `${item.vegetable} ${item.name}`.toLowerCase().includes(query))) {
      grouped.set(recipe.vegetable, [...(grouped.get(recipe.vegetable) ?? []), recipe]);
    }
    return [...grouped.entries()].map(([vegetable, recipes]) => ({ vegetable, recipes }));
  });

  momNav = [
    { key: 'mom' as ViewKey, label: 'Dashboard', icon: 'home' },
    { key: 'today' as ViewKey, label: "Today's Meal", icon: 'restaurant' },
    { key: 'weekly' as ViewKey, label: 'Weekly Plan', icon: 'calendar_view_week' },
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
    this.alertPermission.set(this.currentAlertPermission());
    window.setTimeout(() => this.showSplash.set(false), 4600);
    this.aam.loadState().subscribe((state) => {
      const resetState = this.resetProgressOnce(state.checklist, state.medicines);
      this.applyChecklist(resetState.checklist);
      this.todayMeal = this.normalizeTodayMeals(state.todayMeal);
      if (JSON.stringify(this.todayMeal) !== JSON.stringify(state.todayMeal)) {
        this.aam.saveTodayMeal(this.todayMeal).subscribe();
      }
      this.weeklyPlan = state.weeklyPlan;
      this.recipes.set(state.recipes);
      this.medicines = this.enforceMedicineWindows(resetState.medicines);
      if (JSON.stringify(this.medicines) !== JSON.stringify(resetState.medicines)) {
        this.aam.saveMedicines(this.medicines).subscribe();
      }
      this.monthlyHealth = state.monthlyHealth;
      this.stats = state.stats;
      this.notifications = state.notifications;
      this.setupNotifications();
      this.setupPushMessages();
    });
    this.stopChecklistListener = this.aam.listenChecklist((items) => this.applyChecklist(items));
  }

  ngOnDestroy() {
    this.stopChecklistListener?.();
    if (this.notificationTimer) clearInterval(this.notificationTimer);
    if (this.alertToastTimer) clearTimeout(this.alertToastTimer);
  }

  currentNav() {
    return this.role() === 'admin' ? this.adminNav : this.momNav;
  }

  switchRole(nextRole: Role) {
    this.role.set(nextRole);
    this.aam.loginAs(nextRole === 'mom' ? 'MOM' : 'ADMIN');
    this.active.set(nextRole === 'mom' ? 'mom' : 'admin');
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

  saveMissedReason(missedReason: string) {
    const item = this.reasonItem();
    if (!item) return;
    this.setMissedReason(item, missedReason);
    this.reasonItem.set(null);
  }

  saveSugarReadings(form: Record<string, string>) {
    this.monthlyHealth = {
      ...this.monthlyHealth,
      fastingSugar: this.numberOrUndefined(form['fastingSugar']),
      ppSugar: this.numberOrUndefined(form['ppSugar']),
      hba1c: this.numberOrUndefined(form['hba1c']),
      bp: form['bp'],
      weight: this.numberOrUndefined(form['weight']),
      notes: form['notes'] || 'Sugar readings updated',
    };
    this.aam.saveMonthlyHealth(this.monthlyHealth).subscribe();
    this.sugarOpen.set(false);
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
    if (item.status === 'Missed' || this.isExpired(item)) return 'Missed';
    return 'Mark Done';
  }

  canMarkNow(item: ChecklistItem) {
    const window = this.windows[item.id];
    if (!window) return item.status !== 'Done';
    const now = this.minutesNow();
    return item.status !== 'Done' && now >= window.start && now <= window.end;
  }

  public windowLabel(item: ChecklistItem) {
    return this.windows[item.id]?.label ?? item.time;
  }

  taskTone(item: ChecklistItem, index: number) {
    if (item.status === 'Done') return 'done';
    if (item.status === 'Missed') return 'missed';
    return 'pending';
  }

  openRecipeForMeal(meal: MealItem) {
    const recipe = this.findRecipeForMeal(meal);
    this.mealRecipe.set(recipe ?? null);
  }

  allRecipes(): RecipeItem[] {
    const firebaseRecipes = this.recipes();
    const names = new Set(LOCAL_RECIPES.map((recipe) => recipe.name.toLowerCase()));
    return [...LOCAL_RECIPES, ...firebaseRecipes.filter((recipe) => !names.has(recipe.name.toLowerCase()))];
  }

  startMenuChange(meal: MealItem) {
    const name = this.mealName(meal);
    this.replacingMeal.set(this.replacingMeal() === name ? null : name);
    this.menuSearch.set('');
  }

  isReplacingMeal(meal: MealItem) {
    return this.replacingMeal() === this.mealName(meal);
  }

  chooseMenu(meal: MealItem, option: MealOption) {
    this.todayMeal = this.todayMeal.map((entry) => entry.meal === meal.meal ? {
      ...entry,
      menu: option.menu,
      curry: option.curry,
      portion: option.portion,
      alternative: entry.menu,
      nutrition: this.optionNutrition(option),
    } : entry);
    this.replacingMeal.set(null);
    this.menuSearch.set('');
    this.syncWeeklyPlanMeal(meal, option);
    this.aam.saveTodayMeal(this.todayMeal).subscribe();
  }

  chooseMenuByName(meal: MealItem, optionName: string) {
    const option = MEAL_OPTIONS.find((item) => item.meal === this.mealName(meal) && item.name === optionName);
    if (option) this.chooseMenu(meal, option);
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

  mainDishLabel(meal: MealItem) {
    return this.mealName(meal) === 'Breakfast' ? 'Tiffin' : 'Curry';
  }

  medicineLabel(medicine: MedicineItem, index: number) {
    const fixedLabels = ['Morning Tab', 'Afternoon Tab', 'Evening Tab'];
    if (index >= 0 && index < fixedLabels.length) return fixedLabels[index];
    const text = `${medicine.name} ${medicine.time}`.toLowerCase();
    if (text.includes('afternoon') || text.includes('2:') || text.includes('1:') || text.includes('3:')) return 'Afternoon Tab';
    if (text.includes('night') || text.includes('evening') || text.includes('8:30 pm') || text.includes('8 pm') || text.includes('9 pm')) return 'Evening Tab';
    if (text.includes('morning') || text.includes('8:')) return 'Morning Tab';
    return medicine.name;
  }

  markMedicineTaken(medicine: MedicineItem) {
    if (this.isMedicineMissed(medicine)) return;
    this.medicines = this.medicines.map((entry) => entry === medicine ? { ...entry, status: 'Taken' } : entry);
    this.aam.saveMedicines(this.medicines).subscribe();
  }

  enableAlerts() {
    if (!('Notification' in window)) {
      this.alertPermission.set('unsupported');
      return;
    }
    Notification.requestPermission().then((permission) => {
      this.alertPermission.set(permission);
      if (permission === 'granted') {
        this.showAppNotification('AAM alerts enabled', 'Missed meals and tablets will show here.');
        this.alertsReadyAt = Date.now() + 120000;
      }
    });
  }

  async enablePushAlerts() {
    if (!hasFirebaseMessagingConfig()) {
      this.pushStatus.set('Firebase push config missing');
      this.showInAppAlert('Firebase setup needed', 'Paste Firebase web config and Web Push key first.');
      return;
    }
    if (!('Notification' in window)) {
      this.pushStatus.set('Notifications unsupported');
      this.showInAppAlert('Notifications unsupported', 'This browser cannot receive push notifications.');
      return;
    }
    const permission = await Notification.requestPermission();
    this.alertPermission.set(permission);
    if (permission !== 'granted') {
      this.pushStatus.set('Notification permission blocked');
      return;
    }
    try {
      const token = await this.registerPushToken();
      if (!token) {
        this.pushStatus.set('Push not supported on this browser');
        this.showInAppAlert('Push unavailable', 'This browser cannot create a Firebase push token.');
        return;
      }
      this.aam.savePushToken(token).subscribe();
      this.pushStatus.set('Push alerts connected');
      this.showAppNotification('AAM push connected', 'This phone is ready for reminder push notifications.');
      this.setupPushMessages();
    } catch {
      this.pushStatus.set('Push setup failed');
      this.showInAppAlert('Push setup failed', 'Check Firebase config, Web Push key, and HTTPS deployment.');
    }
  }

  async toggleNotifications(enabled: boolean) {
    if (enabled) {
      await this.enablePushAlerts();
      return;
    }
    this.pushStatus.set('Push alerts paused');
    this.showInAppAlert('Alerts off', 'Push paused');
  }

  dismissAlertToast() {
    this.alertToast.set(null);
    if (this.alertToastTimer) clearTimeout(this.alertToastTimer);
  }

  medicineTone(medicine: MedicineItem, index: number) {
    const label = this.medicineLabel(medicine, index);
    if (label === 'Morning Tab') return 'medicine-morning';
    if (label === 'Afternoon Tab') return 'medicine-afternoon';
    if (label === 'Evening Tab') return 'medicine-evening';
    const status = medicine.status.toLowerCase();
    if (status === 'taken') return 'done';
    if (status === 'missed') return 'missed';
    return 'pending';
  }

  mealOptions(meal: MealItem) {
    return MEAL_OPTIONS.filter((option) => option.meal === this.mealName(meal));
  }

  weeklyRecipes(day: WeeklyPlanItem) {
    return [day.breakfast, day.lunch, day.dinner]
      .map((name) => this.findRecipeByText(name))
      .filter((recipe): recipe is RecipeItem => Boolean(recipe));
  }

  weeklyMealCards(day: WeeklyPlanItem) {
    return [
      { label: 'Breakfast', value: day.breakfast },
      { label: 'Lunch', value: day.lunch },
      { label: 'Dinner', value: day.dinner },
    ];
  }

  recipeProtein(recipe: RecipeItem | LocalRecipe | null) {
    return recipe && 'protein' in recipe ? recipe.protein : '';
  }

  recipeFiber(recipe: RecipeItem | LocalRecipe | null) {
    return recipe && 'fiber' in recipe ? recipe.fiber : '';
  }

  private applyChecklist(items: ChecklistItem[]) {
    const next = this.enforceTimeWindows(items);
    this.checklist.set(next);
    if (JSON.stringify(next) !== JSON.stringify(items)) {
      this.aam.saveChecklist(next).subscribe();
    }
  }

  private percentFor(ids: string[], items: ChecklistItem[]) {
    const matches = items.filter((item) => ids.includes(item.id));
    return Math.round((matches.filter((item) => item.status === 'Done').length / Math.max(matches.length, 1)) * 100);
  }

  private resetProgressOnce(checklist: ChecklistItem[], medicines: MedicineItem[]) {
    if (localStorage.getItem(this.progressResetKey) === 'true') {
      return { checklist, medicines };
    }
    const resetChecklist = checklist.map((item) => ({
      ...item,
      status: 'Pending' as TaskStatus,
      completedAt: undefined,
      missedReason: undefined,
    }));
    const resetMedicines = medicines.map((medicine) => ({ ...medicine, status: 'Pending' }));
    localStorage.setItem(this.progressResetKey, 'true');
    this.aam.saveChecklist(this.enforceTimeWindows(resetChecklist)).subscribe();
    this.aam.saveMedicines(this.enforceMedicineWindows(resetMedicines)).subscribe();
    return { checklist: resetChecklist, medicines: resetMedicines };
  }

  private normalizeTodayMeals(meals: MealItem[]) {
    return meals.map((meal) => {
      const options = this.mealOptions(meal);
      const valid = options.some((option) => option.name === meal.curry || option.curry === meal.curry || option.menu === meal.menu);
      if (valid || !options.length) return meal;
      const fallback = options[0];
      return {
        ...meal,
        menu: fallback.menu,
        curry: fallback.curry,
        portion: fallback.portion,
        alternative: meal.menu,
        nutrition: this.optionNutrition(fallback),
      };
    });
  }

  private mealName(meal: MealItem): MealName {
    const value = meal.meal.toLowerCase();
    if (value.includes('breakfast')) return 'Breakfast';
    if (value.includes('dinner')) return 'Dinner';
    return 'Lunch';
  }

  private setupNotifications() {
    if (!('Notification' in window)) return;
    this.alertPermission.set(Notification.permission);
    if (this.notificationTimer) clearInterval(this.notificationTimer);
    this.notificationTimer = setInterval(() => this.sendDueNotifications(), 60000);
  }

  private async setupPushMessages() {
    if (this.pushListenerStarted || !hasFirebaseMessagingConfig()) return;
    try {
      const { initializeApp, getApps } = await import('firebase/app');
      const { getMessaging, isSupported, onMessage } = await import('firebase/messaging');
      if (!(await isSupported())) return;
      const app = getApps().length ? getApps()[0] : initializeApp(firebaseConfig);
      const messaging = getMessaging(app);
      onMessage(messaging, (payload) => {
        this.showAppNotification(
          payload.notification?.title || 'AAM reminder',
          payload.notification?.body || 'Please check your meal or medicine reminder.',
        );
      });
      this.pushListenerStarted = true;
    } catch {
      this.pushStatus.set('Push listener failed');
    }
  }

  private sendDueNotifications() {
    const nextChecklist = this.enforceTimeWindows(this.checklist());
    if (JSON.stringify(nextChecklist) !== JSON.stringify(this.checklist())) {
      this.checklist.set(nextChecklist);
      this.aam.saveChecklist(nextChecklist).subscribe();
    }
    const nextMedicines = this.enforceMedicineWindows(this.medicines);
    if (JSON.stringify(nextMedicines) !== JSON.stringify(this.medicines)) {
      this.medicines = nextMedicines;
      this.aam.saveMedicines(this.medicines).subscribe();
    }
    if (!('Notification' in window) || Notification.permission !== 'granted' || Date.now() < this.alertsReadyAt) return;
    const candidates: Array<{ key: string; title: string; body: string; due: number }> = [];
    for (const item of this.checklist()) {
      if (!this.shouldNotifyChecklist(item)) continue;
      candidates.push({
        key: `checklist-${this.todayKey()}-${item.id}`,
        title: `${item.label} missed`,
        body: this.windowLabel(item),
        due: this.checklistDueMinutes(item),
      });
    }
    for (const [index, medicine] of this.medicines.entries()) {
      if (!this.isMedicineMissed(medicine)) continue;
      candidates.push({
        key: `medicine-${this.todayKey()}-${index}-${medicine.time}`,
        title: `${this.medicineLabel(medicine, index)} missed`,
        body: medicine.time,
        due: this.medicineDueMinutes(medicine),
      });
    }
    const next = candidates
      .filter((candidate) => !this.notified.has(candidate.key))
      .sort((a, b) => a.due - b.due)[0];
    if (next) this.notifyOnce(next.key, next.title, next.body);
  }

  private shouldNotifyChecklist(item: ChecklistItem) {
    return item.status === 'Missed' || (item.status === 'Pending' && this.isExpired(item));
  }

  private checklistDueMinutes(item: ChecklistItem) {
    return this.windows[item.id]?.end ?? this.parseTime(item.time);
  }

  private notifyOnce(key: string, title: string, body: string) {
    if (this.notified.has(key)) return;
    this.notified.add(key);
    this.showAppNotification(title, body, key);
  }

  private showAppNotification(title: string, body: string, tag = title) {
    this.showInAppAlert(title, body);
    if (!('Notification' in window) || Notification.permission !== 'granted') return;
    const options: NotificationOptions = {
      body,
      icon: 'app-icon.jpeg',
      badge: 'app-icon.jpeg',
      tag,
    };
    try {
      new Notification(title, options);
      return;
    } catch {
      if ('serviceWorker' in navigator) {
        navigator.serviceWorker.getRegistration()
          .then((registration) => registration?.showNotification(title, options));
      }
    }
  }

  private showInAppAlert(title: string, body: string) {
    this.alertToast.set({ title, body });
    if (this.alertToastTimer) clearTimeout(this.alertToastTimer);
    this.alertToastTimer = setTimeout(() => this.alertToast.set(null), 9000);
  }

  private currentAlertPermission(): AlertPermission {
    return 'Notification' in window ? Notification.permission : 'unsupported';
  }

  private async registerPushToken() {
    const { initializeApp, getApps } = await import('firebase/app');
    const { getMessaging, getToken, isSupported } = await import('firebase/messaging');
    if (!(await isSupported()) || !('serviceWorker' in navigator)) return '';
    const app = getApps().length ? getApps()[0] : initializeApp(firebaseConfig);
    const registration = await navigator.serviceWorker.register('/firebase-messaging-sw.js', {
      scope: '/firebase-cloud-messaging-push-scope',
    });
    const messaging = getMessaging(app);
    return getToken(messaging, {
      vapidKey: firebaseVapidKey,
      serviceWorkerRegistration: registration,
    });
  }

  private enforceMedicineWindows(medicines: MedicineItem[]) {
    return medicines.map((medicine) => {
      if (medicine.status.toLowerCase() === 'taken') return medicine;
      return this.isMedicineMissed(medicine) ? { ...medicine, status: 'Missed' } : { ...medicine, status: 'Pending' };
    });
  }

  private isMedicineMissed(medicine: MedicineItem) {
    if (medicine.status.toLowerCase() === 'taken') return false;
    const minutes = this.medicineDueMinutes(medicine);
    return minutes >= 0 && this.minutesNow() > minutes;
  }

  private medicineDueMinutes(medicine: MedicineItem) {
    const minutes = this.parseTime(medicine.time);
    return minutes < 0 ? -1 : minutes + 30;
  }

  private todayKey() {
    return new Date().toISOString().slice(0, 10);
  }

  private findRecipeForMeal(meal: MealItem) {
    const haystack = `${meal.curry} ${meal.menu}`.toLowerCase();
    return this.allRecipes().find((recipe) => haystack.includes(recipe.name.toLowerCase()))
      ?? this.allRecipes().find((recipe) => haystack.includes(recipe.vegetable.toLowerCase()))
      ?? this.allRecipes().find((recipe) => recipe.name.toLowerCase().includes((meal.curry || '').toLowerCase()));
  }

  private findRecipeByText(text: string) {
    const haystack = text.toLowerCase();
    return this.allRecipes().find((recipe) => haystack.includes(recipe.name.toLowerCase()))
      ?? this.allRecipes().find((recipe) => haystack.includes(recipe.vegetable.toLowerCase()))
      ?? this.allRecipes().find((recipe) => recipe.name.toLowerCase().includes(haystack));
  }

  private syncWeeklyPlanMeal(meal: MealItem, option: MealOption) {
    const todayName = this.today.toLocaleDateString('en-US', { weekday: 'long' });
    const key = this.mealName(meal).toLowerCase() as 'breakfast' | 'lunch' | 'dinner';
    this.weeklyPlan = this.weeklyPlan.map((day) => day.day === todayName ? {
      ...day,
      [key]: option.menu,
      curry: key === 'lunch' ? option.curry : day.curry,
      alternative: `Updated today: ${option.name}`,
    } : day);
    this.aam.saveWeeklyPlan(this.weeklyPlan).subscribe();
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

  private optionNutrition(option: MealOption): NutritionInfo {
    const nutrition = this.estimateNutrition(`${option.meal} ${option.menu} ${option.curry} ${option.portion}`);
    return { ...nutrition, protein: option.protein };
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

  private numberOrUndefined(value: unknown) {
    const number = Number(value);
    return Number.isFinite(number) ? number : undefined;
  }

  private enforceTimeWindows(items: ChecklistItem[]) {
    const now = this.minutesNow();
    return items.map((item) => {
      const window = this.windows[item.id];
      if (!window) return item;
      if (item.status === 'Done') return item;
      if (item.status === 'Missed' && now <= window.end) {
        return { ...item, status: 'Pending' as TaskStatus, completedAt: undefined, missedReason: undefined };
      }
      return now > window.end ? { ...item, status: 'Missed' as TaskStatus, completedAt: undefined } : item;
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
