# AAM API Documentation

Base URL: `http://localhost:3000/api`

## Auth

`POST /auth/register`

```json
{ "name": "Admin", "email": "admin@aam.local", "password": "Aam@1234", "role": "ADMIN" }
```

`POST /auth/login`

```json
{ "email": "admin@aam.local", "password": "Aam@1234" }
```

## App Data

- `GET /dashboard` - today cards, completion, meals, reminders, monthly health, notifications
- `GET /meal-plans/today` - today's breakfast, lunch, and dinner
- `GET /meal-plans/weekly` - Monday to Sunday weekly meal plan
- `GET /recipes` - approved recipe list
- `GET /medicines` - medicine reminders with dosage, time, food rule, and status
- `GET /reminders` - meal, tablet, walking, water, and monthly sugar reminders
- `GET /alerts` - missed tablets, no activity, high pain, and monthly sugar alerts
- `GET /monthly-health` - monthly sugar, BP, weight, HbA1c, and notes
- `GET /reports` - daily and weekly stats
- `GET /notifications` is represented through dashboard notifications

## Demo Users

All demo users use password `Aam@1234`.

- `mom@aam.local`
- `admin@aam.local`
