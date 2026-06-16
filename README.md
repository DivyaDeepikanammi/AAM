# AAM

AAM is a simple home food and medicine tracking app with two login types: Mom and Admin.

## Stack

- Frontend: Angular, Angular Material, RxJS, SCSS
- Backend: NestJS, Prisma ORM, PostgreSQL
- Tooling: Docker Compose, TypeScript, seed data
- Live app state: Firebase Realtime Database

## Project Structure

```text
frontend/
backend/
docker-compose.yml
```

## Quick Start

```bash
cp backend/.env.example backend/.env
docker compose up -d postgres
cd backend
npm install
npm run prisma:migrate
npm run prisma:seed
npm run start:dev
```

```bash
cd frontend
npm install
npm start
```

## Login Types

- Mom Login
- Admin Login

Demo password: `Aam@1234`

## Firebase Realtime Database

Frontend checklist and app state use:

```text
https://aamdb-840c9-default-rtdb.firebaseio.com/aam
```

Mom checklist updates are saved to `/aam/checklist`, and Admin listens to the same data.

## Pages

- Login Page
- Register Page
- Mom Dashboard
- Admin Dashboard
- Today's Meal Page
- Weekly Meal Plan Page
- Recipe List Page
- Medicine Reminder Page
- Reports / Stats Page
- Notifications Page

## Database Tables

- `users`
- `meals`
- `meal_logs`
- `weekly_meal_plans`
- `recipes`
- `medicines`
- `medicine_logs`
- `reminders`
- `notifications`
- `pain_logs`
- `monthly_health_checks`
- `stats`
