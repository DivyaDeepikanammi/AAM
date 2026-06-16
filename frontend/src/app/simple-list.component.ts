import { CommonModule } from '@angular/common';
import { Component, Input } from '@angular/core';

@Component({
  selector: 'aam-list',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="list">
      <article *ngFor="let item of items">
        <strong>{{ item.mealType || item.date }}</strong>
        <span>{{ item.title }}</span>
        <small>{{ item.date | date:'EEE, MMM d' }}</small>
      </article>
    </div>
  `,
  styles: [`
    .list { display: grid; gap: 12px; }
    article { display: grid; grid-template-columns: 130px 1fr 120px; gap: 12px; align-items: center; padding: 16px; border: 1px solid #e5dfd2; border-radius: 8px; background: #fff; }
    small { color: #66756f; text-align: right; }
    @media (max-width: 640px) { article { grid-template-columns: 1fr; } small { text-align: left; } }
  `],
})
export class SimpleListComponent {
  @Input() items: any[] = [];
}

