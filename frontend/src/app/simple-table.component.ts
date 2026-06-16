import { CommonModule } from '@angular/common';
import { Component, Input } from '@angular/core';

@Component({
  selector: 'aam-table',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="table">
      <article *ngFor="let row of rows">
        <span *ngFor="let cell of values(row)">{{ cell }}</span>
      </article>
    </div>
  `,
  styles: [`
    .table { display: grid; gap: 8px; }
    article { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 8px; padding: 12px; border-bottom: 1px solid #ebe5d8; }
    span { overflow-wrap: anywhere; }
  `],
})
export class SimpleTableComponent {
  @Input() rows: any[] = [];

  values(row: Record<string, unknown>) {
    return Object.entries(row)
      .filter(([key]) => !['id', 'passwordHash'].includes(key))
      .slice(0, 5)
      .map(([, value]) => typeof value === 'object' ? JSON.stringify(value) : String(value));
  }
}

