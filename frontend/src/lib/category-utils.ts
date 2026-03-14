/** Tailwind color classes for ranking category badges. */
export function getCategoryColor(category: string): string {
  switch (category) {
    case 'financial':
      return 'bg-organic/10 text-organic border-organic/20';
    case 'field':
      return 'bg-low-risk/10 text-low-risk border-low-risk/20';
    case 'environment':
      return 'bg-destructive/10 text-destructive border-destructive/20';
    case 'animal':
      return 'bg-conventional/10 text-conventional border-conventional/20';
    case 'worker':
      return 'bg-primary/10 text-primary border-primary/20';
    default:
      return 'bg-muted text-muted-foreground border-border';
  }
}

/** Danish label for ranking categories. */
export function getCategoryLabel(category: string): string {
  switch (category) {
    case 'financial':
      return 'Økonomi';
    case 'field':
      return 'Landbrugsareal';
    case 'environment':
      return 'Miljø';
    case 'animal':
      return 'Husdyr';
    case 'worker':
      return 'Medarbejdere';
    default:
      return category;
  }
}
