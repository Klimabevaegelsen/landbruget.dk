'use client';

import { Moon, Sun, Monitor } from 'lucide-react';
import { useTheme } from './theme-provider';

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  return (
    <div className="border-border bg-card flex items-center rounded-lg border p-1">
      <button
        onClick={() => setTheme('light')}
        data-testid="theme-light-button"
        className={`flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors ${
          theme === 'light'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground hover:text-foreground'
        }`}
        aria-label="Light mode"
      >
        <Sun className="h-4 w-4" />
      </button>
      <button
        onClick={() => setTheme('dark')}
        data-testid="theme-dark-button"
        className={`flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors ${
          theme === 'dark'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground hover:text-foreground'
        }`}
        aria-label="Dark mode"
      >
        <Moon className="h-4 w-4" />
      </button>
      <button
        onClick={() => setTheme('system')}
        data-testid="theme-system-button"
        className={`flex items-center justify-center rounded-md px-3 py-2 text-sm font-medium transition-colors ${
          theme === 'system'
            ? 'bg-background text-foreground shadow-sm'
            : 'text-muted-foreground hover:text-foreground'
        }`}
        aria-label="System mode"
      >
        <Monitor className="h-4 w-4" />
      </button>
    </div>
  );
}

export function SimpleThemeToggle() {
  const { theme, setTheme } = useTheme();

  const toggleTheme = () => {
    if (theme === 'light') {
      setTheme('dark');
    } else if (theme === 'dark') {
      setTheme('system');
    } else {
      setTheme('light');
    }
  };

  const getIcon = () => {
    switch (theme) {
      case 'light':
        return <Sun className="h-4 w-4" />;
      case 'dark':
        return <Moon className="h-4 w-4" />;
      default:
        return <Monitor className="h-4 w-4" />;
    }
  };

  return (
    <button
      onClick={toggleTheme}
      data-testid="toggle-theme-button"
      className="text-muted-foreground hover:bg-muted hover:text-foreground flex items-center justify-center rounded-md p-2 transition-colors"
      aria-label="Toggle theme"
    >
      {getIcon()}
    </button>
  );
}
