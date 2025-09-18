'use client';

import { useUIStore } from '@/stores/ui-store';
import { Sun, Moon, Monitor } from 'lucide-react';

interface ThemeToggleProps {
  className?: string;
}

export function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, setTheme } = useUIStore();

  const themes = [
    { id: 'light', icon: Sun, label: 'Light' },
    { id: 'dark', icon: Moon, label: 'Dark' },
    { id: 'system', icon: Monitor, label: 'System' },
  ] as const;

  return (
    <div className={`${className}`}>
      <div className="flex items-center space-x-1 rounded-lg border border-white/20 bg-black/80 p-1 backdrop-blur-sm">
        {themes.map(({ id, icon: Icon, label }) => (
          <button
            key={id}
            onClick={() => setTheme(id)}
            className={`flex items-center space-x-1 rounded px-2 py-1 text-xs transition-colors ${
              theme === id
                ? 'bg-white/20 text-white'
                : 'text-gray-300 hover:bg-white/10 hover:text-white'
            }`}
            title={`Switch to ${label} theme`}
          >
            <Icon className="h-3 w-3" />
            <span className="hidden sm:inline">{label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
