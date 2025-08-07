'use client'

import { useUIStore } from '@/stores/ui-store'
import { Sun, Moon, Monitor } from 'lucide-react'

interface ThemeToggleProps {
  className?: string
}

export function ThemeToggle({ className = '' }: ThemeToggleProps) {
  const { theme, setTheme } = useUIStore()
  
  const themes = [
    { id: 'light', icon: Sun, label: 'Light' },
    { id: 'dark', icon: Moon, label: 'Dark' },
    { id: 'system', icon: Monitor, label: 'System' }
  ] as const
  
  return (
    <div className={`${className}`}>
      <div className="flex items-center space-x-1 bg-black/80 backdrop-blur-sm rounded-lg border border-white/20 p-1">
        {themes.map(({ id, icon: Icon, label }) => (
          <button
            key={id}
            onClick={() => setTheme(id)}
            className={`flex items-center space-x-1 px-2 py-1 rounded text-xs transition-colors ${
              theme === id
                ? 'bg-white/20 text-white'
                : 'text-gray-300 hover:text-white hover:bg-white/10'
            }`}
            title={`Switch to ${label} theme`}
          >
            <Icon className="w-3 h-3" />
            <span className="hidden sm:inline">{label}</span>
          </button>
        ))}
      </div>
    </div>
  )
} 