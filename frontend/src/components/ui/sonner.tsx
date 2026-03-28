'use client';

import { useTheme } from '@/components/theme/theme-provider';
import { Toaster as Sonner, type ToasterProps } from 'sonner';

export function Toaster(props: ToasterProps) {
  const { theme } = useTheme();

  return (
    <Sonner
      theme={theme === 'system' ? undefined : (theme as ToasterProps['theme'])}
      className="toaster group"
      toastOptions={{
        classNames: {
          toast:
            'group toast group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg',
          description: 'group-[.toast]:text-muted-foreground',
          actionButton:
            'group-[.toast]:bg-primary group-[.toast]:text-primary-foreground',
          cancelButton:
            'group-[.toast]:bg-muted group-[.toast]:text-muted-foreground',
          success:
            'group-[.toaster]:border-primary/20 group-[.toaster]:bg-primary/5 group-[.toaster]:text-primary',
          error:
            'group-[.toaster]:border-destructive/20 group-[.toaster]:bg-destructive/5 group-[.toaster]:text-destructive',
          loading:
            'group-[.toaster]:border-primary/20 group-[.toaster]:bg-primary/5 group-[.toaster]:text-primary',
        },
      }}
      {...props}
    />
  );
}
