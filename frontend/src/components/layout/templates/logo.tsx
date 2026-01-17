'use client';

import Image from 'next/image';
import { clsx } from 'clsx';
import { useTheme } from '@/components/theme/theme-provider';
import { useEffect, useState } from 'react';

export function Logo({ className }: { className?: string }) {
  const { theme } = useTheme();
  const [isDark, setIsDark] = useState(false);

  useEffect(() => {
    if (theme === 'system') {
      const systemTheme = window.matchMedia(
        '(prefers-color-scheme: dark)'
      ).matches;
      setIsDark(systemTheme);
    } else {
      setIsDark(theme === 'dark');
    }
  }, [theme]);

  return (
    <Image
      src={isDark ? '/img/logo-dark.png' : '/img/logo.png'}
      alt="Landbruget.dk logo"
      width={361 / 2}
      height={54 / 2}
      className={clsx(className, 'overflow-visible')}
      priority
    />
  );
}

export function Mark({ className }: { className?: string }) {
  const { theme } = useTheme();
  const [isDark, setIsDark] = useState(false);

  useEffect(() => {
    if (theme === 'system') {
      const systemTheme = window.matchMedia(
        '(prefers-color-scheme: dark)'
      ).matches;
      setIsDark(systemTheme);
    } else {
      setIsDark(theme === 'dark');
    }
  }, [theme]);

  return (
    <Image
      src={isDark ? '/img/logo-dark.png' : '/img/logo.png'}
      alt="Landbruget.dk mark"
      width={34}
      height={34}
      className={className}
      priority
    />
  );
}
