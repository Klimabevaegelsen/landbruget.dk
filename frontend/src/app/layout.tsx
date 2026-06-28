import type { Metadata, Viewport } from 'next';
import { Fraunces, Plus_Jakarta_Sans } from 'next/font/google';
import './globals.css';
import { Toaster } from '@/components/ui/sonner';
import { ThemeProvider } from '@/components/theme/theme-provider';

const plusJakartaSans = Plus_Jakarta_Sans({
  variable: '--font-plus-jakarta-sans',
  subsets: ['latin'],
});

const fraunces = Fraunces({
  variable: '--font-fraunces',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: 'Landbruget.dk',
  description: 'Dansk landbrugsdata - samlet ét sted',
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  maximumScale: 5,
  userScalable: true,
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="da" suppressHydrationWarning>
      <body
        className={`${plusJakartaSans.variable} ${fraunces.variable} overflow-x-hidden antialiased`}
      >
        <ThemeProvider defaultTheme="system" storageKey="landbruget-theme">
          {children}
          <Toaster />
        </ThemeProvider>
      </body>
    </html>
  );
}
