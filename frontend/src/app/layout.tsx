import type { Metadata, Viewport } from 'next';
import { Plus_Jakarta_Sans } from 'next/font/google';
import './globals.css';
import { ToastProvider_ } from '@/components/ui/toast';
import { ThemeProvider } from '@/components/theme/theme-provider';

const plusJakartaSans = Plus_Jakarta_Sans({
  variable: '--font-plus-jakarta-sans',
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
      <body className={`${plusJakartaSans.variable} antialiased`}>
        <ThemeProvider defaultTheme="system" storageKey="landbruget-theme">
          <ToastProvider_>{children}</ToastProvider_>
        </ThemeProvider>
      </body>
    </html>
  );
}
