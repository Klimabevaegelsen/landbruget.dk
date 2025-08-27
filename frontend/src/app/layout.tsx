import type { Metadata } from 'next';
import { Plus_Jakarta_Sans } from 'next/font/google';
import './globals.css';
import PasswordProtection from '@/components/PasswordProtection';
import { ToastProvider_ } from '@/components/ui/toast';

const plusJakartaSans = Plus_Jakarta_Sans({
  variable: '--font-plus-jakarta-sans',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: 'Landbruget.dk',
  description: 'Dansk landbrugsdata - samlet ét sted',
  viewport:
    'width=device-width, initial-scale=1, maximum-scale=5, user-scalable=yes',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="da" className="bg-primary-foreground">
      <body className={`${plusJakartaSans.variable} antialiased`}>
        <ToastProvider_>
          {process.env.NODE_ENV === 'production' && <PasswordProtection />}
          {children}
        </ToastProvider_>
      </body>
    </html>
  );
}
