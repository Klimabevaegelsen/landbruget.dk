import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: {
    default: 'Data Explorer - Landbruget.dk',
    template: '%s | Landbruget.dk Data Explorer',
  },
  description: 'Explore and analyze comprehensive Danish agricultural datasets. Powered by DuckDB-WASM for instant browser-based analytics.',
  keywords: ['Danish agriculture', 'data explorer', 'agricultural data', 'DuckDB', 'SQL', 'data analysis'],
  authors: [{ name: 'Landbruget.dk' }],
  openGraph: {
    title: 'Data Explorer - Landbruget.dk',
    description: 'Explore and analyze comprehensive Danish agricultural datasets',
    type: 'website',
    locale: 'da_DK',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
