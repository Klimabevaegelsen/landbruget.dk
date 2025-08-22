import type { Metadata } from "next";
import { Plus_Jakarta_Sans } from "next/font/google";
import "./globals.css";
import PasswordProtection from "@/components/PasswordProtection";

const plusJakartaSans = Plus_Jakarta_Sans({
  variable: "--font-plus-jakarta-sans",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Landbruget.dk",
  description: "Dansk landbrugsdata - samlet ét sted",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="da" className="bg-primary-foreground">
      <body className={`${plusJakartaSans.variable} antialiased`}>
        <PasswordProtection />
        {children}
      </body>
    </html>
  );
}
