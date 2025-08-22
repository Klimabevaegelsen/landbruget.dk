import type { Metadata } from "next";
import { Plus_Jakarta_Sans } from "next/font/google";
import "./globals.css";

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
        <div id="password-overlay" style={{
          position: 'fixed',
          top: 0,
          left: 0,
          width: '100%',
          height: '100%',
          backgroundColor: 'white',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 9999,
          flexDirection: 'column',
          gap: '20px'
        }}>
          <h2 style={{ fontSize: '24px', fontWeight: 'bold', color: '#333' }}>
            Landbruget.dk
          </h2>
          <p style={{ color: '#666', marginBottom: '10px' }}>
            Indtast adgangskode:
          </p>
          <input
            type="password"
            id="site-password"
            placeholder="Adgangskode"
            style={{
              padding: '12px',
              border: '1px solid #ddd',
              borderRadius: '6px',
              fontSize: '16px',
              width: '250px'
            }}
          />
          <button
            onClick={() => {
              const input = document.getElementById('site-password') as HTMLInputElement;
              const password = input.value;
              const correctPassword = process.env.NEXT_PUBLIC_SITE_PASSWORD;

              if (password === correctPassword) {
                localStorage.setItem('authenticated', 'true');
                document.getElementById('password-overlay')!.style.display = 'none';
              } else {
                alert('Forkert adgangskode');
                input.value = '';
              }
            }}
            style={{
              padding: '12px 24px',
              backgroundColor: '#3b82f6',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              fontSize: '16px',
              cursor: 'pointer'
            }}
          >
            Log ind
          </button>
        </div>

        <script dangerouslySetInnerHTML={{
          __html: `
            // Check if already authenticated
            if (localStorage.getItem('authenticated') === 'true') {
              document.getElementById('password-overlay').style.display = 'none';
            }

            // Allow Enter key to submit
            document.getElementById('site-password').addEventListener('keypress', function(e) {
              if (e.key === 'Enter') {
                document.querySelector('button').click();
              }
            });
          `
        }} />

        {children}
      </body>
    </html>
  );
}
