'use client';

import { useState, useEffect } from 'react';

interface ResponsiveLayoutProps {
  children: React.ReactNode;
}

export function ResponsiveLayout({ children }: ResponsiveLayoutProps) {
  const [isMobile, setIsMobile] = useState(false);
  
  useEffect(() => {
    const checkMobile = () => setIsMobile(window.innerWidth < 768);
    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);
  
  return (
    <div className={`h-screen w-screen flex ${isMobile ? 'flex-col' : 'flex-row'}`}>
      {/* Main content area */}
      <div className={`${isMobile ? 'h-full' : 'flex-1'} relative`}>
        {children}
      </div>
    </div>
  );
} 