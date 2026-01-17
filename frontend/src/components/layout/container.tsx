import { cn } from '@/lib/utils';

export function Container({
  className,
  subclassName,
  children,
  section,
  variant = 'default',
}: {
  className?: string;
  children: React.ReactNode;
  subclassName?: string;
  section?: boolean;
  variant?: 'default' | 'full' | 'narrow' | 'hero' | 'nav';
}) {
  // Mobile-first container system optimized for different mobile contexts
  const getContainerClasses = () => {
    switch (variant) {
      case 'hero':
        // Hero sections: Edge-to-edge on mobile, centered content with breathing room
        return cn(
          'w-full max-w-none', // Full width container
          'px-3 sm:px-6 md:px-8', // Minimal mobile padding, more on larger screens
          'mx-auto', // Center the container
          className
        );

      case 'nav':
        // Navigation: Full width, minimal padding for maximum usable space
        return cn(
          'w-full max-w-none',
          'px-2 sm:px-4 md:px-6 lg:px-8', // Very minimal mobile padding
          className
        );

      case 'full':
        // Full width: For components that need to span the entire viewport
        return cn(
          'w-full max-w-none',
          'px-0', // No padding - component handles its own spacing
          className
        );

      case 'narrow':
        // Reading content: Optimized for text readability on mobile
        return cn(
          'w-full max-w-none sm:max-w-lg md:max-w-2xl lg:max-w-4xl',
          'px-4 sm:px-6 md:px-8',
          'mx-auto',
          className
        );

      default:
        // Default: Mobile-optimized with progressive enhancement
        return cn(
          'w-full max-w-none sm:max-w-full md:max-w-6xl lg:max-w-7xl',
          'px-3 sm:px-6 md:px-8', // Start small on mobile
          'mx-auto',
          className
        );
    }
  };

  return (
    <div className={getContainerClasses()}>
      <div
        className={cn(
          section && 'py-8 sm:py-12 md:py-16', // Smaller mobile section padding
          subclassName
        )}
      >
        {children}
      </div>
    </div>
  );
}
