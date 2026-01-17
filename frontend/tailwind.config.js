/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          DEFAULT: '#2a8b4e',
          foreground: '#eef8f2',
          darker: '#166534',
        },
        'foreground-darker': '#e0f5e8',
      },
      fontFamily: {
        sans: ['var(--font-plus-jakarta-sans)', 'system-ui', 'sans-serif'],
      },
      // Centralized z-index scale for consistent layering
      zIndex: {
        dropdown: '10',
        sticky: '20',
        fixed: '30',
        'modal-backdrop': '40',
        modal: '50',
        popover: '60',
        tooltip: '70',
        toast: '80',
        max: '100',
      },
      animation: {
        typing: 'typing 2s steps(20, end), blink 0.75s step-end infinite',
        morph: 'morphShape 5s ease-in-out infinite',
      },
      keyframes: {
        typing: {
          from: { width: '0' },
          to: { width: '100%' },
        },
        blink: {
          '50%': { borderColor: 'transparent' },
        },
        morphShape: {
          '0%': { borderRadius: '70% 30% 40% 60% / 40% 60% 30% 70%' },
          '50%': { borderRadius: '30% 70% 60% 40% / 60% 40% 70% 30%' },
          '100%': { borderRadius: '70% 30% 40% 60% / 40% 60% 30% 70%' },
        },
      },
    },
  },
  plugins: [],
};
