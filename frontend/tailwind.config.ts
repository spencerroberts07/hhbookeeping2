import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './app/**/*.{ts,tsx}',
    './components/**/*.{ts,tsx}',
    './lib/**/*.{ts,tsx}',
  ],
  theme: {
    container: {
      center: true,
      padding: '1rem',
      screens: {
        '2xl': '1400px',
      },
    },
    extend: {
      colors: {
        // BookWize brand tokens (per design system memo)
        'deep-navy': '#0B2E72',
        'ledger-blue': '#1454C8',
        'bw-teal': '#13B8B4',
        aqua: '#2DD4BF',
        slate: '#64748B',
        cloud: '#F4F7FB',
        ink: '#111827',
        // Semantic aliases used by shadcn primitives
        border: '#E2E8F0',
        input: '#CBD5E1',
        ring: '#1454C8',
        background: '#F4F7FB',
        foreground: '#111827',
        primary: {
          DEFAULT: '#0B2E72',
          foreground: '#FFFFFF',
        },
        secondary: {
          DEFAULT: '#FFFFFF',
          foreground: '#0B2E72',
        },
        accent: {
          DEFAULT: '#13B8B4',
          foreground: '#FFFFFF',
        },
        destructive: {
          DEFAULT: '#DC2626',
          foreground: '#FFFFFF',
        },
        muted: {
          DEFAULT: '#F4F7FB',
          foreground: '#64748B',
        },
        popover: {
          DEFAULT: '#FFFFFF',
          foreground: '#111827',
        },
        card: {
          DEFAULT: '#FFFFFF',
          foreground: '#111827',
        },
      },
      fontFamily: {
        sans: ['var(--font-inter)', 'Aptos', 'Segoe UI', 'Arial', 'sans-serif'],
      },
      fontSize: {
        // Brand scale
        'display': ['3.5rem', { lineHeight: '1.05', fontWeight: '800' }], // 56px
        'h1': ['2.5rem', { lineHeight: '1.1', fontWeight: '800' }],       // 40px
        'h2': ['1.875rem', { lineHeight: '1.2', fontWeight: '750' }],     // 30px
      },
      borderRadius: {
        xl: '12px',
        '2xl': '16px',
      },
      backgroundImage: {
        'gradient-primary':
          'linear-gradient(135deg, #0B2E72 0%, #1454C8 50%, #13B8B4 100%)',
      },
      boxShadow: {
        sm: '0 1px 2px 0 rgba(15, 23, 42, 0.05)',
        DEFAULT: '0 1px 3px 0 rgba(15, 23, 42, 0.08), 0 1px 2px -1px rgba(15, 23, 42, 0.04)',
        md: '0 4px 6px -1px rgba(15, 23, 42, 0.06), 0 2px 4px -2px rgba(15, 23, 42, 0.04)',
      },
      keyframes: {
        'accordion-down': {
          from: { height: '0' },
          to: { height: 'var(--radix-accordion-content-height)' },
        },
        'accordion-up': {
          from: { height: 'var(--radix-accordion-content-height)' },
          to: { height: '0' },
        },
      },
      animation: {
        'accordion-down': 'accordion-down 0.2s ease-out',
        'accordion-up': 'accordion-up 0.2s ease-out',
      },
    },
  },
  plugins: [require('tailwindcss-animate')],
};

export default config;
