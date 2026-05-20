import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import { ClerkProvider } from '@clerk/nextjs';
import { Toaster } from 'sonner';
import { QueryProvider } from '@/components/providers/query-provider';
import './globals.css';

const inter = Inter({
  subsets: ['latin'],
  variable: '--font-inter',
  display: 'swap',
});

export const metadata: Metadata = {
  title: {
    default: 'BookWize — Smart bookkeeping. Clearer business.',
    template: '%s · BookWize',
  },
  description:
    'Month-end close, automated for Home Hardware dealers. Built for the way dealers actually work.',
  metadataBase: new URL(
    process.env.NEXT_PUBLIC_APP_URL ?? 'https://bookwize.ca',
  ),
  icons: {
    icon: [
      { url: '/brand/bookwize-icon-primary.svg', type: 'image/svg+xml' },
    ],
  },
  openGraph: {
    type: 'website',
    siteName: 'BookWize',
    title: 'BookWize — Smart bookkeeping. Clearer business.',
    description:
      'Month-end close, automated for Home Hardware dealers.',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <ClerkProvider
      appearance={{
        variables: {
          colorPrimary: '#0B2E72',
          colorText: '#111827',
          colorBackground: '#FFFFFF',
          colorInputBackground: '#FFFFFF',
          colorInputText: '#111827',
          borderRadius: '12px',
          fontFamily: 'var(--font-inter), sans-serif',
        },
      }}
    >
      <html lang="en" className={inter.variable}>
        <body>
          <QueryProvider>{children}</QueryProvider>
          <Toaster
            position="top-right"
            toastOptions={{
              style: {
                borderRadius: '12px',
              },
            }}
          />
        </body>
      </html>
    </ClerkProvider>
  );
}
