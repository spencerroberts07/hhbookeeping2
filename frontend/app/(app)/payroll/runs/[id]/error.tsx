'use client';

import { useEffect } from 'react';
import Link from 'next/link';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { AlertTriangle, ArrowLeft } from 'lucide-react';

export default function PayrollRunDetailError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    // Surface in console BEFORE any other side-effect can navigate away.
    // The previous symptom hid the real error behind a Clerk-driven
    // redirect; this prints the message + stack the moment React
    // catches the throw.
    // eslint-disable-next-line no-console
    console.error('[PayrollRunDetail] runtime error:', error);
  }, [error]);

  return (
    <main className="p-6 space-y-4 max-w-3xl">
      <Link
        href="/payroll"
        className="inline-flex items-center gap-1 text-sm text-slate hover:text-ledger-blue"
      >
        <ArrowLeft className="h-4 w-4" /> Back to payroll
      </Link>
      <Card className="border-red-200">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-red-700">
            <AlertTriangle className="h-5 w-5" />
            Pay run page failed to load
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-sm text-slate">
            The run-detail page hit a runtime error. Message and stack
            below — please paste both into support if it persists.
          </p>
          <pre className="bg-cloud rounded-md p-3 text-xs overflow-x-auto whitespace-pre-wrap">
            {error.message || '(no message)'}
            {error.digest && `\n\nDigest: ${error.digest}`}
            {error.stack && `\n\n${error.stack}`}
          </pre>
          <Button onClick={reset} variant="outline">
            Try again
          </Button>
        </CardContent>
      </Card>
    </main>
  );
}
