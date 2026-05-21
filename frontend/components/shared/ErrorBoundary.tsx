'use client';

import { Component, type ReactNode } from 'react';
import { AlertTriangle, RotateCw } from 'lucide-react';
import { Button } from '@/components/ui/button';

interface Props {
  children: ReactNode;
  /** Optional scoping label shown in dev only (e.g. "Dashboard"). */
  label?: string;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

/**
 * Page-level React error boundary. Keeps one broken component from
 * white-screening the entire app. In production we hide the stack
 * trace; in development we print the message + stack for debugging.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: { componentStack?: string | null }) {
    // Hook into a logger later — for now console keeps it dev-friendly.
    console.error('[ErrorBoundary]', this.props.label || '', error, info);
  }

  reset = () => {
    this.setState({ hasError: false, error: null });
  };

  reload = () => {
    if (typeof window !== 'undefined') {
      window.location.reload();
    }
  };

  render() {
    if (!this.state.hasError) return this.props.children;

    const isDev = process.env.NODE_ENV !== 'production';

    return (
      <div className="m-6 rounded-2xl border-2 border-amber-300 bg-amber-50 p-6 max-w-2xl">
        <div className="flex items-start gap-3">
          <AlertTriangle className="h-6 w-6 text-amber-700 shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <h2 className="text-lg font-bold text-amber-900">
              Something went wrong
            </h2>
            <p className="text-sm text-amber-900/80 mt-1">
              {this.props.label
                ? `The ${this.props.label} section hit an error and couldn't render.`
                : "This section hit an error and couldn't render."}
              {' '}Your data is safe — try reloading the page.
            </p>
            {isDev && this.state.error && (
              <pre className="mt-3 max-h-48 overflow-auto rounded bg-white/60 p-2 text-xs text-amber-900 font-mono whitespace-pre-wrap">
                {this.state.error.message}
                {this.state.error.stack ? `\n\n${this.state.error.stack}` : ''}
              </pre>
            )}
            <div className="mt-4 flex gap-2">
              <Button onClick={this.reload} variant="accent" size="sm">
                <RotateCw className="h-4 w-4" />
                Reload page
              </Button>
              <Button onClick={this.reset} variant="outline" size="sm">
                Try again
              </Button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
