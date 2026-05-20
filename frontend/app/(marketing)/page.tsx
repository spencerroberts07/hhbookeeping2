import Image from 'next/image';
import Link from 'next/link';
import {
  ClipboardCheck,
  Wand2,
  Receipt,
  Calendar,
  ShieldCheck,
  LineChart,
} from 'lucide-react';

const FEATURES = [
  {
    icon: Calendar,
    title: 'Month-end checklist',
    body: 'Six-step workflow built for HH dealers: documents, classify, review, journals, validate, close.',
  },
  {
    icon: Wand2,
    title: 'AI classifier',
    body: 'Rules first, vendor memory second, Claude API third — every bank transaction gets a GL home.',
  },
  {
    icon: Receipt,
    title: 'HH AP done right',
    body: 'Parses your weekly statement, separates dating, books remittance clearing automatically.',
  },
  {
    icon: ClipboardCheck,
    title: 'Two-step approval',
    body: 'Bookkeeper builds, approver signs off. Audit-trail drilldown on every posted line.',
  },
  {
    icon: LineChart,
    title: 'Reports you recognize',
    body: 'Income Statement, Balance Sheet, Trial Balance, AR / AP Aging, Payroll. Print or export.',
  },
  {
    icon: ShieldCheck,
    title: 'Multi-store',
    body: 'Run multiple stores under one login. Each store stays scoped — no data crosses entities.',
  },
];

export default function LandingPage() {
  return (
    <>
      <section className="bg-gradient-primary text-white">
        <div className="container mx-auto px-4 py-20 grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
          <div>
            <h1 className="text-h1 lg:text-display font-extrabold leading-tight">
              Month-end close,{' '}
              <span className="text-bw-teal">automated</span> for Home
              Hardware dealers.
            </h1>
            <p className="text-lg text-white/80 mt-6 max-w-xl">
              Smart bookkeeping. Clearer business. Built for the way HH dealers
              actually work — bank to HH AP to payroll to month-end close, in
              one place.
            </p>
            <div className="flex flex-wrap gap-3 mt-8">
              <Link
                href="/sign-up"
                className="rounded-xl bg-bw-teal text-white font-semibold px-6 py-3 hover:bg-aqua transition"
              >
                Start 30-day free trial
              </Link>
              <Link
                href="/pricing"
                className="rounded-xl border border-white/60 text-white font-semibold px-6 py-3 hover:bg-white/10 transition"
              >
                See pricing
              </Link>
            </div>
          </div>
          <div className="hidden lg:block">
            <Image
              src="/brand/bookwize-icon-reversed.svg"
              alt=""
              width={400}
              height={400}
              className="w-full max-w-md mx-auto opacity-90"
            />
          </div>
        </div>
      </section>

      <section className="bg-cloud py-16 px-4">
        <div className="container mx-auto">
          <h2 className="text-h2 text-deep-navy mb-3 max-w-2xl">
            Month-end without the chaos.
          </h2>
          <p className="text-slate mb-12 max-w-2xl">
            Most dealers spend two days a month fighting spreadsheets just to
            close the books. BookWize replaces that with a guided workflow.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {FEATURES.map((f) => {
              const Icon = f.icon;
              return (
                <div
                  key={f.title}
                  className="bg-white rounded-xl border border-border p-6 shadow-sm"
                >
                  <div className="grid h-10 w-10 place-items-center rounded-lg bg-cloud text-ledger-blue mb-3">
                    <Icon className="h-5 w-5" strokeWidth={1.5} />
                  </div>
                  <h3 className="font-semibold text-deep-navy mb-2">
                    {f.title}
                  </h3>
                  <p className="text-sm text-ink">{f.body}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <section className="bg-white py-20 px-4 border-t border-border">
        <div className="container mx-auto text-center max-w-2xl">
          <h2 className="text-h2 text-deep-navy mb-4">
            Built for Home Hardware dealers.
          </h2>
          <p className="text-slate text-lg mb-8">
            Not generic SaaS bolted onto a P&amp;L — BookWize understands HH AP
            statements, dating, ENetEmployer payroll, and the way dealers
            actually close the books each month.
          </p>
          <Link
            href="/sign-up"
            className="inline-block rounded-xl bg-bw-teal text-white font-semibold px-8 py-4 hover:bg-aqua transition"
          >
            Try BookWize free for 30 days
          </Link>
          <p className="text-xs text-slate mt-3">
            No credit card required to start.
          </p>
        </div>
      </section>
    </>
  );
}
