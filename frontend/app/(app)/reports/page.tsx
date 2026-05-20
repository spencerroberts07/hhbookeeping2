'use client';

import Link from 'next/link';
import {
  FileText,
  Scale,
  BookOpen,
  ClipboardList,
  Users,
  CircleDollarSign,
  LineChart,
} from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';

const REPORTS = [
  { href: '/reports/income-statement', label: 'Income Statement', icon: LineChart, desc: 'Revenue, COGS, expenses, net income' },
  { href: '/reports/balance-sheet', label: 'Balance Sheet', icon: Scale, desc: 'Assets, liabilities, equity as of a date' },
  { href: '/reports/general-ledger', label: 'General Ledger', icon: BookOpen, desc: 'Per-account transaction history' },
  { href: '/reports/trial-balance', label: 'Trial Balance', icon: ClipboardList, desc: 'All accounts at a point in time' },
  { href: '/reports/ar-aging', label: 'AR Aging', icon: Users, desc: 'Customer balances by age bucket' },
  { href: '/reports/ap-aging', label: 'AP Aging — HH', icon: CircleDollarSign, desc: 'HH AP outstanding by due date' },
  { href: '/reports/payroll', label: 'Payroll Summary', icon: FileText, desc: 'Per pay-period gross/deductions/net' },
];

export default function ReportsIndex() {
  return (
    <>
      <Topbar title="Reports" />
      <main className="p-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {REPORTS.map((r) => {
            const Icon = r.icon;
            return (
              <Link key={r.href} href={r.href}>
                <Card className="p-4 hover:shadow-md transition cursor-pointer h-full">
                  <CardHeader className="p-0 mb-2">
                    <div className="flex items-center gap-3">
                      <div className="grid h-10 w-10 place-items-center rounded-lg bg-cloud text-ledger-blue">
                        <Icon className="h-5 w-5" strokeWidth={1.5} />
                      </div>
                      <CardTitle className="text-base">{r.label}</CardTitle>
                    </div>
                  </CardHeader>
                  <CardContent className="p-0">
                    <p className="text-sm text-slate">{r.desc}</p>
                  </CardContent>
                </Card>
              </Link>
            );
          })}
        </div>
      </main>
    </>
  );
}
