'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Plus } from 'lucide-react';

// TODO: backend endpoint not built — chart of accounts CRUD.
// The backend uses entity_code-scoped queries against gl_account_balances
// rows derived from QBO imports, but doesn't yet expose an app-native COA
// admin endpoint. Populate this with the latest TB rows once available.
const PLACEHOLDER_ACCOUNTS = [
  { code: '1020', name: 'TD Operating', type: 'Asset', normal: 'Debit' },
  { code: '1085', name: 'Accounts Receivable', type: 'Asset', normal: 'Debit' },
  { code: '1120', name: 'Inventory', type: 'Asset', normal: 'Debit' },
  { code: '2020', name: 'Accounts Payable', type: 'Liability', normal: 'Credit' },
  { code: '2030', name: 'HH AP Clearing', type: 'Liability', normal: 'Credit' },
  { code: '2300', name: 'HST Payable', type: 'Liability', normal: 'Credit' },
  { code: '2320', name: 'CRA Payable', type: 'Liability', normal: 'Credit' },
  { code: '4010', name: 'Lumber Sales', type: 'Revenue', normal: 'Credit' },
  { code: '6120', name: 'Wages & Benefits', type: 'Expense', normal: 'Debit' },
  { code: '6510', name: 'Rent', type: 'Expense', normal: 'Debit' },
];

export default function AccountsPage() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <span>Chart of accounts</span>
          <Button variant="accent" size="sm" disabled>
            <Plus className="h-4 w-4" strokeWidth={1.5} />
            Add account
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <table className="min-w-full text-sm">
          <thead className="bg-cloud">
            <tr>
              <th className="text-left font-semibold text-deep-navy px-4 py-2">Code</th>
              <th className="text-left font-semibold text-deep-navy px-4 py-2">Name</th>
              <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
              <th className="text-left font-semibold text-deep-navy px-4 py-2">Normal balance</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {PLACEHOLDER_ACCOUNTS.map((a) => (
              <tr key={a.code} className="hover:bg-cloud">
                <td className="px-4 py-2 font-mono text-xs text-slate">{a.code}</td>
                <td className="px-4 py-2 text-ink">{a.name}</td>
                <td className="px-4 py-2 text-slate">{a.type}</td>
                <td className="px-4 py-2 text-slate">{a.normal}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="text-xs text-slate p-4">
          This list will populate from your latest GL import once the app-native
          chart-of-accounts endpoint lands.
        </p>
      </CardContent>
    </Card>
  );
}
