'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { getGeneralLedgerReport } from '@/lib/api/reports';
import { listAccounts } from '@/lib/api/accounts';
import { formatMoney, formatDate } from '@/lib/utils';
import { useDrillDown } from '@/components/reports/drill-down/use-drill-down';

// Live app-native General Ledger — backed by /api/reports/general-ledger
// (journal_lines, posted-only). Replaces the previous gl-import-runs
// snapshot reader.
export default function GeneralLedgerPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const today = new Date();
  const ninetyAgo = new Date(today.getTime() - 90 * 24 * 60 * 60 * 1000);
  const [accountCode, setAccountCode] = useState('');
  const [dateFrom, setDateFrom] = useState(ninetyAgo.toISOString().slice(0, 10));
  const [dateTo, setDateTo] = useState(today.toISOString().slice(0, 10));
  const { openAt } = useDrillDown();

  const accounts = useQuery({
    queryKey: ['accounts', entityCode],
    enabled: !!entityCode,
    queryFn: () => listAccounts(entityCode!),
  });

  const report = useQuery({
    queryKey: ['gl-report', entityCode, accountCode, dateFrom, dateTo],
    enabled: !!entityCode && !!accountCode,
    queryFn: () =>
      getGeneralLedgerReport({
        entity_code: entityCode!,
        account_code: accountCode,
        date_from: dateFrom,
        date_to: dateTo,
      }),
  });

  return (
    <ReportShell
      title="General Ledger"
      subtitle={
        report.data
          ? `${report.data.account_code} — ${report.data.account_name}`
          : 'Live ledger — backed by journal_lines'
      }
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4 max-w-2xl no-print">
        <div>
          <Label htmlFor="acct">Account</Label>
          <select
            id="acct"
            value={accountCode}
            onChange={(e) => setAccountCode(e.target.value)}
            className="w-full rounded-md border border-input bg-white px-3 py-2 text-sm"
          >
            <option value="">Pick an account…</option>
            {(accounts.data?.accounts ?? []).map((a) => (
              <option key={a.code} value={a.code}>
                {a.code} · {a.name}
              </option>
            ))}
          </select>
        </div>
        <div>
          <Label htmlFor="df">From</Label>
          <Input
            id="df"
            type="date"
            value={dateFrom}
            onChange={(e) => setDateFrom(e.target.value)}
          />
        </div>
        <div>
          <Label htmlFor="dt">To</Label>
          <Input
            id="dt"
            type="date"
            value={dateTo}
            onChange={(e) => setDateTo(e.target.value)}
          />
        </div>
      </div>

      {!accountCode ? (
        <p className="text-slate">Pick an account to view its transactions.</p>
      ) : report.isLoading ? (
        <Skeleton className="h-96" />
      ) : !report.data ? (
        <p className="text-slate">No data.</p>
      ) : (
        <>
          <div className="flex gap-6 mb-3 text-sm tabular-nums">
            <div>
              <span className="text-slate">Opening:</span>{' '}
              <span className="font-semibold text-deep-navy">
                {formatMoney(report.data.opening_balance)}
              </span>
            </div>
            <div>
              <span className="text-slate">Closing:</span>{' '}
              <span className="font-semibold text-deep-navy">
                {formatMoney(report.data.closing_balance)}
              </span>
            </div>
            <div>
              <span className="text-slate">Transactions:</span>{' '}
              <span className="font-semibold text-deep-navy">
                {report.data.transaction_count}
              </span>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-cloud">
                <tr>
                  <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                  <th className="text-left font-semibold text-deep-navy px-4 py-2">Description</th>
                  <th className="text-left font-semibold text-deep-navy px-4 py-2">Ref</th>
                  <th className="text-right font-semibold text-deep-navy px-4 py-2">Debit</th>
                  <th className="text-right font-semibold text-deep-navy px-4 py-2">Credit</th>
                  <th className="text-right font-semibold text-deep-navy px-4 py-2">Balance</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {report.data.transactions.map((t) => (
                  <tr
                    key={t.id}
                    className="cursor-pointer hover:bg-cloud"
                    onClick={() =>
                      openAt({ kind: 'entry', journal_batch_id: t.journal_batch_id })
                    }
                    title="View full journal entry"
                  >
                    <td className="px-4 py-2 text-ink">{formatDate(t.posting_date)}</td>
                    <td className="px-4 py-2 text-ink">{t.description}</td>
                    <td className="px-4 py-2 text-slate font-mono text-xs">{t.reference}</td>
                    <td className="px-4 py-2 tabular-nums text-right text-ink">
                      {t.debit ? formatMoney(t.debit) : '—'}
                    </td>
                    <td className="px-4 py-2 tabular-nums text-right text-ink">
                      {t.credit ? formatMoney(t.credit) : '—'}
                    </td>
                    <td className="px-4 py-2 tabular-nums text-right font-semibold text-deep-navy">
                      {formatMoney(t.balance)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </ReportShell>
  );
}
