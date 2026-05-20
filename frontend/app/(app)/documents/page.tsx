'use client';

import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useEntityStore } from '@/lib/store/entity';
import { listDocuments, type DocumentSummary } from '@/lib/api/documents';
import { formatDate, formatMonthLabel } from '@/lib/utils';
import { File, FileText, FileSpreadsheet, Grid3x3, List } from 'lucide-react';

const DOC_TYPE_LABEL: Record<DocumentSummary['doc_type'], string> = {
  'bank-pdf': 'Bank PDF',
  'bank-csv': 'Bank CSV',
  'hh-ap-statement': 'HH AP Statement',
  'pos-financial': 'POS Financial',
  'inventory-adjustment': 'Inventory Adj',
  'inventory-value': 'Inventory Value',
  'aged-ar': 'Aged AR',
  'ar-adjustment': 'AR Adjustment',
  'gl-export': 'GL Export',
  'payroll-register': 'Payroll Register',
  'payroll-hours': 'Payroll Hours',
};

export default function DocumentsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [view, setView] = useState<'grid' | 'list'>('grid');
  const [typeFilter, setTypeFilter] = useState<string>('all');

  const docs = useQuery({
    queryKey: ['documents', entityCode],
    enabled: !!entityCode,
    queryFn: () => listDocuments(entityCode!),
  });

  const filtered = useMemo(() => {
    if (!docs.data) return [];
    return docs.data.documents.filter((d) =>
      typeFilter === 'all' ? true : d.doc_type === typeFilter,
    );
  }, [docs.data, typeFilter]);

  return (
    <>
      <Topbar title="Documents" />
      <main className="p-6 space-y-4">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <Select value={typeFilter} onValueChange={setTypeFilter}>
              <SelectTrigger className="w-56">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All types</SelectItem>
                {Object.entries(DOC_TYPE_LABEL).map(([k, v]) => (
                  <SelectItem key={k} value={k}>
                    {v}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex gap-1">
            <Button
              variant={view === 'grid' ? 'secondary' : 'ghost'}
              size="icon"
              onClick={() => setView('grid')}
              aria-label="Grid view"
            >
              <Grid3x3 className="h-4 w-4" strokeWidth={1.5} />
            </Button>
            <Button
              variant={view === 'list' ? 'secondary' : 'ghost'}
              size="icon"
              onClick={() => setView('list')}
              aria-label="List view"
            >
              <List className="h-4 w-4" strokeWidth={1.5} />
            </Button>
          </div>
        </div>

        {docs.isLoading ? (
          <Skeleton className="h-64" />
        ) : !filtered.length ? (
          <Card className="p-8 text-center">
            <p className="text-slate">No documents.</p>
          </Card>
        ) : view === 'grid' ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
            {filtered.map((d) => (
              <Card key={d.id} className="p-4">
                <div className="flex items-start gap-3">
                  <div className="grid h-10 w-10 place-items-center rounded-md bg-cloud text-ledger-blue shrink-0">
                    <DocIcon type={d.doc_type} />
                  </div>
                  <div className="min-w-0">
                    <div className="text-xs">
                      <Badge variant="info">{DOC_TYPE_LABEL[d.doc_type]}</Badge>
                    </div>
                    <div
                      className="font-semibold text-deep-navy text-sm mt-1 truncate"
                      title={d.file_name}
                    >
                      {d.file_name}
                    </div>
                    <div className="text-xs text-slate">
                      {formatDate(d.uploaded_at)}
                      {d.uploaded_by && ` · ${d.uploaded_by}`}
                    </div>
                  </div>
                </div>
              </Card>
            ))}
          </div>
        ) : (
          <Card>
            <CardContent className="p-0">
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">File</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Uploaded</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">By</th>
                    <th className="text-right font-semibold text-deep-navy px-4 py-2">Records</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {filtered.map((d) => (
                    <tr key={d.id} className="hover:bg-cloud">
                      <td className="px-4 py-2">
                        <Badge variant="info">{DOC_TYPE_LABEL[d.doc_type]}</Badge>
                      </td>
                      <td className="px-4 py-2 text-ink">{d.file_name}</td>
                      <td className="px-4 py-2 text-slate">{formatMonthLabel(d.uploaded_at)}</td>
                      <td className="px-4 py-2 text-slate">{d.uploaded_by ?? '—'}</td>
                      <td className="px-4 py-2 tabular-nums text-right text-slate">
                        {d.parsed_record_count ?? '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        )}

        <p className="text-xs text-slate">
          PDF viewing is not yet supported — documents are parsed at upload and
          the source file isn&apos;t archived on the server.
        </p>
      </main>
    </>
  );
}

function DocIcon({ type }: { type: DocumentSummary['doc_type'] }) {
  if (type === 'gl-export' || type === 'aged-ar' || type === 'ar-adjustment') {
    return <FileSpreadsheet className="h-5 w-5" strokeWidth={1.5} />;
  }
  if (type === 'payroll-register' || type === 'payroll-hours') {
    return <FileText className="h-5 w-5" strokeWidth={1.5} />;
  }
  return <File className="h-5 w-5" strokeWidth={1.5} />;
}
