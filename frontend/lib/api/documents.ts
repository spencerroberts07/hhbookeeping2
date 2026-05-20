/**
 * Documents — partial frontend surface.
 *
 * Backend stores parsed metadata per document (hh_ap_documents,
 * gl_import_runs, payroll register PDFs, bank PDF runs, POS import runs)
 * but does NOT yet expose a unified GET /api/documents endpoint. This
 * module aggregates from the existing per-module endpoints to produce a
 * library view. PDF viewing is not yet supported (files aren't archived
 * on the server) — the library shows metadata only.
 */
import { listGlRuns } from './gl';
import { listPosRuns } from './pos';
import { listPayrollRuns } from './payroll';

export interface DocumentSummary {
  id: string;
  doc_type:
    | 'bank-pdf'
    | 'bank-csv'
    | 'hh-ap-statement'
    | 'pos-financial'
    | 'inventory-adjustment'
    | 'inventory-value'
    | 'aged-ar'
    | 'ar-adjustment'
    | 'gl-export'
    | 'payroll-register'
    | 'payroll-hours';
  file_name: string;
  uploaded_at: string;
  uploaded_by: string | null;
  parsed_record_count: number | null;
  linked_journal_count: number | null;
  pdf_available: false; // PDF storage not yet implemented
}

// TODO: backend endpoint not built — GET /api/documents (unified list).
// Until then, aggregate from per-module endpoints. This loses some metadata
// (e.g. uploaded-by isn't on every endpoint).
export async function listDocuments(entityCode: string): Promise<{
  documents: DocumentSummary[];
}> {
  const [gl, payroll, pos] = await Promise.all([
    listGlRuns(entityCode).catch(() => ({ runs: [] })),
    listPayrollRuns({ entity_code: entityCode }).catch(() => ({ runs: [] })),
    listPosRuns({ entity_code: entityCode }).catch(() => ({ runs: [] })),
  ]);

  const docs: DocumentSummary[] = [];

  for (const run of gl.runs) {
    docs.push({
      id: run.id,
      doc_type: 'gl-export',
      file_name: run.file_name,
      uploaded_at: run.imported_at,
      uploaded_by: null,
      parsed_record_count: run.transaction_count,
      linked_journal_count: null,
      pdf_available: false,
    });
  }
  for (const run of payroll.runs) {
    docs.push({
      id: run.id,
      doc_type: 'payroll-register',
      file_name: `Payroll Run ${run.pay_run_number}`,
      uploaded_at: run.created_at,
      uploaded_by: null,
      parsed_record_count: null,
      linked_journal_count: null,
      pdf_available: false,
    });
  }
  for (const run of pos.runs) {
    const docType: DocumentSummary['doc_type'] =
      run.report_type === 'pos_financial'
        ? 'pos-financial'
        : run.report_type === 'inventory_adjustment'
          ? 'inventory-adjustment'
          : run.report_type === 'inventory_value'
            ? 'inventory-value'
            : run.report_type === 'aged_ar'
              ? 'aged-ar'
              : run.report_type === 'ar_adjustment'
                ? 'ar-adjustment'
                : 'pos-financial';
    docs.push({
      id: run.id,
      doc_type: docType,
      file_name: run.file_name,
      uploaded_at: run.imported_at,
      uploaded_by: run.actor_email,
      parsed_record_count: null,
      linked_journal_count: null,
      pdf_available: false,
    });
  }

  docs.sort(
    (a, b) =>
      new Date(b.uploaded_at).getTime() - new Date(a.uploaded_at).getTime(),
  );
  return { documents: docs };
}
