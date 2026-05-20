'use client';

import { useState } from 'react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useEntityStore } from '@/lib/store/entity';
import { PeriodSelector } from './_components/period-selector';
import { ChecklistStep } from './_components/checklist-step';
import { StepDocuments } from './_components/step-documents';
import { StepAutoClassify } from './_components/step-auto-classify';
import { StepReviewQueue } from './_components/step-review-queue';
import { StepJournals } from './_components/step-journals';
import { StepValidation } from './_components/step-validation';
import { StepClose } from './_components/step-close';
import { formatMonthLabel } from '@/lib/utils';

export default function MonthEndPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  // Default to last calendar month-end.
  const today = new Date();
  const lastMonthEnd = new Date(today.getFullYear(), today.getMonth(), 0);
  const [periodEnd, setPeriodEnd] = useState<string>(
    lastMonthEnd.toISOString().slice(0, 10),
  );

  if (!entityCode) {
    return (
      <>
        <Topbar title="Month-end" />
        <main className="p-6">
          <Card className="p-8 text-center">
            <p className="text-slate">Pick an entity from the switcher.</p>
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar
        title="Month-end close"
        periodLabel={formatMonthLabel(periodEnd)}
      />
      <main className="p-6 space-y-6">
        <Card>
          <CardHeader>
            <CardTitle>Select period</CardTitle>
          </CardHeader>
          <CardContent>
            <PeriodSelector value={periodEnd} onChange={setPeriodEnd} />
          </CardContent>
        </Card>

        <ChecklistStep
          number={1}
          title="Document upload"
          description="Bank, HH AP, POS, inventory, payroll, AR."
        >
          <StepDocuments entityCode={entityCode} periodEnd={periodEnd} />
        </ChecklistStep>

        <ChecklistStep
          number={2}
          title="Auto-classify"
          description="Run rules + memory + AI classification on bank transactions."
        >
          <StepAutoClassify entityCode={entityCode} periodEnd={periodEnd} />
        </ChecklistStep>

        <ChecklistStep
          number={3}
          title="Review queue"
          description="Sign off on flagged classification suggestions."
        >
          <StepReviewQueue entityCode={entityCode} />
        </ChecklistStep>

        <ChecklistStep
          number={4}
          title="Journal review"
          description="Build, preview, and approve each journal type."
        >
          <StepJournals entityCode={entityCode} periodEnd={periodEnd} />
        </ChecklistStep>

        <ChecklistStep
          number={5}
          title="Validation"
          description="POS validation and trial balance variance check."
        >
          <StepValidation entityCode={entityCode} periodEnd={periodEnd} />
        </ChecklistStep>

        <ChecklistStep
          number={6}
          title="Close period"
          description="Lock the period. Admin only."
        >
          <StepClose entityCode={entityCode} periodEnd={periodEnd} />
        </ChecklistStep>
      </main>
    </>
  );
}
