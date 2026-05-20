'use client';

import { useState } from 'react';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useOrganization } from '@clerk/nextjs';
import { toast } from 'sonner';
import { Trash2 } from 'lucide-react';

const ROLES = [
  { value: 'org:bookkeeper', label: 'Bookkeeper' },
  { value: 'org:approver', label: 'Approver' },
  { value: 'org:viewer', label: 'Viewer' },
];

export function StepInvite() {
  const store = useOnboardingStore();
  const { organization } = useOrganization();
  const [email, setEmail] = useState('');
  const [role, setRole] = useState('org:bookkeeper');
  const [inviting, setInviting] = useState(false);

  const sendInvite = async () => {
    if (!email || !organization) return;
    setInviting(true);
    try {
      await organization.inviteMember({
        emailAddress: email,
        role,
      });
      store.setField('invited_team', [
        ...store.invited_team,
        { email, role },
      ]);
      setEmail('');
      toast.success(`Invite sent to ${email}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Invite failed';
      toast.error(msg);
    } finally {
      setInviting(false);
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">Invite your team</h2>
        <p className="text-slate">
          Optional — you can skip this and invite people later from Settings →
          Team. Approvers can sign off on month-end close; viewers see reports
          but can&apos;t change data.
        </p>
      </div>
      <div className="grid grid-cols-[1fr_auto_auto] gap-2 items-end">
        <div>
          <Label htmlFor="invite_email">Email</Label>
          <Input
            id="invite_email"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="bookkeeper@dealer.ca"
          />
        </div>
        <div className="w-44">
          <Label htmlFor="invite_role">Role</Label>
          <Select value={role} onValueChange={setRole}>
            <SelectTrigger id="invite_role">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {ROLES.map((r) => (
                <SelectItem key={r.value} value={r.value}>
                  {r.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <Button
          type="button"
          variant="accent"
          onClick={sendInvite}
          disabled={!email || inviting || !organization}
        >
          {inviting ? 'Sending…' : 'Invite'}
        </Button>
      </div>
      {store.invited_team.length > 0 && (
        <div className="border border-border rounded-xl">
          <ul className="divide-y divide-border">
            {store.invited_team.map((entry, idx) => (
              <li key={idx} className="flex items-center justify-between px-4 py-2">
                <div>
                  <div className="text-sm font-medium text-ink">{entry.email}</div>
                  <div className="text-xs text-slate">
                    {ROLES.find((r) => r.value === entry.role)?.label ?? entry.role}
                  </div>
                </div>
                <button
                  className="rounded-md p-1 text-slate hover:bg-cloud"
                  onClick={() =>
                    store.setField(
                      'invited_team',
                      store.invited_team.filter((_, i) => i !== idx),
                    )
                  }
                  aria-label="Remove invite"
                >
                  <Trash2 className="h-4 w-4" strokeWidth={1.5} />
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="flex justify-between pt-4">
        <Button type="button" variant="ghost" onClick={() => store.goTo('payroll')}>
          Back
        </Button>
        <div className="flex gap-2">
          <Button
            type="button"
            variant="secondary"
            onClick={() => store.goTo('billing')}
          >
            Skip
          </Button>
          <Button type="button" onClick={() => store.goTo('billing')}>
            Continue
          </Button>
        </div>
      </div>
    </div>
  );
}
