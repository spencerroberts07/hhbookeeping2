'use client';

import { useState } from 'react';
import { useOrganization } from '@clerk/nextjs';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Trash2 } from 'lucide-react';
import { useIsAdmin } from '@/lib/store/user';
import { toast } from 'sonner';

const ROLES = [
  { value: 'org:admin', label: 'Admin' },
  { value: 'org:approver', label: 'Approver' },
  { value: 'org:bookkeeper', label: 'Bookkeeper' },
  { value: 'org:viewer', label: 'Viewer' },
];

export default function TeamSettingsPage() {
  const { organization, memberships } = useOrganization({
    memberships: { infinite: true },
  });
  const isAdmin = useIsAdmin();
  const [email, setEmail] = useState('');
  const [role, setRole] = useState('org:bookkeeper');
  const [inviting, setInviting] = useState(false);

  const sendInvite = async () => {
    if (!organization || !email) return;
    setInviting(true);
    try {
      await organization.inviteMember({ emailAddress: email, role });
      setEmail('');
      toast.success(`Invite sent to ${email}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Invite failed';
      toast.error(msg);
    } finally {
      setInviting(false);
    }
  };

  const removeMember = async (membershipId: string) => {
    if (!organization) return;
    try {
      const m = memberships?.data?.find((mm) => mm.id === membershipId);
      if (!m) return;
      await m.destroy();
      toast.success('Member removed');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Remove failed';
      toast.error(msg);
    }
  };

  return (
    <div className="space-y-4">
      {isAdmin && (
        <Card>
          <CardHeader>
            <CardTitle>Invite a team member</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-[1fr_180px_auto] gap-2 items-end">
              <div>
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="staff@dealer.ca"
                />
              </div>
              <div>
                <Label htmlFor="role">Role</Label>
                <Select value={role} onValueChange={setRole}>
                  <SelectTrigger id="role">
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
                variant="accent"
                onClick={sendInvite}
                disabled={!email || inviting}
              >
                {inviting ? 'Sending…' : 'Send invite'}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Current members</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {!memberships?.data ? (
            <Skeleton className="h-32 m-4" />
          ) : (
            <table className="min-w-full text-sm">
              <thead className="bg-cloud">
                <tr>
                  <th className="text-left font-semibold text-deep-navy px-4 py-2">
                    Name
                  </th>
                  <th className="text-left font-semibold text-deep-navy px-4 py-2">
                    Email
                  </th>
                  <th className="text-left font-semibold text-deep-navy px-4 py-2">
                    Role
                  </th>
                  {isAdmin && <th className="px-4 py-2" />}
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {memberships.data.map((m) => (
                  <tr key={m.id} className="hover:bg-cloud">
                    <td className="px-4 py-2 text-ink">
                      {m.publicUserData?.firstName} {m.publicUserData?.lastName}
                    </td>
                    <td className="px-4 py-2 text-slate">
                      {m.publicUserData?.identifier}
                    </td>
                    <td className="px-4 py-2">
                      <Badge variant="info">
                        {ROLES.find((r) => r.value === m.role)?.label ?? m.role}
                      </Badge>
                    </td>
                    {isAdmin && (
                      <td className="px-4 py-2 text-right">
                        <button
                          aria-label="Remove member"
                          onClick={() => removeMember(m.id)}
                          className="rounded-md p-1 text-slate hover:bg-cloud hover:text-red-700"
                        >
                          <Trash2 className="h-4 w-4" strokeWidth={1.5} />
                        </button>
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
