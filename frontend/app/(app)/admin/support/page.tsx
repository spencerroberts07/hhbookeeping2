'use client';

import { useState } from 'react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { linkEntityToOrg } from '@/lib/api/admin';
import { toast } from 'sonner';

export default function SupportPage() {
  const [entityCode, setEntityCode] = useState('');
  const [clerkOrgId, setClerkOrgId] = useState('');
  const [linking, setLinking] = useState(false);

  const onLink = async () => {
    setLinking(true);
    try {
      await linkEntityToOrg({ entity_code: entityCode, clerk_org_id: clerkOrgId });
      toast.success('Linked');
    } finally {
      setLinking(false);
    }
  };

  return (
    <>
      <Topbar title="Support tools (admin)" />
      <main className="p-6 grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Manual entity ↔ Clerk org mapping</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <p className="text-xs text-slate">
              Bind an existing entity to a Clerk organization. Use this when
              automatic webhook linking didn&apos;t find a match.
            </p>
            <div>
              <Label htmlFor="ec">Entity code</Label>
              <Input
                id="ec"
                value={entityCode}
                onChange={(e) => setEntityCode(e.target.value)}
                placeholder="1877-8"
              />
            </div>
            <div>
              <Label htmlFor="oid">Clerk org id</Label>
              <Input
                id="oid"
                value={clerkOrgId}
                onChange={(e) => setClerkOrgId(e.target.value)}
                placeholder="org_..."
              />
            </div>
            <Button
              onClick={onLink}
              disabled={!entityCode || !clerkOrgId || linking}
            >
              {linking ? 'Linking…' : 'Link'}
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>User lookup</CardTitle>
          </CardHeader>
          <CardContent>
            {/* TODO: backend endpoint not built — admin user search */}
            <p className="text-sm text-slate">
              User lookup by email or Clerk user id — landing endpoint pending.
            </p>
          </CardContent>
        </Card>
      </main>
    </>
  );
}
