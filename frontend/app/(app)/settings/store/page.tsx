'use client';

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { updateEntity } from '@/lib/api/entities';
import { toast } from 'sonner';
import { useIsAdmin } from '@/lib/store/user';

export default function StoreSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const entityName = useEntityStore((s) => s.activeEntityName);
  const isAdmin = useIsAdmin();
  const [name, setName] = useState(entityName ?? '');
  const [saving, setSaving] = useState(false);

  const onSave = async () => {
    if (!entityCode) return;
    setSaving(true);
    try {
      await updateEntity(entityCode, { entity_name: name });
      toast.success('Saved');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Store settings</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4 max-w-md">
        <div>
          <Label htmlFor="code">Store number</Label>
          <Input id="code" value={entityCode ?? ''} disabled />
          <p className="text-xs text-slate mt-1">
            Store number is set at signup and can&apos;t be changed.
          </p>
        </div>
        <div>
          <Label htmlFor="name">Store name</Label>
          <Input
            id="name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={!isAdmin}
          />
        </div>
        {isAdmin && (
          <Button onClick={onSave} disabled={saving || name === entityName}>
            {saving ? 'Saving…' : 'Save'}
          </Button>
        )}
        {!isAdmin && (
          <p className="text-xs text-slate">Only admins can edit store details.</p>
        )}
      </CardContent>
    </Card>
  );
}
