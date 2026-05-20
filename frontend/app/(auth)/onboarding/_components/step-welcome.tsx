'use client';

import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { createEntity } from '@/lib/api/entities';
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
import { toast } from 'sonner';
import type { AxiosError } from 'axios';

const PROVINCES = [
  'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
];

const schema = z.object({
  store_number: z.string().min(2).max(20),
  store_name: z.string().min(1),
  province: z.string().min(2).max(4),
  fiscal_year_end_month: z.coerce.number().int().min(1).max(12),
  fiscal_year_end_day: z.coerce.number().int().min(1).max(31),
});
type FormValues = z.infer<typeof schema>;

export function StepWelcome() {
  const store = useOnboardingStore();
  const [submitting, setSubmitting] = useState(false);

  const {
    register,
    handleSubmit,
    setValue,
    watch,
    formState: { errors },
  } = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: {
      store_number: store.store_number,
      store_name: store.store_name,
      province: store.province,
      fiscal_year_end_month: store.fiscal_year_end_month,
      fiscal_year_end_day: store.fiscal_year_end_day,
    },
  });

  const province = watch('province');

  const onSubmit = async (values: FormValues) => {
    setSubmitting(true);
    try {
      store.setField('store_number', values.store_number);
      store.setField('store_name', values.store_name);
      store.setField('province', values.province);
      store.setField('fiscal_year_end_month', values.fiscal_year_end_month);
      store.setField('fiscal_year_end_day', values.fiscal_year_end_day);

      const entity = await createEntity({
        entity_code: values.store_number,
        entity_name: values.store_name,
        fiscal_year_end_month: values.fiscal_year_end_month,
        fiscal_year_end_day: values.fiscal_year_end_day,
        province: values.province,
        base_currency: 'CAD',
      });
      store.setField('entity_code', entity.entity_code);
      store.goTo('bank');
      toast.success(`Entity ${entity.entity_code} created`);
    } catch (err) {
      const axiosErr = err as AxiosError<{ detail?: string }>;
      const status = axiosErr.response?.status;
      const detail = axiosErr.response?.data?.detail;
      // 409 conflict — entity already exists. We tolerate and move on, since
      // the user may be resuming onboarding after a crash.
      if (status === 409) {
        store.setField('entity_code', values.store_number);
        store.goTo('bank');
      } else {
        toast.error(detail ?? 'Could not create entity');
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">
          Welcome to BookWize
        </h2>
        <p className="text-slate">
          Tell us about your Home Hardware store. This becomes your primary
          entity in the system — every transaction will be scoped to it.
        </p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <Label htmlFor="store_number">Store number</Label>
          <Input
            id="store_number"
            placeholder="1877-8"
            {...register('store_number')}
            aria-invalid={!!errors.store_number}
          />
          {errors.store_number && (
            <p className="text-xs text-red-600 mt-1">
              {errors.store_number.message}
            </p>
          )}
        </div>
        <div>
          <Label htmlFor="store_name">Store name</Label>
          <Input
            id="store_name"
            placeholder="Bridlewood Home Hardware"
            {...register('store_name')}
            aria-invalid={!!errors.store_name}
          />
          {errors.store_name && (
            <p className="text-xs text-red-600 mt-1">
              {errors.store_name.message}
            </p>
          )}
        </div>
        <div>
          <Label htmlFor="province">Province</Label>
          <Select
            value={province}
            onValueChange={(v) => setValue('province', v)}
          >
            <SelectTrigger id="province">
              <SelectValue placeholder="Pick a province" />
            </SelectTrigger>
            <SelectContent>
              {PROVINCES.map((p) => (
                <SelectItem key={p} value={p}>
                  {p}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <Label htmlFor="fye_month">Fiscal year-end month</Label>
            <Input
              id="fye_month"
              type="number"
              min={1}
              max={12}
              {...register('fiscal_year_end_month')}
            />
          </div>
          <div>
            <Label htmlFor="fye_day">Day</Label>
            <Input
              id="fye_day"
              type="number"
              min={1}
              max={31}
              {...register('fiscal_year_end_day')}
            />
          </div>
        </div>
      </div>
      <div className="flex justify-end pt-4">
        <Button type="submit" disabled={submitting}>
          {submitting ? 'Creating…' : 'Continue'}
        </Button>
      </div>
    </form>
  );
}
