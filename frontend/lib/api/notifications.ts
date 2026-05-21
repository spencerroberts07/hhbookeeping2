import { api } from './client';

export interface NotificationPreferences {
  month_end_reminders: boolean;
  variance_alerts: boolean;
  approval_requests: boolean;
  payment_receipts: boolean;
}

export async function getNotificationPreferences(
  entityCode: string,
): Promise<{ entity_code: string; notification_preferences: NotificationPreferences }> {
  const res = await api.get(`/api/entities/${entityCode}/notifications`);
  return res.data;
}

export async function updateNotificationPreferences(
  entityCode: string,
  patch: Partial<NotificationPreferences>,
): Promise<{ entity_code: string; notification_preferences: NotificationPreferences }> {
  const res = await api.patch(
    `/api/entities/${entityCode}/notifications`,
    patch,
  );
  return res.data;
}
