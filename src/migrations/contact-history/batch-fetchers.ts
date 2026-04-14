/**
 * Batch fetch functions for contact history migration
 * These functions fetch data for multiple care recipients at once (1 query instead of N queries)
 */

interface HistoryEvent {
  type: 'call' | 'text' | 'inquiry' | 'contact_merge' | 'formal_affirmation' | 'lead_send';
  timestamp: Date;
  description: string;
  sourceId: number;
  sourceTable: string;
  careRecipientId: number;
}

/**
 * Build description for call event
 */
function buildCallDescription(row: any): string {
  const direction = row.inbound ? 'Inbound' : 'Outbound';
  const duration = row.duration_in_seconds
    ? `${Math.floor(row.duration_in_seconds / 60)}m ${row.duration_in_seconds % 60}s`
    : '0s';
  const disposition = row.disposition_label || 'No disposition';
  const agent = row.agent_name || row.agent_email || 'Unknown';

  return `${direction} call - ${disposition} - ${duration} - ${agent}`;
}

/**
 * Build description for text event
 */
function buildTextDescription(row: any): string {
  const direction = row.direction === 0 ? 'Received' : 'Sent';
  const preview = row.content
    ? row.content.substring(0, 40) + (row.content.length > 40 ? '...' : '')
    : 'No content';
  const status = row.status || '';

  return `${direction} text - ${status} - "${preview}"`;
}

/**
 * Build description for inquiry event
 */
function buildInquiryDescription(row: any): string {
  const inquiryType = row.type || 'Inquiry';
  const resourceType = row.resource_type_name || 'Unknown';
  const location = row.location || '';

  return `${inquiryType} - ${resourceType} - ${location}`.trim();
}

/**
 * Build description for inquiry log event
 */
function buildInquiryLogDescription(row: any): string {
  if (row.message.startsWith('Two contacts found')) {
    return 'Contact merge - Duplicate contacts merged';
  } else if (row.message.startsWith('Medical Alert')) {
    return 'Medical Alert - Contact flagged';
  }
  return row.message.substring(0, 80);
}

/**
 * Build description for formal affirmation event
 */
function buildAffirmationDescription(row: any): string {
  const kind = row.kind || 'Affirmation';
  const consent = row.consent ? 'Consented' : 'Declined';
  const agent = row.agent_name || row.agent_email || 'Unknown';

  return `${kind} - ${consent} - ${agent}`;
}

/**
 * Build description for lead send event
 */
function buildLeadSendDescription(row: any): string {
  const leadType = row.type || 'Lead';
  const propertyName = row.property_name || 'Unknown provider';
  const resourceType = row.resource_type_name || '';
  const status = row.status || '';

  return `${leadType} sent to ${propertyName} - ${resourceType} - ${status}`.trim();
}

/**
 * Fetch calls for batch of care recipients
 */
export async function fetchCallsForBatch(mysqlConn: any, careRecipientIds: number[]): Promise<HistoryEvent[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT 
      ccc.id,
      ccc.completed_at,
      ccc.duration_in_seconds,
      ccc.inbound,
      d.label as disposition_label,
      a.full_name as agent_name,
      a.email as agent_email,
      c.care_recipient_id
    FROM call_center_calls ccc
    JOIN contacts c ON c.id = ccc.target_id AND ccc.target_type = 'Contact'
    LEFT JOIN dispositions d ON d.id = ccc.disposition_id
    LEFT JOIN accounts a ON a.id = ccc.account_id
    WHERE c.care_recipient_id IN (?)
      AND ccc.interlocutor = 'consumer'
      AND ccc.completed_at IS NOT NULL
      AND c.deleted_at IS NULL
    ORDER BY ccc.completed_at DESC
    `,
    [careRecipientIds]
  );

  return rows.map((row: any) => ({
    type: 'call' as const,
    timestamp: row.completed_at,
    description: buildCallDescription(row),
    sourceId: row.id,
    sourceTable: 'call_center_calls',
    careRecipientId: row.care_recipient_id,
  }));
}

/**
 * Fetch texts for batch of care recipients
 */
export async function fetchTextsForBatch(mysqlConn: any, careRecipientIds: number[]): Promise<HistoryEvent[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT 
      cct.id,
      cct.created_at,
      cct.direction,
      cct.content,
      cct.status,
      c.care_recipient_id
    FROM call_center_texts cct
    JOIN contacts c ON c.id = cct.target_id AND cct.target_type = 'Contact'
    WHERE c.care_recipient_id IN (?)
      AND c.deleted_at IS NULL
    ORDER BY cct.created_at DESC
    `,
    [careRecipientIds]
  );

  return rows.map((row: any) => ({
    type: 'text' as const,
    timestamp: row.created_at,
    description: buildTextDescription(row),
    sourceId: row.id,
    sourceTable: 'call_center_texts',
    careRecipientId: row.care_recipient_id,
  }));
}

/**
 * Fetch inquiries for batch of care recipients
 */
export async function fetchInquiriesForBatch(mysqlConn: any, careRecipientIds: number[]): Promise<HistoryEvent[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT
      i.id,
      i.created_at,
      i.type,
      i.location,
      lrt.name as resource_type_name,
      c.care_recipient_id
    FROM inquiries i
    JOIN contacts c ON c.id = i.contact_id
    LEFT JOIN local_resource_types lrt ON lrt.id = i.local_resource_type_id
    WHERE c.care_recipient_id IN (?)
      AND i.type != 'AgentInquiry'
      AND c.deleted_at IS NULL
    ORDER BY i.created_at DESC
    `,
    [careRecipientIds]
  );

  return rows.map((row: any) => ({
    type: 'inquiry' as const,
    timestamp: row.created_at,
    description: buildInquiryDescription(row),
    sourceId: row.id,
    sourceTable: 'inquiries',
    careRecipientId: row.care_recipient_id,
  }));
}

/**
 * Fetch inquiry logs for batch of care recipients
 */
export async function fetchInquiryLogsForBatch(mysqlConn: any, careRecipientIds: number[]): Promise<HistoryEvent[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT
      il.id,
      il.created_at,
      il.message,
      c.care_recipient_id
    FROM inquiry_logs il
    JOIN inquiries i ON i.id = il.inquiry_id
    JOIN contacts c ON c.id = i.contact_id
    WHERE c.care_recipient_id IN (?)
      AND (il.message LIKE 'Two contacts found%' OR il.message LIKE 'Medical Alert%')
      AND c.deleted_at IS NULL
    ORDER BY il.created_at DESC
    `,
    [careRecipientIds]
  );

  return rows.map((row: any) => ({
    type: 'contact_merge' as const,
    timestamp: row.created_at,
    description: buildInquiryLogDescription(row),
    sourceId: row.id,
    sourceTable: 'inquiry_logs',
    careRecipientId: row.care_recipient_id,
  }));
}

/**
 * Fetch formal affirmations for batch of care recipients
 */
export async function fetchFormalAffirmationsForBatch(mysqlConn: any, careRecipientIds: number[]): Promise<HistoryEvent[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT
      fa.id,
      fa.created_at,
      fa.kind,
      fa.consent,
      a.full_name as agent_name,
      a.email as agent_email,
      c.care_recipient_id
    FROM formal_affirmations fa
    JOIN contacts c ON c.id = fa.contact_id
    LEFT JOIN accounts a ON a.id = fa.account_id
    WHERE c.care_recipient_id IN (?)
      AND c.deleted_at IS NULL
    ORDER BY fa.created_at DESC
    `,
    [careRecipientIds]
  );

  return rows.map((row: any) => ({
    type: 'formal_affirmation' as const,
    timestamp: row.created_at,
    description: buildAffirmationDescription(row),
    sourceId: row.id,
    sourceTable: 'formal_affirmations',
    careRecipientId: row.care_recipient_id,
  }));
}

/**
 * Fetch lead sends for batch of care recipients
 */
export async function fetchLeadSendsForBatch(mysqlConn: any, careRecipientIds: number[]): Promise<HistoryEvent[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT
      lrl.id,
      lrl.sent_at,
      lrl.status,
      lrl.type,
      lp.name as property_name,
      lrt.name as resource_type_name,
      c.care_recipient_id
    FROM local_resource_leads lrl
    JOIN inquiries i ON i.id = lrl.inquiry_id
    JOIN contacts c ON c.id = i.contact_id
    LEFT JOIN local_resources lr ON lr.id = lrl.local_resource_id
    LEFT JOIN local_properties lp ON lp.id = lr.local_property_id
    LEFT JOIN local_resource_types lrt ON lrt.id = lr.local_resource_type_id
    WHERE c.care_recipient_id IN (?)
      AND lrl.sent_at IS NOT NULL
      AND c.deleted_at IS NULL
    ORDER BY lrl.sent_at DESC
    `,
    [careRecipientIds]
  );

  return rows.map((row: any) => ({
    type: 'lead_send' as const,
    timestamp: row.sent_at,
    description: buildLeadSendDescription(row),
    sourceId: row.id,
    sourceTable: 'local_resource_leads',
    careRecipientId: row.care_recipient_id,
  }));
}

