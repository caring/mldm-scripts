#!/usr/bin/env ts-node
/**
 * Preview script to show what data will be migrated for specific leads
 * Usage: ts-node src/migrations/lead-status-tour-history/preview-data.ts <lead_id1> <lead_id2> ...
 */

import { connectMySQL, disconnectMySQL, getMySQLConnection } from '../../db/mysql';
import { connectPostgres, disconnectPostgres, getPostgresClient } from '../../db/postgres';
import {
  buildLeadStatusSummary,
  deriveLeadPriority,
  DirLeadStatus,
} from './lead-status-tour-history';

interface DirLead {
  id: number;
  created_at: Date;
  followup_rank: number | null;
  allowFollowup: boolean | number | string | null;
}

async function previewData() {
  const leadIds = process.argv.slice(2).map(id => parseInt(id, 10));

  if (leadIds.length === 0) {
    console.log('Usage: ts-node src/migrations/lead-status-tour-history/preview-data.ts <lead_id1> <lead_id2> ...');
    console.log('Example: ts-node src/migrations/lead-status-tour-history/preview-data.ts 57601684 57601685');
    process.exit(1);
  }

  console.log(`\n=== Preview Migration Data for ${leadIds.length} Lead(s) ===\n`);

  try {
    await connectMySQL();
    await connectPostgres();

    const mysqlConn = getMySQLConnection();
    const pgClient = getPostgresClient();

    // Fetch lead data
    const placeholders = leadIds.map(() => '?').join(', ');
    const [leads] = await mysqlConn.query(
      `
      SELECT
        lrl.id,
        lrl.created_at,
        c.followup_rank,
        c.allow_followup AS allowFollowup
      FROM local_resource_leads lrl
      LEFT JOIN inquiries i ON i.id = lrl.inquiry_id
      LEFT JOIN contacts c ON c.id = i.contact_id
      WHERE lrl.id IN (${placeholders})
        AND lrl.deleted_at IS NULL
      `,
      leadIds
    );

    // Fetch statuses with account names
    const [statuses] = await mysqlConn.query(
      `
      SELECT
        ls.local_resource_lead_id AS lead_id,
        ls.created_at,
        ls.status,
        ls.sub_status,
        ls.tour_date,
        ls.tour_time,
        ls.source,
        COALESCE(a.full_name, a.email) AS account_name
      FROM lead_statuses ls
      LEFT JOIN accounts a ON a.id = ls.account_id
      WHERE ls.local_resource_lead_id IN (${placeholders})
      ORDER BY ls.local_resource_lead_id, ls.created_at DESC
      `,
      leadIds
    );

    // Check MM status
    const mmResult = await pgClient.query(
      `
      SELECT id, "legacyId", "legacyLeadStatusAndTourHistory", "leadPriority", "pipelineStage", "mldmMigratedModmonAt"
      FROM care_recipient_leads
      WHERE "legacyId" = ANY($1)
        AND "deletedAt" IS NULL
      `,
      [leadIds.map(id => id.toString())]
    );

    const mmMap = new Map();
    mmResult.rows.forEach(row => mmMap.set(parseInt(row.legacyId, 10), row));

    // Group statuses by lead
    const statusesByLead = new Map<number, DirLeadStatus[]>();
    statuses.forEach((status: any) => {
      if (!statusesByLead.has(status.lead_id)) {
        statusesByLead.set(status.lead_id, []);
      }
      statusesByLead.get(status.lead_id)!.push(status);
    });

    // Display preview for each lead
    for (const lead of leads as DirLead[]) {
      const leadStatuses = statusesByLead.get(lead.id) || [];
      const mmData = mmMap.get(lead.id);

      console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      console.log(`Lead ID: ${lead.id}`);
      console.log(`Created: ${lead.created_at}`);
      console.log(`\nMM Status:`);
      if (mmData) {
        console.log(`  ✓ Found in care_recipient_leads (id: ${mmData.id})`);
        console.log(`  Previously migrated: ${mmData.mldmMigratedModmonAt || 'No'}`);
        console.log(`  Current leadPriority: ${mmData.leadPriority || 'null'}`);
        console.log(`  Current pipelineStage: ${mmData.pipelineStage || 'null'}`);
        console.log(`  Current summary length: ${mmData.legacyLeadStatusAndTourHistory?.length || 0} chars`);
      } else {
        console.log(`  ✗ NOT found in care_recipient_leads (will be skipped)`);
      }

      console.log(`\nDIR Data:`);
      console.log(`  Status count: ${leadStatuses.length}`);
      console.log(`  Follow-up rank: ${lead.followup_rank}`);
      console.log(`  Allow follow-up: ${lead.allowFollowup}`);

      const newLeadPriority = deriveLeadPriority(lead.allowFollowup, lead.followup_rank);
      const newSummary = leadStatuses.length > 0 ? buildLeadStatusSummary(leadStatuses) : null;

      console.log(`\nNew Values (to be written):`);
      console.log(`  leadPriority: ${newLeadPriority}`);
      console.log(`  pipelineStage: Working`);
      console.log(`  Summary length: ${newSummary?.length || 0} chars`);

      if (newSummary) {
        console.log(`\n  Full Summary Text:`);
        console.log(`  ┌${'─'.repeat(78)}┐`);
        newSummary.split('\n').forEach(line => {
          console.log(`  │ ${line.padEnd(76)} │`);
        });
        console.log(`  └${'─'.repeat(78)}┘`);
      } else {
        console.log(`\n  (No statuses found - summary will be null)`);
      }

      console.log();
    }

    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);

    await disconnectMySQL();
    await disconnectPostgres();
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

previewData();
