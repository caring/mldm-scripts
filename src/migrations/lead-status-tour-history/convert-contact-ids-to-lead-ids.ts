#!/usr/bin/env ts-node
/**
 * Convert legacy contact IDs (care seeker IDs) to legacy lead IDs
 * Usage: ts-node src/migrations/lead-status-tour-history/convert-contact-ids-to-lead-ids.ts <contact_id1> <contact_id2> ...
 * Or: ts-node src/migrations/lead-status-tour-history/convert-contact-ids-to-lead-ids.ts --file contacts.csv
 */

import { promises as fs } from 'fs';
import { connectMySQL, disconnectMySQL, getMySQLConnection } from '../../db/mysql';

async function convertContactIdsToLeadIds() {
  const args = process.argv.slice(2);
  
  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    console.log(`
Usage:
  # Convert inline contact IDs
  ts-node src/migrations/lead-status-tour-history/convert-contact-ids-to-lead-ids.ts 123456 789012

  # Convert from CSV file (column: legacyContactId or contact_id)
  ts-node src/migrations/lead-status-tour-history/convert-contact-ids-to-lead-ids.ts --file contacts.csv

Output:
  - Prints CSV with columns: legacy_lead_id,legacy_contact_id,created_at
  - Can be piped to file: ... > lead_ids.csv
`);
    process.exit(0);
  }

  let contactIds: number[] = [];

  // Parse from file or inline
  if (args[0] === '--file') {
    const filePath = args[1];
    if (!filePath) {
      console.error('Error: --file requires a file path');
      process.exit(1);
    }

    const content = await fs.readFile(filePath, 'utf-8');
    const lines = content.split('\n').map(line => line.trim()).filter(line => line);
    
    // Check if first line is header
    const firstLine = lines[0].toLowerCase();
    const hasHeader = firstLine.includes('contact') || firstLine.includes('legacy');
    const dataLines = hasHeader ? lines.slice(1) : lines;

    contactIds = dataLines
      .map(line => {
        // Handle CSV (take first column)
        const parts = line.split(',');
        return parseInt(parts[0].trim(), 10);
      })
      .filter(id => !isNaN(id));

  } else {
    // Parse inline IDs
    contactIds = args
      .map(arg => parseInt(arg.trim(), 10))
      .filter(id => !isNaN(id));
  }

  if (contactIds.length === 0) {
    console.error('Error: No valid contact IDs found');
    process.exit(1);
  }

  console.error(`Converting ${contactIds.length} contact IDs to lead IDs...`);
  console.error('');

  try {
    await connectMySQL();
    const mysqlConn = getMySQLConnection();

    const placeholders = contactIds.map(() => '?').join(', ');
    const query = `
      SELECT 
        lrl.id AS legacy_lead_id,
        i.contact_id AS legacy_contact_id,
        lrl.created_at
      FROM local_resource_leads lrl
      INNER JOIN inquiries i ON i.id = lrl.inquiry_id
      WHERE i.contact_id IN (${placeholders})
        AND lrl.deleted_at IS NULL
      ORDER BY i.contact_id, lrl.created_at DESC
    `;

    const [rows] = await mysqlConn.query(query, contactIds);

    console.error(`Found ${rows.length} leads for ${contactIds.length} contacts`);
    console.error('');

    // Output CSV header
    console.log('legacy_lead_id,legacy_contact_id,created_at');

    // Output CSV rows
    for (const row of rows as any[]) {
      console.log(`${row.legacy_lead_id},${row.legacy_contact_id},${row.created_at.toISOString()}`);
    }

    // Summary to stderr (so it doesn't pollute CSV output)
    console.error('');
    console.error('Summary:');
    const byContact = new Map<number, number>();
    for (const row of rows as any[]) {
      const count = byContact.get(row.legacy_contact_id) || 0;
      byContact.set(row.legacy_contact_id, count + 1);
    }
    console.error(`  Contacts with leads: ${byContact.size}`);
    console.error(`  Total leads: ${rows.length}`);
    console.error(`  Avg leads per contact: ${(rows.length / byContact.size).toFixed(1)}`);

    const notFound = contactIds.filter(id => !byContact.has(id));
    if (notFound.length > 0) {
      console.error('');
      console.error(`Warning: ${notFound.length} contacts had no leads:`);
      notFound.forEach(id => console.error(`  - Contact ID: ${id}`));
    }

    await disconnectMySQL();

  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

convertContactIdsToLeadIds();
