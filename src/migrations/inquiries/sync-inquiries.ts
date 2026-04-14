import { connectMySQL, disconnectMySQL, getMySQLConnection } from '../../db/mysql';
import { connectPostgres, disconnectPostgres, getPostgresClient } from '../../db/postgres';
import { parseTimeParam, formatDate, getRelativeDescription } from '../../utils/time-utils';
import {
  ensureMigrationDir,
  appendBatch,
  appendRow,
  readBatches,
  readRows,
  readSummary,
  writeSummary,
  RowRecord,
} from '../../utils/file-utils';
import {
  parseMigrationArgs,
  printMigrationHelp,
  isHelpRequested,
  printMigrationOptions,
  MigrationCLIOptions,
  parseIds,
} from '../../utils/migration-cli';
import {
  CareRecipientLeadRef,
  fetchCareRecipientLeadsByDateRange,
  resolveExplicitCareRecipientLeads,
} from '../../utils/care-recipient-lead-selection';

const MIGRATION_NAME = 'sync_inquiries';

interface LegacyLeadInquiryRow {
  legacy_lead_id: number;
  legacy_inquiry_id: number | null;
}

interface LegacyInquiryRow {
  legacy_inquiry_id: number;
  first_name: string | null;
  last_name: string | null;
  email_address: string | null;
  phone_number: string | null;
  inquiry_for: string | null;
  source: string | null;
  status: string | null;
  region_id: number | null;
  cobrand: string | null;
  reason: string | null;
  type: string | null;
  form_name: string | null;
  location: string | null;
  min_budget: number | null;
  max_budget: number | null;
  campaign_url: string | null;
  ip_address: string | null;
  living_situation: string | null;
  preferred_call_time: string | null;
  price: string | null;
  token: string | null;
  age: string | null;
  inquiry_for_name: string | null;
  zip_code: string | null;
  motive: string | null;
  browser: string | null;
  affiliate_campaign: string | null;
  alternate_call_time: string | null;
  utm_campaign: string | null;
  utm_content: string | null;
  utm_medium: string | null;
  utm_term: string | null;
  utm_account: string | null;
  msclkid: string | null;
  gclid: string | null;
  matchtype: string | null;
  ad: string | null;
  network: string | null;
  sitetarget: string | null;
  device: string | null;
  landing_page: string | null;
  created_at: Date;
  updated_at: Date;
}

interface MappingRow {
  careRecipientLeadId: string;
  legacyLeadId: number;
  legacyInquiryId: number;
  existingInquiryId: string | null;
}

async function migrateInquiries() {
  if (isHelpRequested()) {
    printMigrationHelp('migrate:inquiries');
    return;
  }

  const options = parseMigrationArgs();

  console.log('=== Inquiry Sync Migration ===\n');
  printMigrationOptions(options);

  try {
    await ensureMigrationDir(MIGRATION_NAME);

    if (options.report) {
      await generateReport();
      return;
    }

    if (options.retryFailed) {
      console.log('Retry failed functionality - to be implemented');
      return;
    }

    await runMigration(options);
  } catch (error) {
    console.error('Migration failed:', error);
    throw error;
  }
}

async function generateReport() {
  console.log('=== Migration Report ===\n');

  const summary = await readSummary();
  const migrationSummary = summary[MIGRATION_NAME];

  if (!migrationSummary) {
    console.log('No migration data found');
    return;
  }

  console.log('Status:', migrationSummary.status);
  console.log('Progress:');
  console.log(`  Batches: ${migrationSummary.completed_batches}`);
  console.log(`  Batch size: ${migrationSummary.batch_size}`);
  console.log();
  console.log('Results:');
  console.log(`  Total rows fetched: ${migrationSummary.total_rows_fetched}`);
  console.log(`  ✓ Success: ${migrationSummary.total_success}`);
  console.log(`  ⊘ Skipped: ${migrationSummary.total_skipped}`);
  console.log(`  ✗ Failed: ${migrationSummary.total_failed}`);
  console.log();
  console.log('Timestamps:');
  console.log(`  Started: ${migrationSummary.started_at}`);
  console.log(`  Last updated: ${migrationSummary.last_updated_at}`);
}

async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectMySQL();
  await connectPostgres();
  console.log('✓ Connected to both databases\n');

  const mysqlConn = getMySQLConnection();
  const pgClient = getPostgresClient();

  try {
    // Check if --ids parameter is provided (care seeker IDs)
    const careSeekerIds = await parseIds(options);

    if (careSeekerIds && careSeekerIds.length > 0) {
      // Care seeker ID-based migration
      await runCareSeekerIdBasedMigration(mysqlConn, pgClient, careSeekerIds, options);
      return;
    }

    // Existing flow: explicit lead IDs or date range
    const explicitResolution = await resolveExplicitCareRecipientLeads(options, pgClient);
    const fromDate = parseTimeParam(options.from);
    const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

    console.log('Migration scope:');
    if (explicitResolution) {
      console.log(`  Explicit legacy IDs provided: ${explicitResolution.requestedCount}`);
      console.log(`  Matching MM care_recipient_leads found: ${explicitResolution.matchedLeads.length}`);
    } else {
      console.log(`  MM leads created before: ${formatDate(fromDate)}`);
      if (toDate) {
        console.log(`  MM leads created after: ${formatDate(toDate)}`);
        console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
      }
    }
    console.log();

    const batches = await readBatches(MIGRATION_NAME);
    const processedIds: Set<string> = new Set();
    for (const batch of batches) {
      const batchRows = await readRows(`${MIGRATION_NAME}/${batch.batch_id}`);
      batchRows.forEach((r: RowRecord) => processedIds.add(r.source_id));
    }

    console.log('Existing progress:');
    console.log(`  Batches: ${batches.length}`);
    console.log(`  Leads processed: ${processedIds.size}`);
    console.log();

    let summary = await readSummary();
    if (!summary[MIGRATION_NAME]) {
      summary[MIGRATION_NAME] = {
        status: 'in_progress',
        time_range: {
          from: formatDate(fromDate),
          to: toDate ? formatDate(toDate) : null,
          to_relative: toDate ? getRelativeDescription(fromDate, toDate) : undefined,
        },
        batch_size: options.batchSize,
        total_batches: 0,
        completed_batches: 0,
        total_rows_fetched: 0,
        total_success: 0,
        total_skipped: 0,
        total_duplicate: 0,
        total_failed: 0,
        started_at: formatDate(new Date()),
        last_updated_at: null,
      };
      await writeSummary(summary);
    }

    let batchNumber = batches.length + 1;

    if (explicitResolution) {
      const unprocessed = explicitResolution.matchedLeads.filter(
        (lead) => !processedIds.has(lead.careRecipientLeadId)
      );
      for (let i = 0; i < unprocessed.length; i += options.batchSize) {
        const batchLeads = unprocessed.slice(i, i + options.batchSize);
        const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
        console.log(`\n=== Processing ${batchId} ===`);
        if (!options.dryRun) {
          await ensureMigrationDir(`${MIGRATION_NAME}/${batchId}`);
        }
        await processBatch({
          options,
          batchId,
          batchNumber,
          batchLeads,
          fromDate,
          toDate,
          mysqlConn,
          pgClient,
          processedIds,
        });
        batchNumber++;
      }
    } else {
      let offset = batches.reduce((sum, batch) => sum + batch.fetched_count, 0);
      let hasMore = true;

      while (hasMore) {
        const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
        console.log(`\n=== Processing ${batchId} ===`);
        if (!options.dryRun) {
          await ensureMigrationDir(`${MIGRATION_NAME}/${batchId}`);
        }

        const batchLeads = await fetchCareRecipientLeadsByDateRange(
          pgClient,
          fromDate,
          toDate,
          options.batchSize,
          offset
        );

        if (batchLeads.length === 0) {
          console.log('No more leads to process');
          hasMore = false;
          break;
        }

        await processBatch({
          options,
          batchId,
          batchNumber,
          batchLeads,
          fromDate,
          toDate,
          mysqlConn,
          pgClient,
          processedIds,
        });

        offset += options.batchSize;
        batchNumber++;
      }
    }

    summary = await readSummary();
    summary[MIGRATION_NAME].status = 'completed';
    summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
    await writeSummary(summary);

    console.log('\n=== Migration Complete ===');
    console.log(`Total rows fetched: ${summary[MIGRATION_NAME].total_rows_fetched}`);
    console.log(`✓ Success: ${summary[MIGRATION_NAME].total_success}`);
    console.log(`⊘ Skipped: ${summary[MIGRATION_NAME].total_skipped}`);
    console.log(`✗ Failed: ${summary[MIGRATION_NAME].total_failed}`);
  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
  }
}

async function processBatch(args: {
  options: MigrationCLIOptions;
  batchId: string;
  batchNumber: number;
  batchLeads: CareRecipientLeadRef[];
  fromDate: Date;
  toDate: Date | null;
  mysqlConn: any;
  pgClient: any;
  processedIds: Set<string>;
}): Promise<void> {
  const {
    options,
    batchId,
    batchNumber,
    batchLeads,
    fromDate,
    toDate,
    mysqlConn,
    pgClient,
    processedIds,
  } = args;

  const unprocessed = batchLeads.filter((lead) => !processedIds.has(lead.careRecipientLeadId));
  console.log(
    `Processing ${unprocessed.length} leads (${batchLeads.length - unprocessed.length} already processed)`
  );

  if (unprocessed.length === 0) {
    return;
  }

  const legacyLeadIds = unprocessed.map((lead) => lead.legacyId);
  const leadInquiryMap = await fetchLegacyLeadInquiryMap(mysqlConn, legacyLeadIds);

  const mappings: MappingRow[] = [];
  for (const lead of unprocessed) {
    const legacyInquiryId = leadInquiryMap.get(lead.legacyId);
    if (legacyInquiryId) {
      mappings.push({
        careRecipientLeadId: lead.careRecipientLeadId,
        legacyLeadId: lead.legacyId,
        legacyInquiryId,
      });
    }
  }

  if (mappings.length === 0) {
    console.log('No legacy inquiry mappings found for this batch');
  } else {
    console.log('\nMapping (legacyInquiryId -> legacyLeadId -> careRecipientLeadId):');
    mappings.slice(0, 20).forEach((m) => {
      console.log(`  ${m.legacyInquiryId} -> ${m.legacyLeadId} -> ${m.careRecipientLeadId}`);
    });
    if (mappings.length > 20) {
      console.log(`  ... and ${mappings.length - 20} more`);
    }
  }

  let batchSuccess = 0;
  let batchSkipped = 0;
  let batchFailed = 0;

  if (!options.dryRun && mappings.length > 0) {
    const legacyInquiryIds = [...new Set(mappings.map((m) => m.legacyInquiryId))];
    const legacyInquiries = await fetchLegacyInquiries(mysqlConn, legacyInquiryIds);
    const existingInquiryIdByCRLId = await fetchExistingInquiryIdsForCRL(
      pgClient,
      mappings.map((m) => m.careRecipientLeadId)
    );
    const legacyInquiryById = new Map<number, LegacyInquiryRow>();
    legacyInquiries.forEach((row) => legacyInquiryById.set(row.legacy_inquiry_id, row));

    for (const m of mappings) {
      m.existingInquiryId = existingInquiryIdByCRLId.get(m.careRecipientLeadId) || null;
    }

    // Step 4 + 5: upsert inquiry content and keep CRL.inquiryId mapping
    await syncMMInquiriesForMappings(pgClient, mappings, legacyInquiryById);
  }

  for (const lead of unprocessed) {
    const mapping = mappings.find((m) => m.careRecipientLeadId === lead.careRecipientLeadId);
    if (!mapping) {
      batchSkipped++;
      if (!options.dryRun) {
        await appendRow(`${MIGRATION_NAME}/${batchId}`, {
          batch_id: batchId,
          source_id: lead.careRecipientLeadId,
          status: 'skipped' as any,
          reason: 'no_legacy_inquiry_mapping',
          processed_at: formatDate(new Date()),
        });
      }
    } else {
      batchSuccess++;
      if (!options.dryRun) {
        await appendRow(`${MIGRATION_NAME}/${batchId}`, {
          batch_id: batchId,
          source_id: lead.careRecipientLeadId,
          status: 'success',
          legacy_id: lead.legacyId.toString(),
          processed_at: formatDate(new Date()),
        });
      }
    }
    processedIds.add(lead.careRecipientLeadId);
  }

  if (!options.dryRun) {
    await appendBatch(MIGRATION_NAME, {
      batch_id: batchId,
      status: 'completed',
      query: {
        from: formatDate(fromDate),
        to: toDate ? formatDate(toDate) : null,
        last_created_at: null,
        last_id: unprocessed[unprocessed.length - 1]?.careRecipientLeadId || null,
        limit: options.batchSize,
      },
      fetched_count: batchLeads.length,
      started_at: formatDate(new Date()),
      completed_at: formatDate(new Date()),
    });
  }

  const summary = await readSummary();
  summary[MIGRATION_NAME].completed_batches = batchNumber;
  summary[MIGRATION_NAME].total_rows_fetched += batchLeads.length;
  summary[MIGRATION_NAME].total_success += batchSuccess;
  summary[MIGRATION_NAME].total_skipped += batchSkipped;
  summary[MIGRATION_NAME].total_failed += batchFailed;
  summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
  await writeSummary(summary);

  console.log(`\nBatch ${batchId} Summary:`);
  console.log(`  ✓ Success: ${batchSuccess}`);
  console.log(`  ⊘ Skipped: ${batchSkipped}`);
  console.log(`  ✗ Failed: ${batchFailed}`);
}

/**
 * Run migration for specific care seeker IDs (legacy contact IDs)
 */
async function runCareSeekerIdBasedMigration(
  mysqlConn: any,
  pgClient: any,
  careSeekerIds: number[],
  options: MigrationCLIOptions
) {
  console.log(`Migration scope: ${careSeekerIds.length} care seeker IDs (legacy contact IDs)`);
  console.log();

  // Step 1: Get MM leads that need inquiries (inquiryId IS NULL)
  console.log('Fetching MM leads that need inquiries...');
  const mmLeadsNeedingInquiries = await fetchMMLeadsNeedingInquiries(pgClient, careSeekerIds);
  console.log(`✓ Found ${mmLeadsNeedingInquiries.length} MM leads needing inquiries`);
  console.log();

  if (mmLeadsNeedingInquiries.length === 0) {
    console.log('No leads need inquiry sync. All done!');
    return;
  }

  // Step 2: Get inquiry mapping from DIR
  console.log('Fetching inquiry mapping from DIR...');
  const legacyLeadIds = mmLeadsNeedingInquiries.map(l => l.legacyId);
  const inquiryMap = await fetchLegacyLeadInquiryMap(mysqlConn, legacyLeadIds);
  console.log(`✓ Found ${inquiryMap.size} leads with inquiries in DIR`);
  console.log();

  // Build mappings
  const mappings: MappingRow[] = [];
  for (const mmLead of mmLeadsNeedingInquiries) {
    const inquiryId = inquiryMap.get(mmLead.legacyId);
    if (inquiryId) {
      mappings.push({
        careRecipientLeadId: mmLead.id,
        legacyLeadId: mmLead.legacyId,
        legacyInquiryId: inquiryId,
        existingInquiryId: null,
      });
    }
  }

  console.log(`Mapped ${mappings.length} leads to inquiries`);
  console.log();

  if (mappings.length === 0) {
    console.log('No inquiry mappings found in DIR');
    return;
  }

  // Step 3: Fetch full inquiry data from DIR
  const uniqueInquiryIds = [...new Set(mappings.map(m => m.legacyInquiryId))];
  console.log(`Fetching ${uniqueInquiryIds.length} unique inquiries from DIR...`);
  const legacyInquiries = await fetchLegacyInquiries(mysqlConn, uniqueInquiryIds);
  const legacyInquiryById = new Map<number, LegacyInquiryRow>();
  legacyInquiries.forEach((row) => legacyInquiryById.set(row.legacy_inquiry_id, row));
  console.log(`✓ Fetched ${legacyInquiries.length} inquiries`);
  console.log();

  // Step 4 & 5: Sync inquiries to MM and update lead mappings
  if (!options.dryRun) {
    console.log('Syncing inquiries to MM...');
    await syncMMInquiriesForMappings(pgClient, mappings, legacyInquiryById);
    console.log(`✓ Synced ${uniqueInquiryIds.length} inquiries and updated ${mappings.length} leads`);
  } else {
    console.log(`[DRY RUN] Would sync ${uniqueInquiryIds.length} inquiries and update ${mappings.length} leads`);
  }

  console.log();
  console.log('=== Migration Complete ===');
  console.log(`Care seekers processed: ${careSeekerIds.length}`);
  console.log(`Leads updated: ${mappings.length}`);
  console.log(`Inquiries synced: ${uniqueInquiryIds.length}`);
}

/**
 * Fetch MM leads that need inquiries for given care seeker IDs
 */
async function fetchMMLeadsNeedingInquiries(
  pgClient: any,
  careSeekerIds: number[]
): Promise<Array<{ id: string; legacyId: number }>> {
  if (careSeekerIds.length === 0) return [];

  const result = await pgClient.query(
    `
    SELECT
      crl.id,
      crl."legacyId"
    FROM care_recipient_leads crl
    INNER JOIN care_recipients cr ON cr.id = crl."careRecipientId"
    INNER JOIN care_seekers cs ON cs.id = cr."careSeekerId"
    WHERE cs."legacyId" = ANY($1)
      AND crl."legacyId" IS NOT NULL
      AND crl."inquiryId" IS NULL
      AND crl."deletedAt" IS NULL
      AND cr."deletedAt" IS NULL
      AND cs."deletedAt" IS NULL
    ORDER BY crl."createdAt" DESC
    `,
    [careSeekerIds.map(id => id.toString())]
  );

  return result.rows.map((row: any) => ({
    id: row.id,
    legacyId: parseInt(row.legacyId, 10),
  }));
}

async function fetchLegacyLeadInquiryMap(
  mysqlConn: any,
  legacyLeadIds: number[]
): Promise<Map<number, number>> {
  if (legacyLeadIds.length === 0) return new Map();

  const placeholders = legacyLeadIds.map(() => '?').join(', ');
  const [rows] = await mysqlConn.query(
    `
    SELECT
      lrl.id AS legacy_lead_id,
      lrl.inquiry_id AS legacy_inquiry_id
    FROM local_resource_leads lrl
    WHERE lrl.id IN (${placeholders})
      AND lrl.deleted_at IS NULL
      AND lrl.inquiry_id IS NOT NULL
    `,
    legacyLeadIds
  );

  const map = new Map<number, number>();
  (rows as LegacyLeadInquiryRow[]).forEach((row) => {
    if (row.legacy_inquiry_id) {
      map.set(row.legacy_lead_id, row.legacy_inquiry_id);
    }
  });
  return map;
}

async function fetchLegacyInquiries(
  mysqlConn: any,
  legacyInquiryIds: number[]
): Promise<LegacyInquiryRow[]> {
  if (legacyInquiryIds.length === 0) return [];

  const placeholders = legacyInquiryIds.map(() => '?').join(', ');
  const [rows] = await mysqlConn.query(
    `
    SELECT
      i.id AS legacy_inquiry_id,
      i.first_name,
      i.last_name,
      i.email_address,
      i.phone_number,
      i.inquiry_for,
      i.source,
      i.status,
      i.region_id,
      i.cobrand,
      i.reason,
      i.type,
      i.form_name,
      i.location,
      i.min_budget,
      i.max_budget,
      i.campaign_url,
      i.ip_address,
      i.living_situation,
      i.preferred_call_time,
      i.price,
      i.token,
      i.age,
      i.inquiry_for_name,
      i.zip_code,
      i.motive,
      i.browser,
      i.affiliate_campaign,
      i.alternate_call_time,
      i.utm_campaign,
      i.utm_content,
      i.utm_medium,
      i.utm_term,
      i.utm_account,
      i.msclkid,
      i.gclid,
      i.matchtype,
      i.ad,
      i.network,
      i.sitetarget,
      i.device,
      i.landing_page,
      i.created_at,
      i.updated_at
    FROM inquiries i
    WHERE i.id IN (${placeholders})
    `,
    legacyInquiryIds
  );

  return rows as LegacyInquiryRow[];
}

async function fetchExistingInquiryIdsForCRL(
  pgClient: any,
  careRecipientLeadIds: string[]
): Promise<Map<string, string>> {
  if (careRecipientLeadIds.length === 0) return new Map();

  const result = await pgClient.query(
    `
    SELECT id, "inquiryId"
    FROM care_recipient_leads
    WHERE id = ANY($1::uuid[])
      AND "deletedAt" IS NULL
    `,
    [careRecipientLeadIds]
  );

  const map = new Map<string, string>();
  result.rows.forEach((row: any) => {
    if (row.inquiryId) {
      map.set(row.id, row.inquiryId);
    }
  });
  return map;
}

async function syncMMInquiriesForMappings(
  pgClient: any,
  mappings: MappingRow[],
  legacyInquiryById: Map<number, LegacyInquiryRow>
): Promise<void> {
  const createdInquiryIdByLegacyInquiryId = new Map<number, string>();

  for (const mapping of mappings) {
    const legacyInquiry = legacyInquiryById.get(mapping.legacyInquiryId);
    if (!legacyInquiry) continue;

    let inquiryId = mapping.existingInquiryId || createdInquiryIdByLegacyInquiryId.get(mapping.legacyInquiryId) || null;
    if (inquiryId) {
      await updateMMInquiry(pgClient, inquiryId, legacyInquiry);
    } else {
      inquiryId = await insertMMInquiry(pgClient, legacyInquiry);
      createdInquiryIdByLegacyInquiryId.set(mapping.legacyInquiryId, inquiryId);
    }

    await pgClient.query(
      `
      UPDATE care_recipient_leads
      SET
        "inquiryId" = $1::uuid,
        "updatedAt" = NOW()
      WHERE id = $2::uuid
        AND "deletedAt" IS NULL
      `,
      [inquiryId, mapping.careRecipientLeadId]
    );
  }
}

function toMMInquiryParams(legacy: LegacyInquiryRow): any[] {
  return [
    legacy.first_name,
    legacy.last_name,
    legacy.email_address,
    legacy.phone_number,
    legacy.inquiry_for,
    legacy.source,
    legacy.status || 'new',
    legacy.region_id,
    legacy.cobrand,
    legacy.reason,
    legacy.type,
    legacy.form_name,
    legacy.location,
    legacy.min_budget,
    legacy.max_budget,
    legacy.campaign_url,
    legacy.ip_address,
    legacy.living_situation,
    legacy.preferred_call_time,
    legacy.price,
    legacy.token,
    legacy.age,
    legacy.inquiry_for_name,
    legacy.zip_code,
    legacy.motive,
    legacy.browser,
    legacy.affiliate_campaign,
    legacy.alternate_call_time,
    legacy.utm_campaign,
    legacy.utm_content,
    legacy.utm_medium,
    legacy.utm_term,
    legacy.utm_account,
    legacy.msclkid,
    legacy.gclid,
    legacy.matchtype,
    legacy.ad,
    legacy.network,
    legacy.sitetarget,
    legacy.device,
    legacy.landing_page,
    legacy.created_at,
    legacy.updated_at,
  ];
}

async function updateMMInquiry(pgClient: any, inquiryId: string, legacy: LegacyInquiryRow): Promise<void> {
  const params = [...toMMInquiryParams(legacy), inquiryId];
  await pgClient.query(
    `
    UPDATE inquiries
    SET
      "firstName" = $1,
      "lastName" = $2,
      "email" = $3,
      "phoneNumber" = $4,
      "inquiryFor" = $5,
      "source" = $6,
      "status" = $7,
      "regionId" = $8,
      "cobrand" = $9,
      "reason" = $10,
      "type" = $11,
      "formName" = $12,
      "location" = $13,
      "minBudget" = $14,
      "maxBudget" = $15,
      "campaignUrl" = $16,
      "ipAddress" = $17,
      "livingSituation" = $18,
      "preferredCallTime" = $19,
      "price" = $20,
      "token" = $21,
      "age" = $22,
      "inquiryForName" = $23,
      "zipCode" = $24,
      "motive" = $25,
      "browser" = $26,
      "affiliateCampaign" = $27,
      "alternateCallTime" = $28,
      "utmCampaign" = $29,
      "utmContent" = $30,
      "utmMedium" = $31,
      "utmTerm" = $32,
      "utmAccount" = $33,
      "msclkid" = $34,
      "gclid" = $35,
      "matchType" = $36,
      "ad" = $37,
      "network" = $38,
      "siteTarget" = $39,
      "device" = $40,
      "landingPageUrl" = $41,
      "mldmMigratedModmonAt" = NOW(),
      "createdAt" = COALESCE("createdAt", $42),
      "updatedAt" = $43
    WHERE id = $44::uuid
    `,
    params
  );
}

async function insertMMInquiry(pgClient: any, legacy: LegacyInquiryRow): Promise<string> {
  const params = toMMInquiryParams(legacy);
  const result = await pgClient.query(
    `
    INSERT INTO inquiries (
      "firstName",
      "lastName",
      "email",
      "phoneNumber",
      "inquiryFor",
      "source",
      "status",
      "regionId",
      "cobrand",
      "reason",
      "type",
      "formName",
      "location",
      "minBudget",
      "maxBudget",
      "campaignUrl",
      "ipAddress",
      "livingSituation",
      "preferredCallTime",
      "price",
      "token",
      "age",
      "inquiryForName",
      "zipCode",
      "motive",
      "browser",
      "affiliateCampaign",
      "alternateCallTime",
      "utmCampaign",
      "utmContent",
      "utmMedium",
      "utmTerm",
      "utmAccount",
      "msclkid",
      "gclid",
      "matchType",
      "ad",
      "network",
      "siteTarget",
      "device",
      "landingPageUrl",
      "mldmMigratedModmonAt",
      "createdAt",
      "updatedAt"
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
      $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
      $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
      $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
      $41, NOW(), $42, $43
    )
    RETURNING id
    `,
    params
  );
  return result.rows[0].id;
}

migrateInquiries().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
