import { connectMySQL, disconnectMySQL, getMySQLConnection } from './db/mysql';
import { connectPostgres, disconnectPostgres, getPostgresClient } from './db/postgres';

interface AffiliateNote {
  formatted_text_id: number;
  note_content: string;
  inquiry_id: number;
  contact_id: number;
  dir_care_recipient_id: number;
  dir_account_id: number | null;
  created_at: Date;
  updated_at: Date;
}

async function testAffiliateNotesQuery() {
  console.log('=== Testing Affiliate Notes Query ===\n');

  try {
    // Connect to both databases
    console.log('Connecting to databases...');
    await connectMySQL();
    await connectPostgres();
    console.log('✓ Connected to both databases\n');

    const mysqlConn = getMySQLConnection();
    const pgClient = getPostgresClient();

    // Query 100 affiliate notes from DIR
    console.log('Fetching 100 affiliate notes from DIR...');
    const dirQuery = `
      SELECT 
        ft.id as formatted_text_id,
        ft.original_content as note_content,
        i.id as inquiry_id,
        i.contact_id,
        c.care_recipient_id as dir_care_recipient_id,
        i.account_id as dir_account_id,
        i.created_at,
        i.updated_at
      FROM formatted_texts ft
      INNER JOIN inquiries i ON ft.owner_id = i.id AND ft.owner_type = 'Inquiry'
      INNER JOIN contacts c ON i.contact_id = c.id
      WHERE ft.name = 'affiliate_notes'
        AND ft.original_content IS NOT NULL
        AND TRIM(ft.original_content) != ''
        AND c.care_recipient_id IS NOT NULL
      ORDER BY i.created_at DESC
      LIMIT 100
    `;

    const [dirRows] = await mysqlConn.query(dirQuery);
    const affiliateNotes = dirRows as AffiliateNote[];
    
    console.log(`✓ Fetched ${affiliateNotes.length} affiliate notes from DIR\n`);

    if (affiliateNotes.length === 0) {
      console.log('No affiliate notes found in DIR');
      return;
    }

    // Show detailed sample data
    console.log('=== SAMPLE AFFILIATE NOTES (First 5) ===\n');
    affiliateNotes.slice(0, 5).forEach((note, idx) => {
      console.log(`📝 Note ${idx + 1}:`);
      console.log(`  formatted_text_id: ${note.formatted_text_id}`);
      console.log(`  note_content: "${note.note_content}"`);
      console.log(`  note_length: ${note.note_content.length} chars`);
      console.log(`  inquiry_id: ${note.inquiry_id}`);
      console.log(`  contact_id: ${note.contact_id}`);
      console.log(`  dir_care_recipient_id: ${note.dir_care_recipient_id}`);
      console.log(`  dir_account_id: ${note.dir_account_id || 'null'}`);
      console.log(`  created_at: ${note.created_at.toISOString()}`);
      console.log(`  updated_at: ${note.updated_at.toISOString()}`);
      console.log();
    });

    // Extract unique care_recipient_ids
    const dirCareRecipientIds = [...new Set(affiliateNotes.map(n => n.dir_care_recipient_id.toString()))];
    console.log(`Found ${dirCareRecipientIds.length} unique care_recipient_ids\n`);

    // Check which care_recipients exist in MM
    console.log('=== CARE_RECIPIENT MAPPING (DIR → MM) ===\n');
    const mmQuery = `
      SELECT id, "legacyId"
      FROM care_recipients
      WHERE "legacyId" = ANY($1)
    `;

    const mmResult = await pgClient.query(mmQuery, [dirCareRecipientIds]);
    const mappings = new Map<string, string>();
    mmResult.rows.forEach((row: any) => {
      mappings.set(row.legacyId, row.id);
    });

    console.log(`Found ${mappings.size} care_recipients in MM (out of ${dirCareRecipientIds.length})`);
    console.log('\nFirst 5 mappings:');
    let count = 0;
    for (const [dirId, mmId] of mappings.entries()) {
      if (count < 5) {
        console.log(`  DIR ${dirId} → MM ${mmId}`);
        count++;
      }
    }
    console.log();

    // Check if any notes already exist in MM
    console.log('Checking if any notes already exist in MM...');
    const legacyIds = affiliateNotes.map(n => n.formatted_text_id.toString());
    const existingNotesQuery = `
      SELECT "legacyId"
      FROM care_recipient_notes
      WHERE "legacyId" = ANY($1)
    `;

    const existingResult = await pgClient.query(existingNotesQuery, [legacyIds]);
    const existingLegacyIds = new Set(existingResult.rows.map((row: any) => row.legacyId));

    console.log(`✓ Found ${existingLegacyIds.size} notes already in MM (out of ${affiliateNotes.length})\n`);

    // Show breakdown by status with examples
    console.log('=== TRANSFORMATION EXAMPLES ===\n');

    let canMigrate = 0;
    let alreadyExists = 0;
    let noCareRecipient = 0;
    let exampleShown = false;

    affiliateNotes.forEach((note) => {
      const legacyId = note.formatted_text_id.toString();
      const dirCrId = note.dir_care_recipient_id.toString();
      const mmCareRecipientId = mappings.get(dirCrId);

      if (existingLegacyIds.has(legacyId)) {
        alreadyExists++;
      } else if (!mappings.has(dirCrId)) {
        noCareRecipient++;
      } else {
        canMigrate++;

        // Show first example of successful transformation
        if (!exampleShown) {
          console.log('Example of successful transformation:');
          console.log('\n📥 INPUT (from DIR):');
          console.log(`  formatted_text_id: ${note.formatted_text_id}`);
          console.log(`  note_content: "${note.note_content}"`);
          console.log(`  inquiry_id: ${note.inquiry_id}`);
          console.log(`  contact_id: ${note.contact_id}`);
          console.log(`  dir_care_recipient_id: ${note.dir_care_recipient_id}`);
          console.log(`  dir_account_id: ${note.dir_account_id || 'null'}`);
          console.log(`  created_at: ${note.created_at.toISOString()}`);
          console.log(`  updated_at: ${note.updated_at.toISOString()}`);

          console.log('\n🔄 TRANSFORMATION:');
          console.log(`  legacyId: "${note.formatted_text_id}"`);
          console.log(`  DIR care_recipient_id ${dirCrId} → MM careRecipientId ${mmCareRecipientId}`);
          console.log(`  DIR account_id ${note.dir_account_id || 'null'} → MM agentAccountId (would be mapped)`);
          console.log(`  value: "${note.note_content.trim()}" (trimmed)`);

          console.log('\n📤 OUTPUT (to MM care_recipient_notes):');
          console.log(`  {`);
          console.log(`    id: <generated UUID>,`);
          console.log(`    legacyId: "${note.formatted_text_id}",`);
          console.log(`    careRecipientId: "${mmCareRecipientId}",`);
          console.log(`    value: "${note.note_content.trim()}",`);
          console.log(`    agentAccountId: ${note.dir_account_id ? '<mapped UUID>' : 'null'},`);
          console.log(`    agentName: "",`);
          console.log(`    source: "affiliate_notes",`);
          console.log(`    createdAt: "${note.created_at.toISOString()}",`);
          console.log(`    updatedAt: "${note.updated_at.toISOString()}"`);
          console.log(`  }`);
          console.log();
          exampleShown = true;
        }
      }
    });

    console.log('=== SUMMARY ===');
    console.log(`Total notes fetched from DIR: ${affiliateNotes.length}`);
    console.log(`Unique care_recipients: ${dirCareRecipientIds.length}`);
    console.log(`Care_recipients found in MM: ${mappings.size} (${((mappings.size / dirCareRecipientIds.length) * 100).toFixed(1)}%)`);
    console.log(`Care_recipients NOT in MM: ${dirCareRecipientIds.length - mappings.size}`);
    console.log(`Notes already migrated: ${existingLegacyIds.size} (${((existingLegacyIds.size / affiliateNotes.length) * 100).toFixed(1)}%)`);
    console.log(`Notes ready to migrate: ${affiliateNotes.length - existingLegacyIds.size}`);
    console.log();

    console.log('=== MIGRATION READINESS ===');
    console.log(`✓ Can migrate: ${canMigrate} (${((canMigrate / affiliateNotes.length) * 100).toFixed(1)}%)`);
    console.log(`⊘ Already exists: ${alreadyExists} (${((alreadyExists / affiliateNotes.length) * 100).toFixed(1)}%)`);
    console.log(`✗ No care_recipient in MM: ${noCareRecipient} (${((noCareRecipient / affiliateNotes.length) * 100).toFixed(1)}%)`);

  } catch (error) {
    console.error('Error:', error);
    throw error;
  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
    console.log('\n✓ Disconnected from databases');
  }
}

// Run the test
testAffiliateNotesQuery()
  .then(() => {
    console.log('\n✓ Test completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n✗ Test failed:', error);
    process.exit(1);
  });

