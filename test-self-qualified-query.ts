import { connectMySQL, disconnectMySQL, getMySQLConnection } from './src/db/mysql';

async function testSelfQualifiedQuery() {
  try {
    console.log('Connecting to MySQL...');
    await connectMySQL();
    const mysqlConn = getMySQLConnection();

    // Test 1: Count all self-qualified notes (all time)
    console.log('\n=== Test 1: All Time ===');
    const [allTimeResult] = await mysqlConn.query(`
      SELECT COUNT(*) as total
      FROM contacts
      WHERE self_qualified_notes IS NOT NULL
        AND TRIM(self_qualified_notes) != ''
        AND care_recipient_id IS NOT NULL
    `);
    console.log(`Total self-qualified notes (all time): ${allTimeResult[0].total}`);

    // Test 2: Count last 4 years
    console.log('\n=== Test 2: Last 4 Years ===');
    const [fourYearsResult] = await mysqlConn.query(`
      SELECT COUNT(*) as total
      FROM contacts
      WHERE self_qualified_notes IS NOT NULL
        AND TRIM(self_qualified_notes) != ''
        AND care_recipient_id IS NOT NULL
        AND created_at >= DATE_SUB(NOW(), INTERVAL 4 YEAR)
    `);
    console.log(`Total self-qualified notes (last 4 years): ${fourYearsResult[0].total}`);

    // Test 3: Date range
    console.log('\n=== Test 3: Date Range ===');
    const [dateRangeResult] = await mysqlConn.query(`
      SELECT 
        MIN(created_at) as oldest,
        MAX(created_at) as newest,
        COUNT(*) as total
      FROM contacts
      WHERE self_qualified_notes IS NOT NULL
        AND TRIM(self_qualified_notes) != ''
        AND care_recipient_id IS NOT NULL
    `);
    console.log(`Oldest note: ${dateRangeResult[0].oldest}`);
    console.log(`Newest note: ${dateRangeResult[0].newest}`);
    console.log(`Total notes: ${dateRangeResult[0].total}`);

    // Test 4: Sample notes
    console.log('\n=== Test 4: Sample Notes (first 5) ===');
    const [sampleResult] = await mysqlConn.query(`
      SELECT 
        id,
        care_recipient_id,
        LEFT(self_qualified_notes, 50) as note_preview,
        LENGTH(self_qualified_notes) as note_length,
        created_at
      FROM contacts
      WHERE self_qualified_notes IS NOT NULL
        AND TRIM(self_qualified_notes) != ''
        AND care_recipient_id IS NOT NULL
      ORDER BY created_at DESC
      LIMIT 5
    `);
    console.table(sampleResult);

    // Test 5: Exact query from migration script
    console.log('\n=== Test 5: Migration Script Query ===');
    const fromDate = new Date();
    const toDate = new Date();
    toDate.setFullYear(toDate.getFullYear() - 4);
    
    const [migrationResult] = await mysqlConn.query(`
      SELECT
        c.id as contact_id,
        c.care_recipient_id,
        c.self_qualified_notes,
        c.account_id,
        c.created_at,
        c.updated_at
      FROM contacts c
      WHERE c.self_qualified_notes IS NOT NULL
        AND TRIM(c.self_qualified_notes) != ''
        AND c.care_recipient_id IS NOT NULL
        AND c.created_at <= ?
        AND c.created_at >= ?
      ORDER BY c.created_at DESC, c.id DESC
      LIMIT 10
    `, [fromDate, toDate]);
    
    console.log(`Migration query returned: ${migrationResult.length} rows`);
    if (migrationResult.length > 0) {
      console.log('First row:', migrationResult[0]);
    }

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await disconnectMySQL();
  }
}

testSelfQualifiedQuery();

