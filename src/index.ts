import { connectMySQL, disconnectMySQL } from './db/mysql';
import { connectPostgres, disconnectPostgres } from './db/postgres';

console.log('MLDM Scripts - Data Migration Project');
console.log(`Node version: ${process.version}\n`);

async function main() {
  try {
    // Test MySQL connection
    console.log('=== Testing MySQL Connection ===');
    const mysqlConnection = await connectMySQL();
    const [mysqlRows] = await mysqlConnection.query('SELECT DATABASE() as db, VERSION() as version');
    console.log('MySQL info:', mysqlRows);
    await disconnectMySQL();
    console.log('');

    // Test PostgreSQL connection
    console.log('=== Testing PostgreSQL Connection ===');
    const pgClient = await connectPostgres();
    const pgResult = await pgClient.query('SELECT current_database() as db, version()');
    console.log('PostgreSQL info:', pgResult.rows[0]);
    await disconnectPostgres();

    console.log('\n✅ All connection tests successful');
  } catch (error) {
    console.error('❌ Error:', error);
    await disconnectMySQL();
    await disconnectPostgres();
    process.exit(1);
  }
}

main();

