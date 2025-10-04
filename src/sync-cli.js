const SyncEngine = require('./sync-engine');

const args = process.argv.slice(2);
const validTables = ['cars', 'locations', 'companies', 'users', 'loads'];
const tables = args.length > 0 ? args : validTables;

// Validate table names
const invalidTables = tables.filter(t => !validTables.includes(t));
if (invalidTables.length > 0) {
    console.error(`Invalid table names: ${invalidTables.join(', ')}`);
    console.error(`Valid tables: ${validTables.join(', ')}`);
    process.exit(1);
}

const syncEngine = new SyncEngine();

async function runSync() {
    try {
        console.log(`[sync] Starting sync for tables: ${tables.join(', ')}`);
        await syncEngine.runFullSync('manual', tables);
        console.log('[sync] Sync completed successfully');
    } catch (error) {
        console.error('[sync] Sync failed:', error);
        process.exit(1);
    }
}

runSync();
