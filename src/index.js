const cron = require('node-cron');
const SyncEngine = require('./sync-engine');
const config = require('./config');

const syncEngine = new SyncEngine();

async function runManualSync() {
    console.log('[sync] Manual sync started...');
    await syncEngine.runFullSync();
    console.log('[sync] Manual sync finished.');
}

function startScheduledSync() {
    const cronPattern = `*/${config.sync.intervalMinutes} * * * *`;
    console.log(`[sync] Scheduling sync every ${config.sync.intervalMinutes} minute(s) (cron: ${cronPattern}).`);

    cron.schedule(cronPattern, async () => {
        console.log(`[sync] Scheduled sync triggered at ${new Date().toISOString()}`);
        try {
            await syncEngine.runFullSync();
        } catch (error) {
            console.error('[sync] Scheduled sync failed:', error.message);
        }
    });
}

async function main() {
    console.log('[sync] Transport Sync PoC starting...');
    await runManualSync();
    startScheduledSync();
    console.log('[sync] Sync engine is running. Press Ctrl+C to stop.');
}

process.on('SIGINT', () => {
    console.log('\n[sync] Shutting down sync engine...');
    process.exit(0);
});

main().catch(error => {
    console.error('[sync] Fatal error during startup:', error.message);
    process.exit(1);
});
