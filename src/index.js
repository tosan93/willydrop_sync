const cron = require('node-cron');
const SyncEngine = require('./sync-engine');
const config = require('./config');

const syncEngine = new SyncEngine();

// Manual sync for testing
async function runManualSync() {
    console.log('Manual sync started...');
    await syncEngine.runFullSync();
}

// Scheduled sync
function startScheduledSync() {
    const cronPattern = `*/${config.sync.intervalMinutes} * * * *`; // Every N minutes
    
    console.log(`📅 Starting scheduled sync every ${config.sync.intervalMinutes} minutes...`);
    
    cron.schedule(cronPattern, async () => {
        console.log(`⏰ Scheduled sync triggered at ${new Date().toLocaleString()}`);
        await syncEngine.runFullSync();
    });
}

// Main execution
async function main() {
    console.log('🎬 Transport Sync PoC Starting...');
    
    // Run initial sync
    await runManualSync();
    
    // Start scheduled sync
    startScheduledSync();
    
    console.log('✨ Sync engine is running! Press Ctrl+C to stop.');
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n👋 Shutting down sync engine...');
    process.exit(0);
});

main().catch(console.error);