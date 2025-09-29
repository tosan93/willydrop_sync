require('dotenv').config();

module.exports = {
    supabase: {
        url: process.env.SUPABASE_URL,
        serviceKey: process.env.SUPABASE_SERVICE_KEY
    },
    airtable: {
        token: process.env.AIRTABLE_TOKEN,
        baseId: process.env.AIRTABLE_BASE_ID,
        tableName: 'Transports'
    },
    sync: {
        intervalMinutes: parseInt(process.env.SYNC_INTERVAL_MINUTES) || 2
    }
};