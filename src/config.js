require('dotenv').config();

module.exports = {
    supabase: {
        url: process.env.SUPABASE_URL,
        serviceKey: process.env.SUPABASE_SERVICE_KEY,
        tableName: process.env.SUPABASE_CARS_TABLE || 'cars'
    },
    airtable: {
        token: process.env.AIRTABLE_TOKEN,
        baseId: process.env.AIRTABLE_BASE_ID,
        tableName: process.env.AIRTABLE_TABLE_NAME || 'Cars'
    },
    sync: {
        intervalMinutes: parseInt(process.env.SYNC_INTERVAL_MINUTES, 10) || 2
    }
};
