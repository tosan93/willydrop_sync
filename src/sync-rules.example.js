module.exports = {
    // Set to false to revert to previous behaviour where blank values can overwrite non-blank values.
    preventBlankOverwrite: true,
    // Use the allowlists below to opt fields into allowing blank overwrites when preventBlankOverwrite is true.
    // Example: allow pickup_location_id to be cleared explicitly during Supabase -> Airtable syncs.
    allowBlankOverwrite: {
        airtableToSupabase: {
            cars: [],
            locations: []
        },
        supabaseToAirtable: {
            cars: [],
            locations: []
        }
    }
};
