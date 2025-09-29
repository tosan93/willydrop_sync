const AirtableClient = require('./airtable-client');
const SupabaseClient = require('./supabase-client');

class SyncEngine {
    constructor() {
        this.airtable = new AirtableClient();
        this.supabase = new SupabaseClient();
        
        // Correct field mapping based on your Airtable
        this.fieldMapping = {
            // Airtable field name : Code field name
            'Name': 'name',
            'Pickup Address': 'pickup_address',
            'Pickup Latitude': 'pickup_lat',        // ← Was "Pickup Lat"
            'Pickup Longitude': 'pickup_lng',       // ← Was "Pickup Lng" 
            'Delivery Address': 'delivery_address',
            'Delivery Latitude': 'delivery_lat',    // ← Was "Delivery Lat"
            'Delivery Longitude': 'delivery_lng',   // ← Was "Delivery Lng"
            'Price (EUR)': 'price_eur',             // ← Was "Price EUR"
            'Status': 'status',
            'Urgency': 'urgency',
            'Country From': 'country_from',
            'Country To': 'country_to',
            'Postcode From': 'postcode_from',
            'Postcode To': 'postcode_to'
        };
    }
    
       async runFullSync() {
        console.log('🚀 Starting full bidirectional sync...');
        await this.syncAirtableToSupabase();
        await this.syncSupabaseToAirtable();
        console.log('🎉 Full sync complete!');
    }

    async syncAirtableToSupabase() {
        console.log('🔄 Starting Airtable → Supabase sync...');
        
        try {
            const airtableRecords = await this.airtable.getAllRecords();
            console.log(`Found ${airtableRecords.length} records in Airtable`);
            
            for (const record of airtableRecords) {
                await this.syncRecordToSupabase(record);
            }
            
            console.log('✅ Airtable → Supabase sync complete');
        } catch (error) {
            console.error('❌ Airtable → Supabase sync failed:', error);
        }
    }

    async syncSupabaseToAirtable() {
        console.log('🔄 Starting Supabase → Airtable sync...');
        
        try {
            const supabaseRecords = await this.supabase.getAllTransports();
            console.log(`Found ${supabaseRecords.length} records in Supabase`);
            
            for (const record of supabaseRecords) {
                if (!record.airtable_id) {
                    // Record doesn't exist in Airtable, create it
                    await this.createRecordInAirtable(record);
                }
            }
            
            console.log('✅ Supabase → Airtable sync complete');
        } catch (error) {
            console.error('❌ Supabase → Airtable sync failed:', error);
        }
    }


    mapAirtableFields(airtableRecord) {
        const mapped = { airtable_id: airtableRecord.airtable_id };
        
        Object.entries(this.fieldMapping).forEach(([airtableField, codeField]) => {
            mapped[codeField] = airtableRecord[airtableField];
        });
        
        console.log('Mapped record:', mapped);
        return mapped;
    }
    
    async syncRecordToSupabase(airtableRecord) {
        try {
            // Map field names
            const mappedRecord = this.mapAirtableFields(airtableRecord);
            
            // Validate required fields
            if (!mappedRecord.name) {
                console.error('❌ Missing required field: name');
                return;
            }
            
            const existingRecord = await this.supabase.findByAirtableId(mappedRecord.airtable_id);
            
            const transportData = {
                name: mappedRecord.name,
                pickup_address: mappedRecord.pickup_address,
                pickup_lat: mappedRecord.pickup_lat,
                pickup_lng: mappedRecord.pickup_lng,
                delivery_address: mappedRecord.delivery_address,
                delivery_lat: mappedRecord.delivery_lat,
                delivery_lng: mappedRecord.delivery_lng,
                price_eur: mappedRecord.price_eur,
                status: mappedRecord.status || 'available',
                urgency: mappedRecord.urgency || 'normal',
                country_from: mappedRecord.country_from,
                country_to: mappedRecord.country_to,
                postcode_from: mappedRecord.postcode_from,
                postcode_to: mappedRecord.postcode_to,
                airtable_id: mappedRecord.airtable_id
            };
            
            console.log('Transport data to sync:', transportData);
            
            if (existingRecord) {
                await this.supabase.updateTransport(existingRecord.id, transportData);
                console.log(`📝 Updated: ${mappedRecord.name}`);
            } else {
                await this.supabase.createTransport(transportData);
                console.log(`➕ Created: ${mappedRecord.name}`);
            }
            
        } catch (error) {
            console.error(`❌ Failed to sync ${mappedRecord.name}:`, error);
        }
    }

    async createRecordInAirtable(supabaseRecord) {
        try {
            // Map back to exact Airtable field names
            const airtableData = {
                'Name': supabaseRecord.name,
                'Pickup Address': supabaseRecord.pickup_address,
                'Pickup Latitude': supabaseRecord.pickup_lat,
                'Pickup Longitude': supabaseRecord.pickup_lng,
                'Delivery Address': supabaseRecord.delivery_address,
                'Delivery Latitude': supabaseRecord.delivery_lat,
                'Delivery Longitude': supabaseRecord.delivery_lng,
                'Price (EUR)': supabaseRecord.price_eur,
                'Status': supabaseRecord.status,
                'Urgency': supabaseRecord.urgency,
                'Country From': supabaseRecord.country_from,
                'Country To': supabaseRecord.country_to,
                'Postcode From': supabaseRecord.postcode_from,
                'Postcode To': supabaseRecord.postcode_to
            };
            
            console.log('Creating in Airtable:', airtableData);
            
            const airtableRecord = await this.airtable.createRecord(airtableData);
            
            // Update Supabase with Airtable ID
            await this.supabase.updateTransport(supabaseRecord.id, {
                airtable_id: airtableRecord.airtable_id
            });
            
            console.log(`➕ Created in Airtable: ${supabaseRecord.name}`);
        } catch (error) {
            console.error(`❌ Failed to create in Airtable ${supabaseRecord.name}:`, error);
        }
    }
}

module.exports = SyncEngine;