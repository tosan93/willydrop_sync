const AirtableClient = require('./airtable-client');
const SupabaseClient = require('./supabase-client');

const CAR_FIELDS = [
    'external_id',
    'make',
    'model',
    'vin',
    'license_plate',
    'status',
    'customer_id',
    'pickup_location_id',
    'delivery_location_id',
    'earliest_availability_date',
    'pick_up_date',
    'special_instructions',
    'carrier_rate',
    'customer_rate',
    'priority',
    'delivery_date_actual',
    'delivery_date_customer_view',
    'delivery_date_quoted',
    'distance'
];

const NUMERIC_FIELDS = ['carrier_rate', 'customer_rate', 'distance'];
const REQUIRED_FIELDS = ['make', 'model'];

class SyncEngine {
    constructor() {
        this.airtable = new AirtableClient();
        this.supabase = new SupabaseClient();
    }

    async runFullSync() {
        console.log('[sync] Starting full bidirectional sync...');
        await this.syncAirtableToSupabase();
        await this.syncSupabaseToAirtable();
        console.log('[sync] Full sync complete.');
    }

    async syncAirtableToSupabase() {
        console.log('[sync] Processing Airtable -> Supabase changes...');

        let processed = 0;
        const airtableRecords = await this.airtable.getAllRecords();

        for (const record of airtableRecords) {
            try {
                await this.upsertSupabaseFromAirtable(record);
                processed += 1;
            } catch (error) {
                console.error(`[sync] Failed to sync Airtable record ${record.airtable_id}:`, error.message);
            }
        }

        console.log(`[sync] Airtable -> Supabase processed ${processed} records.`);
    }

    async syncSupabaseToAirtable() {
        console.log('[sync] Processing Supabase -> Airtable changes...');

        const [supabaseCars, airtableRecords] = await Promise.all([
            this.supabase.getAllCars(),
            this.airtable.getAllRecords()
        ]);

        const airtableBySupabaseId = new Map();
        airtableRecords.forEach(record => {
            if (record.supabase_id) {
                airtableBySupabaseId.set(record.supabase_id, record);
            }
        });

        let processed = 0;

        for (const car of supabaseCars) {
            try {
                const airtablePayload = this.mapSupabaseToAirtable(car);
                const existingRecord = airtableBySupabaseId.get(car.id);

                if (existingRecord) {
                    await this.airtable.updateRecord(existingRecord.airtable_id, airtablePayload);
                } else {
                    const createdRecord = await this.airtable.createRecord(airtablePayload);
                    airtableBySupabaseId.set(car.id, createdRecord);
                }

                processed += 1;
            } catch (error) {
                console.error(`[sync] Failed to sync Supabase car ${car.id}:`, error.message);
            }
        }

        console.log(`[sync] Supabase -> Airtable processed ${processed} records.`);
    }

    async upsertSupabaseFromAirtable(record) {
        const supabasePayload = this.mapAirtableToSupabase(record);
        const referencedSupabaseId = record.supabase_id;

        let targetCar = null;

        if (referencedSupabaseId) {
            targetCar = await this.supabase.getCarById(referencedSupabaseId);
            if (!targetCar) {
                console.warn(`[sync] Airtable record ${record.airtable_id} references missing Supabase car ${referencedSupabaseId}.`);
            }
        }

        if (!targetCar && supabasePayload.external_id) {
            targetCar = await this.supabase.findCarByExternalId(supabasePayload.external_id);
        }

        // Avoid nulling required fields on updates.
        REQUIRED_FIELDS.forEach(field => {
            if (supabasePayload[field] === null) {
                delete supabasePayload[field];
            }
        });

        if (targetCar) {
            if (Object.keys(supabasePayload).length > 0) {
                await this.supabase.updateCar(targetCar.id, supabasePayload);
            }

            if (!record.supabase_id || record.supabase_id !== targetCar.id) {
                await this.airtable.updateRecord(record.airtable_id, { supabase_id: targetCar.id });
            }

            console.log(`[sync] Updated Supabase car ${targetCar.id} from Airtable record ${record.airtable_id}.`);
            return;
        }

        this.ensureRequiredFields(record.airtable_id, supabasePayload);

        const createdCar = await this.supabase.createCar({
            ...supabasePayload,
            id: referencedSupabaseId
        });

        if (!record.supabase_id || record.supabase_id !== createdCar.id) {
            await this.airtable.updateRecord(record.airtable_id, { supabase_id: createdCar.id });
        }

        console.log(`[sync] Created Supabase car ${createdCar.id} from Airtable record ${record.airtable_id}.`);
    }

    ensureRequiredFields(recordId, payload) {
        REQUIRED_FIELDS.forEach(field => {
            if (payload[field] === undefined || payload[field] === null) {
                throw new Error(`Missing required field "${field}" for Airtable record ${recordId}.`);
            }
        });
    }

    mapAirtableToSupabase(record) {
        const payload = {};

        CAR_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, record[field]);
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        return payload;
    }

    mapSupabaseToAirtable(car) {
        const payload = {
            supabase_id: car.id
        };

        CAR_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, car[field]);
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        return payload;
    }

    normalizeValue(field, value) {
        if (value === undefined) {
            return undefined;
        }

        if (value === null) {
            return null;
        }

        if (typeof value === 'string') {
            const trimmed = value.trim();

            if (trimmed === '') {
                return REQUIRED_FIELDS.includes(field) ? undefined : null;
            }

            if (NUMERIC_FIELDS.includes(field)) {
                const numericValue = Number(trimmed);
                return Number.isFinite(numericValue) ? numericValue : null;
            }

            return trimmed;
        }

        if (NUMERIC_FIELDS.includes(field)) {
            const numericValue = typeof value === 'number' ? value : Number(value);
            return Number.isFinite(numericValue) ? numericValue : null;
        }

        return value;
    }
}

module.exports = SyncEngine;
