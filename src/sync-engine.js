const config = require('./config');
const AirtableClient = require('./airtable-client');
const SupabaseClient = require('./supabase-client');

const CAR_FIELDS = [
    'external_id',
    'make',
    'model',
    'vin',
    'license_plate',
    'status',
    // 'customer_id', // TODO: sync linked company once mapping is ready
    // 'pickup_location_id', // TODO: sync linked pickup location once mapping is ready
    // 'delivery_location_id', // TODO: sync linked delivery location once mapping is ready
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

const CAR_NUMERIC_FIELDS = new Set(['carrier_rate', 'customer_rate', 'distance']);
const CAR_REQUIRED_FIELDS = new Set(['make', 'model']);

const LOCATION_FIELDS = [
    'address_line1',
    'address_line2',
    'city',
    'postal_code',
    'country_code',
    'latitude',
    'longitude'
];

const LOCATION_NUMERIC_FIELDS = new Set(['latitude', 'longitude']);
const LOCATION_REQUIRED_FIELDS = new Set(['address_line1', 'city', 'postal_code', 'country_code']);

class SyncEngine {
    constructor() {
        this.airtableCars = new AirtableClient();
        this.supabase = new SupabaseClient();

        const locationConfig = config.airtable.locations || {};
        if (locationConfig.tableId || locationConfig.tableName) {
            this.airtableLocations = new AirtableClient({
                tableId: locationConfig.tableId,
                tableName: locationConfig.tableName,
                fieldMapping: locationConfig.fieldMapping || {}
            });
        } else {
            this.airtableLocations = null;
        }
    }

    async runFullSync() {
        console.log('[sync] Starting full bidirectional sync...');
        await this.syncCarsAirtableToSupabase();
        if (this.airtableLocations) {
            await this.syncLocationsAirtableToSupabase();
        } else {
            console.log('[sync] Skipping Airtable -> Supabase locations (no table configured).');
        }

        await this.syncCarsSupabaseToAirtable();
        if (this.airtableLocations) {
            await this.syncLocationsSupabaseToAirtable();
        } else {
            console.log('[sync] Skipping Supabase -> Airtable locations (no table configured).');
        }
        console.log('[sync] Full sync complete.');
    }

    async syncCarsAirtableToSupabase() {
        console.log('[sync] Processing Airtable -> Supabase car changes...');

        let processed = 0;
        const airtableRecords = await this.airtableCars.getAllRecords();

        for (const record of airtableRecords) {
            try {
                await this.upsertCarFromAirtable(record);
                processed += 1;
            } catch (error) {
                console.error(`[sync] Failed to sync Airtable car ${record.airtable_id}:`, error.message);
            }
        }

        console.log(`[sync] Airtable -> Supabase processed ${processed} car records.`);
    }

    async syncLocationsAirtableToSupabase() {
        console.log('[sync] Processing Airtable -> Supabase location changes...');

        let processed = 0;
        const airtableRecords = await this.airtableLocations.getAllRecords();

        for (const record of airtableRecords) {
            try {
                await this.upsertLocationFromAirtable(record);
                processed += 1;
            } catch (error) {
                console.error(`[sync] Failed to sync Airtable location ${record.airtable_id}:`, error.message);
            }
        }

        console.log(`[sync] Airtable -> Supabase processed ${processed} location records.`);
    }

    async syncCarsSupabaseToAirtable() {
        console.log('[sync] Processing Supabase -> Airtable car changes...');

        const [supabaseCars, airtableRecords] = await Promise.all([
            this.supabase.getAllCars(),
            this.airtableCars.getAllRecords()
        ]);

        const airtableBySupabaseId = new Map();
        const airtableByAirtableId = new Map();

        airtableRecords.forEach(record => {
            if (record.supabase_id) {
                airtableBySupabaseId.set(record.supabase_id, record);
            }
            if (record.airtable_id) {
                airtableByAirtableId.set(record.airtable_id, record);
            }
        });

        let processed = 0;

        for (const car of supabaseCars) {
            try {
                const airtablePayload = this.mapCarSupabaseToAirtable(car);
                let existingRecord = airtableBySupabaseId.get(car.id);

                if (!existingRecord && car.airtable_id) {
                    existingRecord = airtableByAirtableId.get(car.airtable_id);
                }

                if (existingRecord) {
                    const updatedRecord = await this.airtableCars.updateRecord(existingRecord.airtable_id, airtablePayload);
                    airtableBySupabaseId.set(car.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);
                    await this.ensureSupabaseAirtableId(car, updatedRecord.airtable_id, 'car');
                    console.log(`[sync] Updated Airtable car ${updatedRecord.airtable_id} from Supabase car ${car.id}.`);
                } else {
                    const createdRecord = await this.airtableCars.createRecord(airtablePayload);
                    airtableBySupabaseId.set(car.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    await this.ensureSupabaseAirtableId(car, createdRecord.airtable_id, 'car');
                    console.log(`[sync] Created Airtable car ${createdRecord.airtable_id} from Supabase car ${car.id}.`);
                }

                processed += 1;
            } catch (error) {
                console.error(`[sync] Failed to sync Supabase car ${car.id}:`, error.message);
            }
        }

        console.log(`[sync] Supabase -> Airtable processed ${processed} car records.`);
    }

    async syncLocationsSupabaseToAirtable() {
        console.log('[sync] Processing Supabase -> Airtable location changes...');

        const [supabaseLocations, airtableRecords] = await Promise.all([
            this.supabase.getAllLocations(),
            this.airtableLocations.getAllRecords()
        ]);

        const airtableBySupabaseId = new Map();
        const airtableByAirtableId = new Map();

        airtableRecords.forEach(record => {
            if (record.supabase_id) {
                airtableBySupabaseId.set(record.supabase_id, record);
            }
            if (record.airtable_id) {
                airtableByAirtableId.set(record.airtable_id, record);
            }
        });

        let processed = 0;

        for (const location of supabaseLocations) {
            try {
                const airtablePayload = this.mapLocationSupabaseToAirtable(location);
                let existingRecord = airtableBySupabaseId.get(location.id);

                if (!existingRecord && location.airtable_id) {
                    existingRecord = airtableByAirtableId.get(location.airtable_id);
                }

                if (existingRecord) {
                    const updatedRecord = await this.airtableLocations.updateRecord(existingRecord.airtable_id, airtablePayload);
                    airtableBySupabaseId.set(location.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);
                    await this.ensureSupabaseAirtableId(location, updatedRecord.airtable_id, 'location');
                    console.log(`[sync] Updated Airtable location ${updatedRecord.airtable_id} from Supabase location ${location.id}.`);
                } else {
                    const createdRecord = await this.airtableLocations.createRecord(airtablePayload);
                    airtableBySupabaseId.set(location.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    await this.ensureSupabaseAirtableId(location, createdRecord.airtable_id, 'location');
                    console.log(`[sync] Created Airtable location ${createdRecord.airtable_id} from Supabase location ${location.id}.`);
                }

                processed += 1;
            } catch (error) {
                console.error(`[sync] Failed to sync Supabase location ${location.id}:`, error.message);
            }
        }

        console.log(`[sync] Supabase -> Airtable processed ${processed} location records.`);
    }

    async upsertCarFromAirtable(record) {
        const supabasePayload = this.mapCarAirtableToSupabase(record);
        const referencedSupabaseId = record.supabase_id;

        let targetCar = null;

        if (referencedSupabaseId) {
            targetCar = await this.supabase.getCarById(referencedSupabaseId);
            if (!targetCar) {
                console.warn(`[sync] Airtable car ${record.airtable_id} references missing Supabase car ${referencedSupabaseId}.`);
            }
        }

        if (!targetCar && supabasePayload.external_id) {
            targetCar = await this.supabase.findCarByExternalId(supabasePayload.external_id);
        }

        CAR_REQUIRED_FIELDS.forEach(field => {
            if (supabasePayload[field] === null) {
                delete supabasePayload[field];
            }
        });

        if (targetCar) {
            if (Object.keys(supabasePayload).length > 0) {
                await this.supabase.updateCar(targetCar.id, supabasePayload);
            }

            if (!record.supabase_id || record.supabase_id !== targetCar.id) {
                await this.airtableCars.updateRecord(record.airtable_id, { supabase_id: targetCar.id });
            }

            console.log(`[sync] Updated Supabase car ${targetCar.id} from Airtable record ${record.airtable_id}.`);
            return;
        }

        this.ensureRequiredFields(record.airtable_id, supabasePayload, CAR_REQUIRED_FIELDS);

        const createdCar = await this.supabase.createCar({
            ...supabasePayload,
            id: referencedSupabaseId
        });

        if (!record.supabase_id || record.supabase_id !== createdCar.id) {
            await this.airtableCars.updateRecord(record.airtable_id, { supabase_id: createdCar.id });
        }

        console.log(`[sync] Created Supabase car ${createdCar.id} from Airtable record ${record.airtable_id}.`);
    }

    async upsertLocationFromAirtable(record) {
        const supabasePayload = this.mapLocationAirtableToSupabase(record);
        const referencedSupabaseId = record.supabase_id;

        let targetLocation = null;

        if (referencedSupabaseId) {
            targetLocation = await this.supabase.getLocationById(referencedSupabaseId);
            if (!targetLocation) {
                console.warn(`[sync] Airtable location ${record.airtable_id} references missing Supabase location ${referencedSupabaseId}.`);
            }
        }

        if (!targetLocation && record.airtable_id) {
            targetLocation = await this.supabase.findLocationByAirtableId(record.airtable_id);
        }

        LOCATION_REQUIRED_FIELDS.forEach(field => {
            if (supabasePayload[field] === null) {
                delete supabasePayload[field];
            }
        });

        if (targetLocation) {
            if (Object.keys(supabasePayload).length > 0) {
                await this.supabase.updateLocation(targetLocation.id, supabasePayload);
            }

            if (!record.supabase_id || record.supabase_id !== targetLocation.id) {
                await this.airtableLocations.updateRecord(record.airtable_id, { supabase_id: targetLocation.id });
            }

            console.log(`[sync] Updated Supabase location ${targetLocation.id} from Airtable record ${record.airtable_id}.`);
            return;
        }

        this.ensureRequiredFields(record.airtable_id, supabasePayload, LOCATION_REQUIRED_FIELDS);

        const createdLocation = await this.supabase.createLocation({
            ...supabasePayload,
            id: referencedSupabaseId
        });

        if (!record.supabase_id || record.supabase_id !== createdLocation.id) {
            await this.airtableLocations.updateRecord(record.airtable_id, { supabase_id: createdLocation.id });
        }

        console.log(`[sync] Created Supabase location ${createdLocation.id} from Airtable record ${record.airtable_id}.`);
    }

    mapCarAirtableToSupabase(record) {
        const payload = {
            airtable_id: record.airtable_id
        };

        CAR_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, record[field], {
                numericFields: CAR_NUMERIC_FIELDS,
                requiredFields: CAR_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const primaryFieldValue = record.id !== undefined ? record.id : (record.raw_fields && record.raw_fields.id);
        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', primaryFieldValue);
        if (airtableNameLabel !== undefined) {
            payload.airtable_id_name_label = airtableNameLabel;
        }

        return payload;
    }

    mapCarSupabaseToAirtable(car) {
        const payload = {
            supabase_id: car.id
        };

        CAR_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, car[field], {
                numericFields: CAR_NUMERIC_FIELDS,
                requiredFields: CAR_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', car.airtable_id_name_label);
        if (airtableNameLabel !== undefined) {
            payload.id = airtableNameLabel;
        }

        return payload;
    }

    mapLocationAirtableToSupabase(record) {
        const payload = {
            airtable_id: record.airtable_id
        };

        LOCATION_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, record[field], {
                numericFields: LOCATION_NUMERIC_FIELDS,
                requiredFields: LOCATION_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const primaryFieldValue = record.id !== undefined ? record.id : (record.raw_fields && record.raw_fields.id);
        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', primaryFieldValue);
        if (airtableNameLabel !== undefined) {
            payload.airtable_id_name_label = airtableNameLabel;
        }

        return payload;
    }

    mapLocationSupabaseToAirtable(location) {
        const payload = {
            supabase_id: location.id
        };

        LOCATION_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, location[field], {
                numericFields: LOCATION_NUMERIC_FIELDS,
                requiredFields: LOCATION_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', location.airtable_id_name_label);
        if (airtableNameLabel !== undefined) {
            payload.id = airtableNameLabel;
        }

        return payload;
    }

    async ensureSupabaseAirtableId(entity, airtableId, entityType) {
        if (!airtableId || entity.airtable_id === airtableId) {
            return;
        }

        const updates = { airtable_id: airtableId };
        if (entityType === 'car') {
            const updated = await this.supabase.updateCar(entity.id, updates);
            Object.assign(entity, updated);
        } else {
            const updated = await this.supabase.updateLocation(entity.id, updates);
            Object.assign(entity, updated);
        }
    }

    ensureRequiredFields(recordId, payload, requiredFields) {
        requiredFields.forEach(field => {
            if (payload[field] === undefined || payload[field] === null) {
                throw new Error(`Missing required field "${field}" for Airtable record ${recordId}.`);
            }
        });
    }

    normalizeValue(field, value, options = {}) {
        const numericFields = options.numericFields instanceof Set
            ? options.numericFields
            : new Set(options.numericFields || []);
        const requiredFields = options.requiredFields instanceof Set
            ? options.requiredFields
            : new Set(options.requiredFields || []);

        if (value === undefined) {
            return undefined;
        }

        if (value === null) {
            return null;
        }

        if (typeof value === 'string') {
            const trimmed = value.trim();

            if (trimmed === '') {
                return requiredFields.has(field) ? undefined : null;
            }

            if (numericFields.has(field)) {
                const numericValue = Number(trimmed);
                return Number.isFinite(numericValue) ? numericValue : null;
            }

            return trimmed;
        }

        if (numericFields.has(field)) {
            const numericValue = typeof value === 'number' ? value : Number(value);
            return Number.isFinite(numericValue) ? numericValue : null;
        }

        return value;
    }
}

module.exports = SyncEngine;
