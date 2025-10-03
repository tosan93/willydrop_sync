const config = require('./config');
const AirtableClient = require('./airtable-client');
const SupabaseClient = require('./supabase-client');

const DEFAULT_SYNC_RULES = {
    preventBlankOverwrite: true,
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

let userSyncRules = {};
try {
    userSyncRules = require('./sync-rules');
} catch (error) {
    if (error && error.code !== 'MODULE_NOT_FOUND') {
        throw error;
    }
}

const SYNC_RULES = mergeSyncRules(DEFAULT_SYNC_RULES, userSyncRules);
const EMPTY_FIELD_SET = new Set();

const CAR_FIELDS = [
    'external_id',
    'make',
    'model',
    'vin',
    'license_plate',
    'status',
    // 'customer_id', // TODO: sync linked company once mapping is ready
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
const CAR_LOCATION_LINK_FIELDS = ['pickup_location_id', 'delivery_location_id'];

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
        this.syncRules = SYNC_RULES;
        this.preventBlankOverwrite = this.syncRules.preventBlankOverwrite !== false;
        this.blankOverwriteAllowlist = this.buildBlankOverwriteAllowlist(this.syncRules.allowBlankOverwrite);

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

        if (this.airtableLocations) {
            await this.syncLocationsAirtableToSupabase();
        } else {
            console.log('[sync] Skipping Airtable -> Supabase locations (no table configured).');
        }

        await this.syncCarsAirtableToSupabase();

        if (this.airtableLocations) {
            await this.syncLocationsSupabaseToAirtable();
        } else {
            console.log('[sync] Skipping Supabase -> Airtable locations (no table configured).');
        }

        await this.syncCarsSupabaseToAirtable();

        console.log('[sync] Full sync complete.');
    }

    async syncCarsAirtableToSupabase() {
        console.log('[sync] Processing Airtable -> Supabase car changes...');

        const locationRecordsPromise = this.airtableLocations
            ? this.airtableLocations.getAllRecords()
            : Promise.resolve([]);

        const [airtableRecords, airtableLocationRecords, supabaseLocations] = await Promise.all([
            this.airtableCars.getAllRecords(),
            locationRecordsPromise,
            this.supabase.getAllLocations()
        ]);

        const locationSupabaseIdByAirtableId = new Map();

        airtableLocationRecords.forEach(locationRecord => {
            const airtableId = this.normalizeId(locationRecord.airtable_id);
            const supabaseId = this.normalizeId(locationRecord.supabase_id);
            if (airtableId && supabaseId) {
                locationSupabaseIdByAirtableId.set(airtableId, supabaseId);
            }
        });

        supabaseLocations.forEach(location => {
            const airtableId = this.normalizeId(location.airtable_id);
            const supabaseId = this.normalizeId(location.id);
            if (airtableId && supabaseId) {
                locationSupabaseIdByAirtableId.set(airtableId, supabaseId);
            }
        });

        let processed = 0;

        for (const record of airtableRecords) {
            try {
                await this.upsertCarFromAirtable(record, { locationSupabaseIdByAirtableId });
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

        const locationRecordsPromise = this.airtableLocations
            ? this.airtableLocations.getAllRecords()
            : Promise.resolve([]);

        const [supabaseCars, airtableRecords, airtableLocationRecords] = await Promise.all([
            this.supabase.getAllCars(),
            this.airtableCars.getAllRecords(),
            locationRecordsPromise
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

        const locationAirtableIdBySupabaseId = new Map();
        airtableLocationRecords.forEach(locationRecord => {
            const airtableId = this.normalizeId(locationRecord.airtable_id);
            const supabaseId = this.normalizeId(locationRecord.supabase_id);
            if (airtableId && supabaseId) {
                locationAirtableIdBySupabaseId.set(supabaseId, airtableId);
            }
        });

        let processed = 0;

        for (const car of supabaseCars) {
            try {
                const airtablePayload = this.mapCarSupabaseToAirtable(car, { locationAirtableIdBySupabaseId });
                let existingRecord = airtableBySupabaseId.get(car.id);

                if (!existingRecord && car.airtable_id) {
                    existingRecord = airtableByAirtableId.get(car.airtable_id);
                }

                if (existingRecord) {
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'cars');
                    let updatedRecord = existingRecord;

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableCars.updateRecord(existingRecord.airtable_id, updatePayload);
                        console.log(`[sync] Updated Airtable car ${updatedRecord.airtable_id} from Supabase car ${car.id}.`);
                    } else {
                        console.log(`[sync] Airtable car ${existingRecord.airtable_id} already aligned with Supabase car ${car.id}.`);
                    }

                    airtableBySupabaseId.set(car.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);
                    await this.ensureSupabaseAirtableId(car, updatedRecord.airtable_id, 'car');
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'cars');
                    const createdRecord = await this.airtableCars.createRecord(createPayload);
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
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'locations');
                    let updatedRecord = existingRecord;

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableLocations.updateRecord(existingRecord.airtable_id, updatePayload);
                        console.log(`[sync] Updated Airtable location ${updatedRecord.airtable_id} from Supabase location ${location.id}.`);
                    } else {
                        console.log(`[sync] Airtable location ${existingRecord.airtable_id} already aligned with Supabase location ${location.id}.`);
                    }

                    airtableBySupabaseId.set(location.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);
                    await this.ensureSupabaseAirtableId(location, updatedRecord.airtable_id, 'location');
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'locations');
                    const createdRecord = await this.airtableLocations.createRecord(createPayload);
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

    async upsertCarFromAirtable(record, context = {}) {
        const rawSupabasePayload = this.mapCarAirtableToSupabase(record, context);
        const referencedSupabaseId = record.supabase_id;

        let targetCar = null;

        if (referencedSupabaseId) {
            targetCar = await this.supabase.getCarById(referencedSupabaseId);
            if (!targetCar) {
                console.warn(`[sync] Airtable car ${record.airtable_id} references missing Supabase car ${referencedSupabaseId}.`);
            }
        }

        if (!targetCar && rawSupabasePayload.external_id) {
            targetCar = await this.supabase.findCarByExternalId(rawSupabasePayload.external_id);
        }

        const cleanedPayload = { ...rawSupabasePayload };
        CAR_REQUIRED_FIELDS.forEach(field => {
            if (cleanedPayload[field] === null) {
                delete cleanedPayload[field];
            }
        });

        if (targetCar) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetCar, 'airtableToSupabase', 'cars');

            if (Object.keys(updatePayload).length > 0) {
                await this.supabase.updateCar(targetCar.id, updatePayload);
            }

            if (!record.supabase_id || record.supabase_id !== targetCar.id) {
                await this.airtableCars.updateRecord(record.airtable_id, { supabase_id: targetCar.id });
            }

            console.log(`[sync] Updated Supabase car ${targetCar.id} from Airtable record ${record.airtable_id}.`);
            return;
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'cars');
        this.ensureRequiredFields(record.airtable_id, createPayload, CAR_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            createPayload.id = referencedSupabaseId;
        }

        const createdCar = await this.supabase.createCar(createPayload);

        if (!record.supabase_id || record.supabase_id !== createdCar.id) {
            await this.airtableCars.updateRecord(record.airtable_id, { supabase_id: createdCar.id });
        }

        console.log(`[sync] Created Supabase car ${createdCar.id} from Airtable record ${record.airtable_id}.`);
    }

    async upsertLocationFromAirtable(record) {
        const rawSupabasePayload = this.mapLocationAirtableToSupabase(record);
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

        const cleanedPayload = { ...rawSupabasePayload };
        LOCATION_REQUIRED_FIELDS.forEach(field => {
            if (cleanedPayload[field] === null) {
                delete cleanedPayload[field];
            }
        });

        if (targetLocation) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetLocation, 'airtableToSupabase', 'locations');

            if (Object.keys(updatePayload).length > 0) {
                await this.supabase.updateLocation(targetLocation.id, updatePayload);
            }

            if (!record.supabase_id || record.supabase_id !== targetLocation.id) {
                await this.airtableLocations.updateRecord(record.airtable_id, { supabase_id: targetLocation.id });
            }

            console.log(`[sync] Updated Supabase location ${targetLocation.id} from Airtable record ${record.airtable_id}.`);
            return;
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'locations');
        this.ensureRequiredFields(record.airtable_id, createPayload, LOCATION_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            createPayload.id = referencedSupabaseId;
        }

        const createdLocation = await this.supabase.createLocation(createPayload);

        if (!record.supabase_id || record.supabase_id !== createdLocation.id) {
            await this.airtableLocations.updateRecord(record.airtable_id, { supabase_id: createdLocation.id });
        }

        console.log(`[sync] Created Supabase location ${createdLocation.id} from Airtable record ${record.airtable_id}.`);
    }

    mapCarAirtableToSupabase(record, options = {}) {
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

        const locationMapSource = options.locationSupabaseIdByAirtableId || new Map();
        const locationSupabaseIdByAirtableId = locationMapSource instanceof Map
            ? locationMapSource
            : new Map(Object.entries(locationMapSource || {}));

        const extractFirstLinkedId = value => {
            if (Array.isArray(value) && value.length > 0) {
                const first = value[0];
                if (typeof first === 'string') {
                    return this.normalizeId(first);
                }
                if (first && typeof first === 'object' && typeof first.id === 'string') {
                    return this.normalizeId(first.id);
                }
            }
            return null;
        };

        CAR_LOCATION_LINK_FIELDS.forEach(field => {
            const linkedValue = record[field];
            const airtableLinkedId = extractFirstLinkedId(linkedValue);

            if (!airtableLinkedId) {
                payload[field] = null;
                return;
            }

            const supabaseLocationId = locationSupabaseIdByAirtableId.get(airtableLinkedId);
            if (supabaseLocationId) {
                payload[field] = supabaseLocationId;
            } else {
                console.warn(`[sync] Airtable car ${record.airtable_id} references ${field} ${airtableLinkedId}, which is missing in Supabase locations.`);
            }
        });

        const primaryFieldValue = record.id !== undefined ? record.id : (record.raw_fields && record.raw_fields.id);
        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', primaryFieldValue);
        if (airtableNameLabel !== undefined) {
            payload.airtable_id_name_label = airtableNameLabel;
        }

        return payload;
    }

    mapCarSupabaseToAirtable(car, options = {}) {
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

        const locationMapSource = options.locationAirtableIdBySupabaseId || new Map();
        const locationAirtableIdBySupabaseId = locationMapSource instanceof Map
            ? locationMapSource
            : new Map(Object.entries(locationMapSource || {}));

        CAR_LOCATION_LINK_FIELDS.forEach(field => {
            const normalizedSupabaseLocationId = this.normalizeId(car[field]);

            if (!normalizedSupabaseLocationId) {
                payload[field] = [];
                return;
            }

            const airtableLocationId = locationAirtableIdBySupabaseId.get(normalizedSupabaseLocationId);
            if (airtableLocationId) {
                payload[field] = [airtableLocationId];
            } else {
                console.warn(`[sync] Supabase car ${car.id} references ${field} ${normalizedSupabaseLocationId}, which is missing an Airtable record.`);
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

    buildBlankOverwriteAllowlist(config = {}) {
        return Object.entries(config || {}).reduce((acc, [direction, tableMap]) => {
            acc[direction] = Object.entries(tableMap || {}).reduce((tableAcc, [tableName, fields]) => {
                if (Array.isArray(fields)) {
                    tableAcc[tableName] = new Set(fields);
                }
                return tableAcc;
            }, {});
            return acc;
        }, {});
    }

    getBlankOverwriteAllowlist(direction, entityType) {
        const directionRules = this.blankOverwriteAllowlist[direction];
        if (!directionRules) {
            return EMPTY_FIELD_SET;
        }
        return directionRules[entityType] || EMPTY_FIELD_SET;
    }

    preparePayloadForUpdate(payload = {}, existingRecord = null, direction, entityType) {
        const cleanedPayload = this.removeUndefinedFromPayload(payload);

        if (!this.preventBlankOverwrite) {
            return { ...cleanedPayload };
        }

        const allowlist = this.getBlankOverwriteAllowlist(direction, entityType);
        const result = {};

        Object.entries(cleanedPayload).forEach(([field, value]) => {
            if (!this.isBlankValue(value)) {
                result[field] = value;
                return;
            }

            if (allowlist.has(field)) {
                result[field] = value;
                return;
            }

            const currentValue = existingRecord ? existingRecord[field] : undefined;
            if (currentValue !== undefined && !this.isBlankValue(currentValue)) {
                return;
            }

            result[field] = value;
        });

        return result;
    }

    removeUndefinedFromPayload(payload = {}) {
        return Object.entries(payload || {}).reduce((acc, [key, value]) => {
            if (value !== undefined) {
                acc[key] = value;
            }
            return acc;
        }, {});
    }

    isBlankValue(value) {
        if (value === undefined || value === null) {
            return true;
        }

        if (typeof value === 'string') {
            return value.trim().length === 0;
        }

        if (Array.isArray(value)) {
            return value.length === 0;
        }

        if (typeof value === 'object') {
            return Object.keys(value).length === 0;
        }

        return false;
    }

    normalizeId(value) {
        if (value === undefined || value === null) {
            return null;
        }

        if (typeof value === 'string') {
            const trimmed = value.trim();
            return trimmed.length > 0 ? trimmed : null;
        }

        if (typeof value === 'number' || typeof value === 'bigint') {
            return String(value);
        }

        return null;
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

function mergeSyncRules(defaultRules, overrides) {
    const base = defaultRules && typeof defaultRules === 'object'
        ? JSON.parse(JSON.stringify(defaultRules))
        : defaultRules;

    if (!overrides || typeof overrides !== 'object') {
        return base;
    }

    if (Array.isArray(overrides)) {
        return overrides.slice();
    }

    const result = typeof base === 'object' && base !== null ? { ...base } : {};

    Object.keys(overrides).forEach(key => {
        const overrideValue = overrides[key];
        const defaultValue = base && typeof base === 'object' ? base[key] : undefined;

        if (Array.isArray(overrideValue)) {
            result[key] = overrideValue.slice();
            return;
        }

        if (overrideValue && typeof overrideValue === 'object') {
            result[key] = mergeSyncRules(defaultValue || {}, overrideValue);
            return;
        }

        result[key] = overrideValue;
    });

    return result;
}
