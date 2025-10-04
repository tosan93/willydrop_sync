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
const COMPANY_FIELDS = [
    'name',
    'type',
    'contact_person',
    'phone',
    'email'
];

const COMPANY_NUMERIC_FIELDS = new Set();
const COMPANY_REQUIRED_FIELDS = new Set(['name']);

const LOAD_FIELDS = [
    'load_number',
    'carrier_id',
    'total_distance_km',
    'estimated_duration_hours',
    'load_status',
    'transport_rate'
];

const LOAD_NUMERIC_FIELDS = new Set(['total_distance_km', 'estimated_duration_hours', 'transport_rate']);
const LOAD_REQUIRED_FIELDS = new Set(['load_number']);
const LOAD_COMPANY_LINK_FIELDS = ['carrier_id'];

const USER_FIELDS = [
    'email',
    'company_id',
    'is_active'
];

const USER_NUMERIC_FIELDS = new Set();
const USER_REQUIRED_FIELDS = new Set(['email']);
const USER_COMPANY_LINK_FIELDS = ['company_id'];

const SUPABASE_UPDATE_METHOD_BY_ENTITY = {
    car: 'updateCar',
    location: 'updateLocation',
    company: 'updateCompany',
    load: 'updateLoad',
    user: 'updateUser'
};

const LOCATION_REQUIRED_FIELDS = new Set(['address_line1', 'city', 'country_code']);

class SyncEngine {
    constructor() {
        this.airtableCars = new AirtableClient();
        this.supabase = new SupabaseClient();
        this.syncRules = SYNC_RULES;
        this.preventBlankOverwrite = this.syncRules.preventBlankOverwrite !== false;
        this.blankOverwriteAllowlist = this.buildBlankOverwriteAllowlist(this.syncRules.allowBlankOverwrite);
        this.syncToleranceMs = config.sync.syncToleranceMs || 1000;
        this.airtableSyncToleranceMs = config.sync.airtableSyncToleranceMs || 60000;

        this.airtableLocations = this.createAirtableClient(config.airtable.locations);
        this.airtableCompanies = this.createAirtableClient(config.airtable.companies);
        this.airtableLoads = this.createAirtableClient(config.airtable.loads);
        this.airtableUsers = this.createAirtableClient(config.airtable.users);
    }

    createAirtableClient(sectionConfig) {
        if (!sectionConfig) {
            return null;
        }

        const options = {};
        if (Object.prototype.hasOwnProperty.call(sectionConfig, 'tableId')) {
            const tableId = this.normalizeId(sectionConfig.tableId);
            options.tableId = tableId || null;
        }

        if (typeof sectionConfig.tableName === 'string' && sectionConfig.tableName.trim().length > 0) {
            options.tableName = sectionConfig.tableName.trim();
        }

        if (sectionConfig.fieldMapping) {
            options.fieldMapping = sectionConfig.fieldMapping || {};
        }

        if (!Object.prototype.hasOwnProperty.call(options, 'tableId') && !options.tableName) {
            return null;
        }

        return new AirtableClient(options);
    }

    async runFullSync(type = 'manual') {
        console.log('[sync] Starting full bidirectional sync...');

        await this.runSyncWithTracking('locations', 'airtable_to_supabase', type, () => this.syncLocationsAirtableToSupabase());
        await this.runSyncWithTracking('companies', 'airtable_to_supabase', type, () => this.syncCompaniesAirtableToSupabase());
        await this.runSyncWithTracking('users', 'airtable_to_supabase', type, () => this.syncUsersAirtableToSupabase());
        await this.runSyncWithTracking('cars', 'airtable_to_supabase', type, () => this.syncCarsAirtableToSupabase());
        await this.runSyncWithTracking('loads', 'airtable_to_supabase', type, () => this.syncLoadsAirtableToSupabase());

        await this.runSyncWithTracking('locations', 'supabase_to_airtable', type, () => this.syncLocationsSupabaseToAirtable());
        await this.runSyncWithTracking('companies', 'supabase_to_airtable', type, () => this.syncCompaniesSupabaseToAirtable());
        await this.runSyncWithTracking('users', 'supabase_to_airtable', type, () => this.syncUsersSupabaseToAirtable());
        await this.runSyncWithTracking('cars', 'supabase_to_airtable', type, () => this.syncCarsSupabaseToAirtable());
        await this.runSyncWithTracking('loads', 'supabase_to_airtable', type, () => this.syncLoadsSupabaseToAirtable());

        console.log('[sync] Full sync complete.');
    }

    async runSyncWithTracking(tableName, direction, type, syncFunction) {
        let syncRunId = null;

        try {
            const syncRun = await this.supabase.createSyncRun(tableName, direction, type);
            syncRunId = syncRun.id;
        } catch (error) {
            console.error(`[sync] Failed to create sync run record for ${tableName} ${direction}:`, error.message);
        }

        let stats = this.initializeStats();
        let functionError = null;

        try {
            const result = await syncFunction();
            if (result && typeof result === 'object' && result.processed !== undefined) {
                stats = result;
            }
        } catch (error) {
            functionError = error;
            console.error(`[sync] Error during ${tableName} ${direction}:`, error.message);
        }

        if (syncRunId) {
            try {
                await this.supabase.updateSyncRun(syncRunId, {
                    processed: stats.processed || 0,
                    updated: stats.updated || 0,
                    errors: stats.errors || 0,
                    finished_at: new Date().toISOString()
                });
            } catch (error) {
                console.error(`[sync] Failed to update sync run record ${syncRunId}:`, error.message);
            }
        }

        if (functionError) {
            throw functionError;
        }

        return stats;
    }

    async syncCompaniesAirtableToSupabase() {
        if (!this.airtableCompanies) {
            console.log('[sync] Skipping Airtable -> Supabase companies (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Airtable -> Supabase company changes...');

        const [airtableRecords, supabaseCompanies] = await Promise.all([
            this.airtableCompanies.getAllRecords(),
            this.supabase.getAllCompanies()
        ]);

        const supabaseCompanyByName = new Map();
        supabaseCompanies.forEach(company => {
            if (company && typeof company.name === 'string') {
                const key = company.name.trim().toLowerCase();
                if (key) {
                    supabaseCompanyByName.set(key, company);
                }
            }
        });

        const stats = this.initializeStats();

        for (const record of airtableRecords) {
            try {
                const result = await this.upsertCompanyFromAirtable(record, { supabaseCompanyByName });
                this.applySyncResult(stats, result);
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Airtable company ${record.airtable_id}:`, error.message);
            }
        }

        this.logSyncSummary('Airtable -> Supabase', 'companies', stats);
        return stats;
    }

    async syncUsersAirtableToSupabase() {
        if (!this.airtableUsers) {
            console.log('[sync] Skipping Airtable -> Supabase users (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Airtable -> Supabase user changes...');

        const companyRecordsPromise = this.airtableCompanies
            ? this.airtableCompanies.getAllRecords()
            : Promise.resolve([]);

        const [airtableRecords, airtableCompanyRecords, supabaseCompanies] = await Promise.all([
            this.airtableUsers.getAllRecords(),
            companyRecordsPromise,
            this.supabase.getAllCompanies()
        ]);

        const companySupabaseIdByAirtableId = new Map();

        airtableCompanyRecords.forEach(companyRecord => {
            const airtableId = this.normalizeId(companyRecord.airtable_id);
            const supabaseId = this.normalizeId(companyRecord.supabase_id);
            if (airtableId && supabaseId) {
                companySupabaseIdByAirtableId.set(airtableId, supabaseId);
            }
        });

        supabaseCompanies.forEach(company => {
            const airtableId = this.normalizeId(company.airtable_id);
            const supabaseId = this.normalizeId(company.id);
            if (airtableId && supabaseId && !companySupabaseIdByAirtableId.has(airtableId)) {
                companySupabaseIdByAirtableId.set(airtableId, supabaseId);
            }
        });

        const stats = this.initializeStats();

        for (const record of airtableRecords) {
            try {
                const result = await this.upsertUserFromAirtable(record, { companySupabaseIdByAirtableId });
                this.applySyncResult(stats, result);
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Airtable user ${record.airtable_id}:`, error.message);
            }
        }

        this.logSyncSummary('Airtable -> Supabase', 'users', stats);
        return stats;
    }

    async syncLoadsAirtableToSupabase() {
        if (!this.airtableLoads) {
            console.log('[sync] Skipping Airtable -> Supabase loads (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Airtable -> Supabase load changes...');

        const companyRecordsPromise = this.airtableCompanies
            ? this.airtableCompanies.getAllRecords()
            : Promise.resolve([]);

        const [airtableRecords, airtableCompanyRecords, supabaseCompanies, supabaseLoads] = await Promise.all([
            this.airtableLoads.getAllRecords(),
            companyRecordsPromise,
            this.supabase.getAllCompanies(),
            this.supabase.getAllLoads()
        ]);

        const companySupabaseIdByAirtableId = new Map();

        airtableCompanyRecords.forEach(companyRecord => {
            const airtableId = this.normalizeId(companyRecord.airtable_id);
            const supabaseId = this.normalizeId(companyRecord.supabase_id);
            if (airtableId && supabaseId) {
                companySupabaseIdByAirtableId.set(airtableId, supabaseId);
            }
        });

        supabaseCompanies.forEach(company => {
            const airtableId = this.normalizeId(company.airtable_id);
            const supabaseId = this.normalizeId(company.id);
            if (airtableId && supabaseId && !companySupabaseIdByAirtableId.has(airtableId)) {
                companySupabaseIdByAirtableId.set(airtableId, supabaseId);
            }
        });

        const supabaseLoadByNumber = new Map();
        supabaseLoads.forEach(load => {
            if (load && typeof load.load_number === 'string') {
                const key = load.load_number.trim();
                if (key) {
                    supabaseLoadByNumber.set(key, load);
                }
            }
        });

        const stats = this.initializeStats();

        for (const record of airtableRecords) {
            try {
                const result = await this.upsertLoadFromAirtable(record, { companySupabaseIdByAirtableId, supabaseLoadByNumber });
                this.applySyncResult(stats, result);
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Airtable load ${record.airtable_id}:`, error.message);
            }
        }

        this.logSyncSummary('Airtable -> Supabase', 'loads', stats);
        return stats;
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

        const stats = this.initializeStats();

        for (const record of airtableRecords) {
            try {
                const result = await this.upsertCarFromAirtable(record, { locationSupabaseIdByAirtableId });
                this.applySyncResult(stats, result);
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Airtable car ${record.airtable_id}:`, error.message);
            }
        }

        this.logSyncSummary('Airtable -> Supabase', 'cars', stats);
        return stats;
    }

    async syncLocationsAirtableToSupabase() {
        if (!this.airtableLocations) {
            console.log('[sync] Skipping Airtable -> Supabase locations (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Airtable -> Supabase location changes...');

        const stats = this.initializeStats();
        const airtableRecords = await this.airtableLocations.getAllRecords();

        for (const record of airtableRecords) {
            try {
                const result = await this.upsertLocationFromAirtable(record);
                this.applySyncResult(stats, result);
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Airtable location ${record.airtable_id}:`, error.message);
            }
        }

        this.logSyncSummary('Airtable -> Supabase', 'locations', stats);
        return stats;
    }

    async syncCompaniesSupabaseToAirtable() {
        if (!this.airtableCompanies) {
            console.log('[sync] Skipping Supabase -> Airtable companies (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Supabase -> Airtable company changes...');

        const [supabaseCompanies, airtableRecords] = await Promise.all([
            this.supabase.getAllCompanies(),
            this.airtableCompanies.getAllRecords()
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

        const stats = this.initializeStats();

        for (const company of supabaseCompanies) {
            const syncMarker = this.resolveSyncMarker(company.last_changed_for_sync, company.last_synced);
            if (this.shouldSkipSync(company.last_changed_for_sync, company.last_synced)) {
                stats.skipped += 1;
                continue;
            }

            try {
                const airtablePayload = this.mapCompanySupabaseToAirtable(company, { syncMarker });
                let existingRecord = airtableBySupabaseId.get(company.id);

                if (!existingRecord && company.airtable_id) {
                    existingRecord = airtableByAirtableId.get(company.airtable_id);
                }

                if (existingRecord) {
                    const comparison = this.compareTimestamps(company.last_changed_for_sync, existingRecord.last_synced);
                    if (comparison === 'dest_newer') {
                        console.log(`[sync] Skipping Supabase company ${company.id} -> Airtable: destination is newer`);
                        this.applySyncResult(stats, { action: 'unchanged' });
                        continue;
                    }
                }

                if (existingRecord) {
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'companies');
                    let updatedRecord = existingRecord;
                    let action = 'unchanged';

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableCompanies.updateRecord(existingRecord.airtable_id, updatePayload);
                        action = 'updated';
                    }

                    airtableBySupabaseId.set(company.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);

                    const linkChanged = this.normalizeId(company.airtable_id) !== this.normalizeId(updatedRecord.airtable_id);
                    await this.ensureSupabaseAirtableId(company, updatedRecord.airtable_id, 'company');
                    if (linkChanged && action === 'unchanged') {
                        action = 'updated';
                    }

                    this.applySyncResult(stats, { action });
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'companies');
                    const createdRecord = await this.airtableCompanies.createRecord(createPayload);
                    airtableBySupabaseId.set(company.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    await this.ensureSupabaseAirtableId(company, createdRecord.airtable_id, 'company');
                    this.applySyncResult(stats, { action: 'created' });
                }

                await this.supabase.updateCompany(company.id, { last_synced: syncMarker });
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Supabase company ${company.id}:`, error.message);
            }
        }

        this.logSyncSummary('Supabase -> Airtable', 'companies', stats);
        return stats;
    }

    async syncUsersSupabaseToAirtable() {
        if (!this.airtableUsers) {
            console.log('[sync] Skipping Supabase -> Airtable users (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Supabase -> Airtable user changes...');

        const companyRecordsPromise = this.airtableCompanies
            ? this.airtableCompanies.getAllRecords()
            : Promise.resolve([]);

        const [supabaseUsers, airtableRecords, airtableCompanyRecords] = await Promise.all([
            this.supabase.getAllUsers(),
            this.airtableUsers.getAllRecords(),
            companyRecordsPromise
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

        const companyAirtableIdBySupabaseId = new Map();
        airtableCompanyRecords.forEach(companyRecord => {
            const airtableId = this.normalizeId(companyRecord.airtable_id);
            const supabaseId = this.normalizeId(companyRecord.supabase_id);
            if (airtableId && supabaseId) {
                companyAirtableIdBySupabaseId.set(supabaseId, airtableId);
            }
        });

        const stats = this.initializeStats();

        for (const user of supabaseUsers) {
            const syncMarker = this.resolveSyncMarker(user.last_changed_for_sync, user.last_synced);
            if (this.shouldSkipSync(user.last_changed_for_sync, user.last_synced)) {
                stats.skipped += 1;
                continue;
            }

            try {
                const airtablePayload = this.mapUserSupabaseToAirtable(user, { companyAirtableIdBySupabaseId, syncMarker });
                let existingRecord = airtableBySupabaseId.get(user.id);

                if (!existingRecord && user.airtable_id) {
                    existingRecord = airtableByAirtableId.get(user.airtable_id);
                }

                if (existingRecord) {
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'users');
                    let updatedRecord = existingRecord;
                    let action = 'unchanged';

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableUsers.updateRecord(existingRecord.airtable_id, updatePayload);
                        action = 'updated';
                    }

                    airtableBySupabaseId.set(user.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);

                    const linkChanged = this.normalizeId(user.airtable_id) !== this.normalizeId(updatedRecord.airtable_id);
                    await this.ensureSupabaseAirtableId(user, updatedRecord.airtable_id, 'user');
                    if (linkChanged && action === 'unchanged') {
                        action = 'updated';
                    }

                    this.applySyncResult(stats, { action });
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'users');
                    const createdRecord = await this.airtableUsers.createRecord(createPayload);
                    airtableBySupabaseId.set(user.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    await this.ensureSupabaseAirtableId(user, createdRecord.airtable_id, 'user');
                    this.applySyncResult(stats, { action: 'created' });
                }

                await this.supabase.updateUser(user.id, { last_synced: syncMarker });
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Supabase user ${user.id}:`, error.message);
            }
        }

        this.logSyncSummary('Supabase -> Airtable', 'users', stats);
        return stats;
    }

    async syncLoadsSupabaseToAirtable() {
        if (!this.airtableLoads) {
            console.log('[sync] Skipping Supabase -> Airtable loads (no table configured).');
            return this.initializeStats();
        }

        console.log('[sync] Processing Supabase -> Airtable load changes...');

        const companyRecordsPromise = this.airtableCompanies
            ? this.airtableCompanies.getAllRecords()
            : Promise.resolve([]);

        const [supabaseLoads, airtableRecords, airtableCompanyRecords] = await Promise.all([
            this.supabase.getAllLoads(),
            this.airtableLoads.getAllRecords(),
            companyRecordsPromise
        ]);

        const airtableBySupabaseId = new Map();
        const airtableByAirtableId = new Map();
        const airtableByLoadNumber = new Map();

        airtableRecords.forEach(record => {
            if (record.supabase_id) {
                airtableBySupabaseId.set(record.supabase_id, record);
            }
            if (record.airtable_id) {
                airtableByAirtableId.set(record.airtable_id, record);
            }
            if (record.load_number) {
                const normalized = typeof record.load_number === 'string' ? record.load_number.trim() : null;
                if (normalized) {
                    airtableByLoadNumber.set(normalized, record);
                }
            }
        });

        const companyAirtableIdBySupabaseId = new Map();
        airtableCompanyRecords.forEach(companyRecord => {
            const airtableId = this.normalizeId(companyRecord.airtable_id);
            const supabaseId = this.normalizeId(companyRecord.supabase_id);
            if (airtableId && supabaseId) {
                companyAirtableIdBySupabaseId.set(supabaseId, airtableId);
            }
        });

        const stats = this.initializeStats();

        for (const load of supabaseLoads) {
            const syncMarker = this.resolveSyncMarker(load.last_changed_for_sync, load.last_synced);
            if (this.shouldSkipSync(load.last_changed_for_sync, load.last_synced)) {
                stats.skipped += 1;
                continue;
            }

            try {
                const airtablePayload = this.mapLoadSupabaseToAirtable(load, { companyAirtableIdBySupabaseId, syncMarker });
                let existingRecord = airtableBySupabaseId.get(load.id);

                if (!existingRecord && load.airtable_id) {
                    existingRecord = airtableByAirtableId.get(load.airtable_id);
                }

                if (!existingRecord && load.load_number) {
                    const normalizedLoadNumber = typeof load.load_number === 'string' ? load.load_number.trim() : null;
                    if (normalizedLoadNumber) {
                        existingRecord = airtableByLoadNumber.get(normalizedLoadNumber);
                    }
                }

                if (existingRecord) {
                    const comparison = this.compareTimestamps(load.last_changed_for_sync, existingRecord.last_synced);
                    if (comparison === 'dest_newer') {
                        console.log(`[sync] Skipping Supabase load ${load.id} -> Airtable: destination is newer`);
                        this.applySyncResult(stats, { action: 'unchanged' });
                        continue;
                    }
                }

                if (existingRecord) {
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'loads');
                    let updatedRecord = existingRecord;
                    let action = 'unchanged';

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableLoads.updateRecord(existingRecord.airtable_id, updatePayload);
                        action = 'updated';
                    }

                    airtableBySupabaseId.set(load.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);
                    if (updatedRecord.load_number) {
                        const normalized = typeof updatedRecord.load_number === 'string' ? updatedRecord.load_number.trim() : null;
                        if (normalized) {
                            airtableByLoadNumber.set(normalized, updatedRecord);
                        }
                    }

                    const linkChanged = this.normalizeId(load.airtable_id) !== this.normalizeId(updatedRecord.airtable_id);
                    await this.ensureSupabaseAirtableId(load, updatedRecord.airtable_id, 'load');
                    if (linkChanged && action === 'unchanged') {
                        action = 'updated';
                    }

                    this.applySyncResult(stats, { action });
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'loads');
                    const createdRecord = await this.airtableLoads.createRecord(createPayload);
                    airtableBySupabaseId.set(load.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    if (createdRecord.load_number) {
                        const normalized = typeof createdRecord.load_number === 'string' ? createdRecord.load_number.trim() : null;
                        if (normalized) {
                            airtableByLoadNumber.set(normalized, createdRecord);
                        }
                    }
                    await this.ensureSupabaseAirtableId(load, createdRecord.airtable_id, 'load');
                    this.applySyncResult(stats, { action: 'created' });
                }

                await this.supabase.updateLoad(load.id, { last_synced: syncMarker });
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Supabase load ${load.id}:`, error.message);
            }
        }

        this.logSyncSummary('Supabase -> Airtable', 'loads', stats);
        return stats;
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

        const stats = this.initializeStats();

        for (const car of supabaseCars) {
            const syncMarker = this.resolveSyncMarker(car.last_changed_for_sync, car.last_synced);
            if (this.shouldSkipSync(car.last_changed_for_sync, car.last_synced)) {
                stats.skipped += 1;
                continue;
            }

            try {
                const airtablePayload = this.mapCarSupabaseToAirtable(car, { locationAirtableIdBySupabaseId, syncMarker });
                let existingRecord = airtableBySupabaseId.get(car.id);

                if (!existingRecord && car.airtable_id) {
                    existingRecord = airtableByAirtableId.get(car.airtable_id);
                }

                if (existingRecord) {
                    const comparison = this.compareTimestamps(car.last_changed_for_sync, existingRecord.last_synced);
                    if (comparison === 'dest_newer') {
                        console.log(`[sync] Skipping Supabase car ${car.id} -> Airtable: destination is newer`);
                        this.applySyncResult(stats, { action: 'unchanged' });
                        continue;
                    }
                }

                if (existingRecord) {
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'cars');
                    let updatedRecord = existingRecord;
                    let action = 'unchanged';

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableCars.updateRecord(existingRecord.airtable_id, updatePayload);
                        action = 'updated';
                    }

                    airtableBySupabaseId.set(car.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);

                    const linkChanged = this.normalizeId(car.airtable_id) !== this.normalizeId(updatedRecord.airtable_id);
                    await this.ensureSupabaseAirtableId(car, updatedRecord.airtable_id, 'car');
                    if (linkChanged && action === 'unchanged') {
                        action = 'updated';
                    }

                    this.applySyncResult(stats, { action });
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'cars');
                    const createdRecord = await this.airtableCars.createRecord(createPayload);
                    airtableBySupabaseId.set(car.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    await this.ensureSupabaseAirtableId(car, createdRecord.airtable_id, 'car');
                    this.applySyncResult(stats, { action: 'created' });
                }

                await this.supabase.updateCar(car.id, { last_synced: syncMarker });
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Supabase car ${car.id}:`, error.message);
            }
        }

        this.logSyncSummary('Supabase -> Airtable', 'cars', stats);
        return stats;
    }

    async syncLocationsSupabaseToAirtable() {
        if (!this.airtableLocations) {
            console.log('[sync] Skipping Supabase -> Airtable locations (no table configured).');
            return this.initializeStats();
        }

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

        const stats = this.initializeStats();

        for (const location of supabaseLocations) {
            const syncMarker = this.resolveSyncMarker(location.last_changed_for_sync, location.last_synced);
            if (this.shouldSkipSync(location.last_changed_for_sync, location.last_synced)) {
                stats.skipped += 1;
                continue;
            }

            try {
                const airtablePayload = this.mapLocationSupabaseToAirtable(location, { syncMarker });
                let existingRecord = airtableBySupabaseId.get(location.id);

                if (!existingRecord && location.airtable_id) {
                    existingRecord = airtableByAirtableId.get(location.airtable_id);
                }

                if (existingRecord) {
                    const comparison = this.compareTimestamps(location.last_changed_for_sync, existingRecord.last_synced);
                    if (comparison === 'dest_newer') {
                        console.log(`[sync] Skipping Supabase location ${location.id} -> Airtable: destination is newer`);
                        this.applySyncResult(stats, { action: 'unchanged' });
                        continue;
                    }
                }

                if (existingRecord) {
                    const updatePayload = this.preparePayloadForUpdate(airtablePayload, existingRecord, 'supabaseToAirtable', 'locations');
                    let updatedRecord = existingRecord;
                    let action = 'unchanged';

                    if (Object.keys(updatePayload).length > 0) {
                        updatedRecord = await this.airtableLocations.updateRecord(existingRecord.airtable_id, updatePayload);
                        action = 'updated';
                    }

                    airtableBySupabaseId.set(location.id, updatedRecord);
                    airtableByAirtableId.set(updatedRecord.airtable_id, updatedRecord);

                    const linkChanged = this.normalizeId(location.airtable_id) !== this.normalizeId(updatedRecord.airtable_id);
                    await this.ensureSupabaseAirtableId(location, updatedRecord.airtable_id, 'location');
                    if (linkChanged && action === 'unchanged') {
                        action = 'updated';
                    }

                    this.applySyncResult(stats, { action });
                } else {
                    const createPayload = this.preparePayloadForUpdate(airtablePayload, null, 'supabaseToAirtable', 'locations');
                    const createdRecord = await this.airtableLocations.createRecord(createPayload);
                    airtableBySupabaseId.set(location.id, createdRecord);
                    airtableByAirtableId.set(createdRecord.airtable_id, createdRecord);
                    await this.ensureSupabaseAirtableId(location, createdRecord.airtable_id, 'location');
                    this.applySyncResult(stats, { action: 'created' });
                }

                await this.supabase.updateLocation(location.id, { last_synced: syncMarker });
            } catch (error) {
                stats.errors += 1;
                console.error(`[sync] Failed to sync Supabase location ${location.id}:`, error.message);
            }
        }

        this.logSyncSummary('Supabase -> Airtable', 'locations', stats);
        return stats;
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

        if (targetCar && this.shouldSkipSync(targetCar.last_changed_for_sync, targetCar.last_synced)) {
            return { action: 'unchanged', supabaseId: targetCar.id };
        }

        if (targetCar) {
            const comparison = this.compareTimestamps(record.last_changed_for_sync, targetCar.last_changed_for_sync, this.airtableSyncToleranceMs);
            if (comparison === 'dest_newer') {
                console.log(`[sync] Skipping Airtable car ${record.airtable_id} -> Supabase: destination is newer (AT: ${record.last_changed_for_sync}, SB: ${targetCar.last_changed_for_sync})`);
                return { action: 'unchanged', supabaseId: targetCar.id };
            }
        }

        const cleanedPayload = { ...rawSupabasePayload };
        CAR_REQUIRED_FIELDS.forEach(field => {
            if (cleanedPayload[field] === null) {
                delete cleanedPayload[field];
            }
        });

        if (targetCar) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetCar, 'airtableToSupabase', 'cars');
            let updatedCar = targetCar;
            let action = 'unchanged';

            if (Object.keys(updatePayload).length > 0) {
                updatedCar = await this.supabase.updateCar(targetCar.id, updatePayload);
                action = 'updated';
            }

            const needsLinkUpdate = !record.supabase_id || record.supabase_id !== updatedCar.id;
            if (needsLinkUpdate) {
                await this.airtableCars.updateRecord(record.airtable_id, { supabase_id: updatedCar.id });
                if (action === 'unchanged') {
                    action = 'updated';
                }
            }

            return { action, supabaseId: updatedCar.id };
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'cars');
        this.ensureRequiredFields(record.airtable_id, createPayload, CAR_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            createPayload.id = referencedSupabaseId;
        }

        const createdCar = await this.supabase.createCar(createPayload);

        const needsLinkUpdate = !record.supabase_id || record.supabase_id !== createdCar.id;
        if (needsLinkUpdate) {
            await this.airtableCars.updateRecord(record.airtable_id, { supabase_id: createdCar.id });
        }

        return { action: 'created', supabaseId: createdCar.id };
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

        if (targetLocation && this.shouldSkipSync(targetLocation.last_changed_for_sync, targetLocation.last_synced)) {
            return { action: 'unchanged', supabaseId: targetLocation.id };
        }

        if (targetLocation) {
            const comparison = this.compareTimestamps(record.last_changed_for_sync, targetLocation.last_changed_for_sync, this.airtableSyncToleranceMs);
            if (comparison === 'dest_newer') {
                console.log(`[sync] Skipping Airtable location ${record.airtable_id} -> Supabase: destination is newer (AT: ${record.last_changed_for_sync}, SB: ${targetLocation.last_changed_for_sync})`);
                return { action: 'unchanged', supabaseId: targetLocation.id };
            }
        }

        const cleanedPayload = { ...rawSupabasePayload };
        LOCATION_REQUIRED_FIELDS.forEach(field => {
            if (cleanedPayload[field] === null) {
                delete cleanedPayload[field];
            }
        });

        if (targetLocation) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetLocation, 'airtableToSupabase', 'locations');
            let updatedLocation = targetLocation;
            let action = 'unchanged';

            if (Object.keys(updatePayload).length > 0) {
                updatedLocation = await this.supabase.updateLocation(targetLocation.id, updatePayload);
                action = 'updated';
            }

            const needsLinkUpdate = !record.supabase_id || record.supabase_id !== updatedLocation.id;
            if (needsLinkUpdate) {
                await this.airtableLocations.updateRecord(record.airtable_id, { supabase_id: updatedLocation.id });
                if (action === 'unchanged') {
                    action = 'updated';
                }
            }

            return { action, supabaseId: updatedLocation.id };
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'locations');
        this.ensureRequiredFields(record.airtable_id, createPayload, LOCATION_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            createPayload.id = referencedSupabaseId;
        }

        const createdLocation = await this.supabase.createLocation(createPayload);

        const needsLinkUpdate = !record.supabase_id || record.supabase_id !== createdLocation.id;
        if (needsLinkUpdate) {
            await this.airtableLocations.updateRecord(record.airtable_id, { supabase_id: createdLocation.id });
        }

        return { action: 'created', supabaseId: createdLocation.id };
    }

    async upsertCompanyFromAirtable(record, context = {}) {
        if (!this.airtableCompanies) {
            return { action: 'skipped' };
        }

        const rawSupabasePayload = this.mapCompanyAirtableToSupabase(record);
        const referencedSupabaseId = this.normalizeId(record.supabase_id);
        const nameLookup = context.supabaseCompanyByName instanceof Map
            ? context.supabaseCompanyByName
            : new Map();

        let targetCompany = null;

        if (referencedSupabaseId) {
            targetCompany = await this.supabase.getCompanyById(referencedSupabaseId);
            if (!targetCompany) {
                console.warn(`[sync] Airtable company ${record.airtable_id} references missing Supabase company ${referencedSupabaseId}.`);
            }
        }

        if (!targetCompany && rawSupabasePayload.name) {
            const lookupKey = rawSupabasePayload.name.trim().toLowerCase();
            if (lookupKey) {
                targetCompany = nameLookup.get(lookupKey) || await this.supabase.findCompanyByName(rawSupabasePayload.name);
            }
        }

        if (targetCompany && this.shouldSkipSync(targetCompany.last_changed_for_sync, targetCompany.last_synced)) {
            return { action: 'unchanged', supabaseId: targetCompany.id };
        }

        if (targetCompany) {
            const comparison = this.compareTimestamps(record.last_changed_for_sync, targetCompany.last_changed_for_sync, this.airtableSyncToleranceMs);
            if (comparison === 'dest_newer') {
                console.log(`[sync] Skipping Airtable company ${record.airtable_id} -> Supabase: destination is newer (AT: ${record.last_changed_for_sync}, SB: ${targetCompany.last_changed_for_sync})`);
                return { action: 'unchanged', supabaseId: targetCompany.id };
            }
        }

        const cleanedPayload = { ...rawSupabasePayload };

        if (targetCompany) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetCompany, 'airtableToSupabase', 'companies');
            let updatedCompany = targetCompany;
            let action = 'unchanged';

            if (Object.keys(updatePayload).length > 0) {
                updatedCompany = await this.supabase.updateCompany(targetCompany.id, updatePayload);
                action = 'updated';
            }

            const needsLinkUpdate = !record.supabase_id || record.supabase_id !== updatedCompany.id;
            if (needsLinkUpdate) {
                await this.airtableCompanies.updateRecord(record.airtable_id, { supabase_id: updatedCompany.id });
                if (action === 'unchanged') {
                    action = 'updated';
                }
            }

            if (updatedCompany.name) {
                const key = updatedCompany.name.trim().toLowerCase();
                if (key) {
                    nameLookup.set(key, updatedCompany);
                }
            }

            return { action, supabaseId: updatedCompany.id };
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'companies');
        this.ensureRequiredFields(record.airtable_id, createPayload, COMPANY_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            createPayload.id = referencedSupabaseId;
        }

        const createdCompany = await this.supabase.createCompany(createPayload);

        const needsLinkUpdate = !record.supabase_id || record.supabase_id !== createdCompany.id;
        if (needsLinkUpdate) {
            await this.airtableCompanies.updateRecord(record.airtable_id, { supabase_id: createdCompany.id });
        }

        if (createdCompany.name) {
            const key = createdCompany.name.trim().toLowerCase();
            if (key) {
                nameLookup.set(key, createdCompany);
            }
        }

        return { action: 'created', supabaseId: createdCompany.id };
    }

    async upsertUserFromAirtable(record, context = {}) {
        if (!this.airtableUsers) {
            return { action: 'skipped' };
        }

        const rawSupabasePayload = this.mapUserAirtableToSupabase(record, context);
        const referencedSupabaseId = this.normalizeId(record.supabase_id);

        let targetUser = null;

        if (referencedSupabaseId) {
            targetUser = await this.supabase.getUserById(referencedSupabaseId);
            if (!targetUser) {
                console.warn(`[sync] Airtable user ${record.airtable_id} references missing Supabase user ${referencedSupabaseId}.`);
            }
        }

        if (!targetUser && rawSupabasePayload.email) {
            targetUser = await this.supabase.findUserByEmail(rawSupabasePayload.email);
        }

        if (targetUser && this.shouldSkipSync(targetUser.last_changed_for_sync, targetUser.last_synced)) {
            return { action: 'unchanged', supabaseId: targetUser.id };
        }

        if (targetUser) {
            const comparison = this.compareTimestamps(record.last_changed_for_sync, targetUser.last_changed_for_sync, this.airtableSyncToleranceMs);
            if (comparison === 'dest_newer') {
                console.log(`[sync] Skipping Airtable user ${record.airtable_id} -> Supabase: destination is newer (AT: ${record.last_changed_for_sync}, SB: ${targetUser.last_changed_for_sync})`);
                return { action: 'unchanged', supabaseId: targetUser.id };
            }
        }

        const cleanedPayload = { ...rawSupabasePayload };

        if (targetUser) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetUser, 'airtableToSupabase', 'users');
            let updatedUser = targetUser;
            let action = 'unchanged';

            if (Object.keys(updatePayload).length > 0) {
                updatedUser = await this.supabase.updateUser(targetUser.id, updatePayload);
                action = 'updated';
            }

            const needsLinkUpdate = !record.supabase_id || record.supabase_id !== updatedUser.id;
            if (needsLinkUpdate) {
                await this.airtableUsers.updateRecord(record.airtable_id, { supabase_id: updatedUser.id });
                if (action === 'unchanged') {
                    action = 'updated';
                }
            }

            return { action, supabaseId: updatedUser.id };
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'users');
        this.ensureRequiredFields(record.airtable_id, createPayload, USER_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            console.warn(`[sync] Airtable user ${record.airtable_id} referenced Supabase user ${referencedSupabaseId}, which was not found; skipping creation.`);
            return { action: 'skipped' };
        }

        const createdUser = await this.supabase.createUser(createPayload);

        const needsLinkUpdate = !record.supabase_id || record.supabase_id !== createdUser.id;
        if (needsLinkUpdate) {
            await this.airtableUsers.updateRecord(record.airtable_id, { supabase_id: createdUser.id });
        }

        return { action: 'created', supabaseId: createdUser.id };
    }

    async upsertLoadFromAirtable(record, context = {}) {
        if (!this.airtableLoads) {
            return { action: 'skipped' };
        }

        const rawSupabasePayload = this.mapLoadAirtableToSupabase(record, context);
        const referencedSupabaseId = this.normalizeId(record.supabase_id);
        const loadNumberLookup = context.supabaseLoadByNumber instanceof Map
            ? context.supabaseLoadByNumber
            : new Map();

        let targetLoad = null;

        if (referencedSupabaseId) {
            targetLoad = await this.supabase.getLoadById(referencedSupabaseId);
            if (!targetLoad) {
                console.warn(`[sync] Airtable load ${record.airtable_id} references missing Supabase load ${referencedSupabaseId}.`);
            }
        }

        if (!targetLoad && rawSupabasePayload.load_number) {
            const key = rawSupabasePayload.load_number.trim();
            if (key) {
                targetLoad = loadNumberLookup.get(key) || await this.supabase.findLoadByLoadNumber(key);
            }
        }

        if (targetLoad && this.shouldSkipSync(targetLoad.last_changed_for_sync, targetLoad.last_synced)) {
            return { action: 'unchanged', supabaseId: targetLoad.id };
        }

        if (targetLoad) {
            const comparison = this.compareTimestamps(record.last_changed_for_sync, targetLoad.last_changed_for_sync, this.airtableSyncToleranceMs);
            if (comparison === 'dest_newer') {
                console.log(`[sync] Skipping Airtable load ${record.airtable_id} -> Supabase: destination is newer (AT: ${record.last_changed_for_sync}, SB: ${targetLoad.last_changed_for_sync})`);
                return { action: 'unchanged', supabaseId: targetLoad.id };
            }
        }

        const cleanedPayload = { ...rawSupabasePayload };

        if (targetLoad) {
            const updatePayload = this.preparePayloadForUpdate(cleanedPayload, targetLoad, 'airtableToSupabase', 'loads');
            let updatedLoad = targetLoad;
            let action = 'unchanged';

            if (Object.keys(updatePayload).length > 0) {
                updatedLoad = await this.supabase.updateLoad(targetLoad.id, updatePayload);
                action = 'updated';
            }

            const needsLinkUpdate = !record.supabase_id || record.supabase_id !== updatedLoad.id;
            if (needsLinkUpdate) {
                await this.airtableLoads.updateRecord(record.airtable_id, { supabase_id: updatedLoad.id });
                if (action === 'unchanged') {
                    action = 'updated';
                }
            }

            if (updatedLoad.load_number) {
                const normalized = updatedLoad.load_number.trim();
                if (normalized) {
                    loadNumberLookup.set(normalized, updatedLoad);
                }
            }

            return { action, supabaseId: updatedLoad.id };
        }

        const createPayload = this.preparePayloadForUpdate(cleanedPayload, null, 'airtableToSupabase', 'loads');
        this.ensureRequiredFields(record.airtable_id, createPayload, LOAD_REQUIRED_FIELDS);
        if (referencedSupabaseId) {
            createPayload.id = referencedSupabaseId;
        }

        const createdLoad = await this.supabase.createLoad(createPayload);

        const needsLinkUpdate = !record.supabase_id || record.supabase_id !== createdLoad.id;
        if (needsLinkUpdate) {
            await this.airtableLoads.updateRecord(record.airtable_id, { supabase_id: createdLoad.id });
        }

        if (createdLoad.load_number) {
            const normalized = createdLoad.load_number.trim();
            if (normalized) {
                loadNumberLookup.set(normalized, createdLoad);
            }
        }

        return { action: 'created', supabaseId: createdLoad.id };
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

        const locationSupabaseIdByAirtableId = options.locationSupabaseIdByAirtableId instanceof Map
            ? options.locationSupabaseIdByAirtableId
            : new Map(Object.entries(options.locationSupabaseIdByAirtableId || {}));

        CAR_LOCATION_LINK_FIELDS.forEach(field => {
            const airtableLinkedId = this.extractLinkedRecordId(record[field]);

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

        const syncMarker = this.normalizeSyncValue(options.syncMarker) || this.resolveSyncMarker(car.last_changed_for_sync, car.last_synced);
        if (syncMarker) {
            payload.last_synced = syncMarker;
        }

        CAR_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, car[field], {
                numericFields: CAR_NUMERIC_FIELDS,
                requiredFields: CAR_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const locationAirtableIdBySupabaseId = options.locationAirtableIdBySupabaseId instanceof Map
            ? options.locationAirtableIdBySupabaseId
            : new Map(Object.entries(options.locationAirtableIdBySupabaseId || {}));

        CAR_LOCATION_LINK_FIELDS.forEach(field => {
            const supabaseLocationId = this.normalizeId(car[field]);

            if (!supabaseLocationId) {
                payload[field] = [];
                return;
            }

            const airtableLocationId = locationAirtableIdBySupabaseId.get(supabaseLocationId);
            if (airtableLocationId) {
                payload[field] = [airtableLocationId];
            } else {
                console.warn(`[sync] Supabase car ${car.id} references ${field} ${supabaseLocationId}, which is missing an Airtable record.`);
            }
        });
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

    mapLocationSupabaseToAirtable(location, options = {}) {
        const payload = {
            supabase_id: location.id
        };

        const syncMarker = this.normalizeSyncValue(options.syncMarker) || this.resolveSyncMarker(location.last_changed_for_sync, location.last_synced);
        if (syncMarker) {
            payload.last_synced = syncMarker;
        }

        LOCATION_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, location[field], {
                numericFields: LOCATION_NUMERIC_FIELDS,
                requiredFields: LOCATION_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });
        return payload;
    }

    mapCompanyAirtableToSupabase(record) {
        const payload = {
            airtable_id: record.airtable_id
        };

        COMPANY_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, record[field], {
                numericFields: COMPANY_NUMERIC_FIELDS,
                requiredFields: COMPANY_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', record.id !== undefined ? record.id : (record.raw_fields && record.raw_fields.id));
        if (airtableNameLabel !== undefined) {
            payload.airtable_id_name_label = airtableNameLabel;
        }

        return payload;
    }

    mapCompanySupabaseToAirtable(company, options = {}) {
        const payload = {
            supabase_id: company.id
        };

        const syncMarker = this.normalizeSyncValue(options.syncMarker) || this.resolveSyncMarker(company.last_changed_for_sync, company.last_synced);
        if (syncMarker) {
            payload.last_synced = syncMarker;
        }

        COMPANY_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, company[field], {
                numericFields: COMPANY_NUMERIC_FIELDS,
                requiredFields: COMPANY_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });
        return payload;
    }

    mapUserAirtableToSupabase(record, options = {}) {
        const payload = {
            airtable_id: record.airtable_id
        };

        USER_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, record[field], {
                numericFields: USER_NUMERIC_FIELDS,
                requiredFields: USER_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const companySupabaseIdByAirtableId = options.companySupabaseIdByAirtableId instanceof Map
            ? options.companySupabaseIdByAirtableId
            : new Map(Object.entries(options.companySupabaseIdByAirtableId || {}));

        USER_COMPANY_LINK_FIELDS.forEach(field => {
            const airtableLinkedId = this.extractLinkedRecordId(record[field]);

            if (!airtableLinkedId) {
                payload[field] = null;
                return;
            }

            const supabaseCompanyId = companySupabaseIdByAirtableId.get(airtableLinkedId);
            if (supabaseCompanyId) {
                payload[field] = supabaseCompanyId;
            } else {
                console.warn(`[sync] Airtable user ${record.airtable_id} references ${field} ${airtableLinkedId}, which is missing in Supabase companies.`);
            }
        });

        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', record.id !== undefined ? record.id : (record.raw_fields && record.raw_fields.id));
        if (airtableNameLabel !== undefined) {
            payload.airtable_id_name_label = airtableNameLabel;
        }

        return payload;
    }

    mapUserSupabaseToAirtable(user, options = {}) {
        const payload = {
            supabase_id: user.id
        };

        const syncMarker = this.normalizeSyncValue(options.syncMarker) || this.resolveSyncMarker(user.last_changed_for_sync, user.last_synced);
        if (syncMarker) {
            payload.last_synced = syncMarker;
        }

        USER_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, user[field], {
                numericFields: USER_NUMERIC_FIELDS,
                requiredFields: USER_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const companyAirtableIdBySupabaseId = options.companyAirtableIdBySupabaseId instanceof Map
            ? options.companyAirtableIdBySupabaseId
            : new Map(Object.entries(options.companyAirtableIdBySupabaseId || {}));

        USER_COMPANY_LINK_FIELDS.forEach(field => {
            const supabaseCompanyId = this.normalizeId(user[field]);

            if (!supabaseCompanyId) {
                payload[field] = [];
                return;
            }

            const airtableCompanyId = companyAirtableIdBySupabaseId.get(supabaseCompanyId);
            if (airtableCompanyId) {
                payload[field] = [airtableCompanyId];
            } else {
                console.warn(`[sync] Supabase user ${user.id} references ${field} ${supabaseCompanyId}, which is missing an Airtable record.`);
            }
        });
        return payload;
    }

    mapLoadAirtableToSupabase(record, options = {}) {
        const payload = {
            airtable_id: record.airtable_id
        };

        LOAD_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, record[field], {
                numericFields: LOAD_NUMERIC_FIELDS,
                requiredFields: LOAD_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const companySupabaseIdByAirtableId = options.companySupabaseIdByAirtableId instanceof Map
            ? options.companySupabaseIdByAirtableId
            : new Map(Object.entries(options.companySupabaseIdByAirtableId || {}));

        LOAD_COMPANY_LINK_FIELDS.forEach(field => {
            const airtableLinkedId = this.extractLinkedRecordId(record[field]);

            if (!airtableLinkedId) {
                payload[field] = null;
                return;
            }

            const supabaseCompanyId = companySupabaseIdByAirtableId.get(airtableLinkedId);
            if (supabaseCompanyId) {
                payload[field] = supabaseCompanyId;
            } else {
                console.warn(`[sync] Airtable load ${record.airtable_id} references ${field} ${airtableLinkedId}, which is missing in Supabase companies.`);
            }
        });

        const airtableNameLabel = this.normalizeValue('airtable_id_name_label', record.id !== undefined ? record.id : (record.raw_fields && record.raw_fields.id));
        if (airtableNameLabel !== undefined) {
            payload.airtable_id_name_label = airtableNameLabel;
        }

        return payload;
    }

    mapLoadSupabaseToAirtable(load, options = {}) {
        const payload = {
            supabase_id: load.id
        };

        const syncMarker = this.normalizeSyncValue(options.syncMarker) || this.resolveSyncMarker(load.last_changed_for_sync, load.last_synced);
        if (syncMarker) {
            payload.last_synced = syncMarker;
        }

        LOAD_FIELDS.forEach(field => {
            const value = this.normalizeValue(field, load[field], {
                numericFields: LOAD_NUMERIC_FIELDS,
                requiredFields: LOAD_REQUIRED_FIELDS
            });
            if (value !== undefined) {
                payload[field] = value;
            }
        });

        const companyAirtableIdBySupabaseId = options.companyAirtableIdBySupabaseId instanceof Map
            ? options.companyAirtableIdBySupabaseId
            : new Map(Object.entries(options.companyAirtableIdBySupabaseId || {}));

        LOAD_COMPANY_LINK_FIELDS.forEach(field => {
            const supabaseCompanyId = this.normalizeId(load[field]);

            if (!supabaseCompanyId) {
                payload[field] = [];
                return;
            }

            const airtableCompanyId = companyAirtableIdBySupabaseId.get(supabaseCompanyId);
            if (airtableCompanyId) {
                payload[field] = [airtableCompanyId];
            } else {
                console.warn(`[sync] Supabase load ${load.id} references ${field} ${supabaseCompanyId}, which is missing an Airtable record.`);
            }
        });
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

    extractLinkedRecordId(value) {
        if (Array.isArray(value) && value.length > 0) {
            const first = value[0];
            if (typeof first === 'string') {
                return this.normalizeId(first);
            }
            if (first && typeof first === 'object' && typeof first.id === 'string') {
                return this.normalizeId(first.id);
            }
        }

        if (value && typeof value === 'object' && typeof value.id === 'string') {
            return this.normalizeId(value.id);
        }

        return null;
    }


    async ensureSupabaseAirtableId(entity, airtableId, entityType) {
        if (!airtableId || entity.airtable_id === airtableId) {
            return;
        }

        const methodName = SUPABASE_UPDATE_METHOD_BY_ENTITY[entityType];
        if (!methodName || typeof this.supabase[methodName] !== 'function') {
            return;
        }

        const updates = { airtable_id: airtableId };
        const updated = await this.supabase[methodName](entity.id, updates);
        Object.assign(entity, updated);
    }



    initializeStats() {
        return {
            processed: 0,
            created: 0,
            updated: 0,
            unchanged: 0,
            skipped: 0,
            errors: 0
        };
    }

    applySyncResult(stats, result) {
        if (!stats || !result) {
            return;
        }

        const action = result.action || 'unknown';

        if (action === 'skipped') {
            stats.skipped += 1;
            return;
        }

        stats.processed += 1;

        if (action === 'created') {
            stats.created += 1;
        } else if (action === 'updated') {
            stats.updated += 1;
        } else {
            stats.unchanged += 1;
        }
    }

    logSyncSummary(direction, entity, stats) {
        const parts = [
            `processed=${stats.processed}`,
            `created=${stats.created}`,
            `updated=${stats.updated}`,
            `unchanged=${stats.unchanged}`,
            `skipped=${stats.skipped}`,
            `errors=${stats.errors}`
        ];
        console.log(`[sync] ${direction} ${entity}: ${parts.join(', ')}.`);
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

    normalizeSyncValue(value) {
        if (!value) {
            return null;
        }

        if (value instanceof Date) {
            return value.toISOString();
        }

        if (typeof value === 'string') {
            const trimmed = value.trim();
            if (trimmed.length === 0) {
                return null;
            }
            try {
                const date = new Date(trimmed);
                if (!isNaN(date.getTime())) {
                    return date.toISOString();
                }
            } catch (e) {
                return null;
            }
        }

        return null;
    }

    resolveSyncMarker(lastChanged, lastSynced) {
        const lastChangedNorm = this.normalizeSyncValue(lastChanged);
        const lastSyncedNorm = this.normalizeSyncValue(lastSynced);

        if (!lastChangedNorm) {
            return new Date().toISOString();
        }

        if (!lastSyncedNorm) {
            return lastChangedNorm;
        }

        const changedDate = new Date(lastChangedNorm);
        const syncedDate = new Date(lastSyncedNorm);

        return changedDate > syncedDate ? lastChangedNorm : new Date().toISOString();
    }

    shouldSkipSync(lastChanged, lastSynced, toleranceMs = null) {
        const lastChangedNorm = this.normalizeSyncValue(lastChanged);
        const lastSyncedNorm = this.normalizeSyncValue(lastSynced);

        if (!lastChangedNorm || !lastSyncedNorm) {
            return false;
        }

        const changedDate = new Date(lastChangedNorm);
        const syncedDate = new Date(lastSyncedNorm);

        const diffMs = changedDate.getTime() - syncedDate.getTime();
        const tolerance = toleranceMs !== null ? toleranceMs : this.syncToleranceMs;
        return diffMs <= tolerance;
    }

    compareTimestamps(sourceLastChanged, destLastChanged, toleranceMs = 0) {
        const sourceNorm = this.normalizeSyncValue(sourceLastChanged);
        const destNorm = this.normalizeSyncValue(destLastChanged);

        if (!sourceNorm && !destNorm) {
            return 'equal';
        }

        if (!sourceNorm) {
            return 'dest_newer';
        }

        if (!destNorm) {
            return 'source_newer';
        }

        const sourceDate = new Date(sourceNorm);
        const destDate = new Date(destNorm);

        const diffMs = Math.abs(sourceDate.getTime() - destDate.getTime());

        if (diffMs <= toleranceMs) {
            return 'equal';
        }

        if (sourceDate > destDate) {
            return 'source_newer';
        } else if (sourceDate < destDate) {
            return 'dest_newer';
        } else {
            return 'equal';
        }
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



















