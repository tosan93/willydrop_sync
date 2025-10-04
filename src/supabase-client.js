const { createClient } = require('@supabase/supabase-js');
const { randomUUID } = require('crypto');
const config = require('./config');

class SupabaseClient {
    constructor() {
        if (!config.supabase.url || !config.supabase.serviceKey) {
            throw new Error('Supabase credentials are missing. Check your environment configuration.');
        }

        this.carsTableName = config.supabase.tableName;
        this.locationsTableName = config.supabase.locationsTableName || 'locations';
        this.companiesTableName = config.supabase.companiesTableName || 'companies';
        this.loadsTableName = config.supabase.loadsTableName || 'loads';
        this.usersTableName = config.supabase.usersTableName || 'users';
        this.client = createClient(config.supabase.url, config.supabase.serviceKey, {
            auth: {
                persistSession: false
            }
        });
    }

    async getAllCars() {
        const { data, error } = await this.client
            .from(this.carsTableName)
            .select('*');

        if (error) throw error;
        return data || [];
    }

    async getCarById(id) {
        const { data, error } = await this.client
            .from(this.carsTableName)
            .select('*')
            .eq('id', id)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findCarByExternalId(externalId) {
        if (!externalId) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.carsTableName)
            .select('*')
            .eq('external_id', externalId)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async createCar(car) {
        const payload = this.cleanPayload({ ...car });
        payload.id = payload.id || randomUUID();

        const { data, error } = await this.client
            .from(this.carsTableName)
            .insert(payload)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async updateCar(id, updates) {
        const payload = this.cleanPayload({ ...updates });

        if (Object.keys(payload).length === 0) {
            return this.getCarById(id);
        }

        const { data, error } = await this.client
            .from(this.carsTableName)
            .update(payload)
            .eq('id', id)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async deleteCar(id) {
        const { error } = await this.client
            .from(this.carsTableName)
            .delete()
            .eq('id', id);

        if (error) throw error;
        return true;
    }

    async getAllLocations() {
        const { data, error } = await this.client
            .from(this.locationsTableName)
            .select('*');

        if (error) throw error;
        return data || [];
    }

    async getLocationById(id) {
        const { data, error } = await this.client
            .from(this.locationsTableName)
            .select('*')
            .eq('id', id)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findLocationByAirtableId(airtableId) {
        if (!airtableId) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.locationsTableName)
            .select('*')
            .eq('airtable_id', airtableId)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async createLocation(location) {
        const payload = this.cleanPayload({ ...location });
        payload.id = payload.id || randomUUID();

        const { data, error } = await this.client
            .from(this.locationsTableName)
            .insert(payload)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async updateLocation(id, updates) {
        const payload = this.cleanPayload({ ...updates });

        if (Object.keys(payload).length === 0) {
            return this.getLocationById(id);
        }

        const { data, error } = await this.client
            .from(this.locationsTableName)
            .update(payload)
            .eq('id', id)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async getAllCompanies() {
        const { data, error } = await this.client
            .from(this.companiesTableName)
            .select('*');

        if (error) throw error;
        return data || [];
    }

    async getCompanyById(id) {
        const { data, error } = await this.client
            .from(this.companiesTableName)
            .select('*')
            .eq('id', id)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findCompanyByAirtableId(airtableId) {
        if (!airtableId) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.companiesTableName)
            .select('*')
            .eq('airtable_id', airtableId)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findCompanyByName(name) {
        if (!name) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.companiesTableName)
            .select('*')
            .eq('name', name.trim())
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async createCompany(company) {
        const payload = this.cleanPayload({ ...company });
        payload.id = payload.id || randomUUID();

        const { data, error } = await this.client
            .from(this.companiesTableName)
            .insert(payload)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async updateCompany(id, updates) {
        const payload = this.cleanPayload({ ...updates });

        if (Object.keys(payload).length === 0) {
            return this.getCompanyById(id);
        }

        const { data, error } = await this.client
            .from(this.companiesTableName)
            .update(payload)
            .eq('id', id)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async getAllLoads() {
        const { data, error } = await this.client
            .from(this.loadsTableName)
            .select('*');

        if (error) throw error;
        return data || [];
    }

    async getLoadById(id) {
        const { data, error } = await this.client
            .from(this.loadsTableName)
            .select('*')
            .eq('id', id)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findLoadByAirtableId(airtableId) {
        if (!airtableId) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.loadsTableName)
            .select('*')
            .eq('airtable_id', airtableId)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findLoadByLoadNumber(loadNumber) {
        if (!loadNumber) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.loadsTableName)
            .select('*')
            .eq('load_number', loadNumber)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async createLoad(load) {
        const payload = this.cleanPayload({ ...load });
        payload.id = payload.id || randomUUID();

        const { data, error } = await this.client
            .from(this.loadsTableName)
            .insert(payload)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async updateLoad(id, updates) {
        const payload = this.cleanPayload({ ...updates });

        if (Object.keys(payload).length === 0) {
            return this.getLoadById(id);
        }

        const { data, error } = await this.client
            .from(this.loadsTableName)
            .update(payload)
            .eq('id', id)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async getAllUsers() {
        const { data, error } = await this.client
            .from(this.usersTableName)
            .select('*');

        if (error) throw error;
        return data || [];
    }

    async getUserById(id) {
        const { data, error } = await this.client
            .from(this.usersTableName)
            .select('*')
            .eq('id', id)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findUserByAirtableId(airtableId) {
        if (!airtableId) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.usersTableName)
            .select('*')
            .eq('airtable_id', airtableId)
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async findUserByEmail(email) {
        if (!email) {
            return null;
        }

        const { data, error } = await this.client
            .from(this.usersTableName)
            .select('*')
            .eq('email', email.trim())
            .maybeSingle();

        if (error) throw error;
        return data || null;
    }

    async createUser(user) {
        const payload = this.cleanPayload({ ...user });
        payload.id = payload.id || randomUUID();

        const { data, error } = await this.client
            .from(this.usersTableName)
            .insert(payload)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async updateUser(id, updates) {
        const payload = this.cleanPayload({ ...updates });

        if (Object.keys(payload).length === 0) {
            return this.getUserById(id);
        }

        const { data, error } = await this.client
            .from(this.usersTableName)
            .update(payload)
            .eq('id', id)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async createSyncRun(tableName, direction, type = 'manual') {
        const payload = {
            table_name: tableName,
            direction: direction,
            type: type,
            started_at: new Date().toISOString(),
            processed: 0,
            updated: 0,
            errors: 0
        };

        const { data, error } = await this.client
            .from('system_sync_runs')
            .insert(payload)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async updateSyncRun(id, updates) {
        const payload = this.cleanPayload({ ...updates });

        if (Object.keys(payload).length === 0) {
            return;
        }

        if (!payload.finished_at && (payload.processed !== undefined || payload.updated !== undefined || payload.errors !== undefined)) {
            payload.finished_at = new Date().toISOString();
        }

        const { error } = await this.client
            .from('system_sync_runs')
            .update(payload)
            .eq('id', id);

        if (error) throw error;
    }

    cleanPayload(payload) {
        return Object.entries(payload).reduce((acc, [key, value]) => {
            if (value !== undefined) {
                acc[key] = value;
            }
            return acc;
        }, {});
    }
}

module.exports = SupabaseClient;
