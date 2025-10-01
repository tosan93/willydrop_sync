const { createClient } = require('@supabase/supabase-js');
const { randomUUID } = require('crypto');
const config = require('./config');

class SupabaseClient {
    constructor() {
        if (!config.supabase.url || !config.supabase.serviceKey) {
            throw new Error('Supabase credentials are missing. Check your environment configuration.');
        }

        this.tableName = config.supabase.tableName;
        this.client = createClient(config.supabase.url, config.supabase.serviceKey, {
            auth: {
                persistSession: false
            }
        });
    }

    async getAllCars() {
        const { data, error } = await this.client
            .from(this.tableName)
            .select('*');

        if (error) throw error;
        return data || [];
    }

    async getCarById(id) {
        const { data, error } = await this.client
            .from(this.tableName)
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
            .from(this.tableName)
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
            .from(this.tableName)
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
            .from(this.tableName)
            .update(payload)
            .eq('id', id)
            .select()
            .single();

        if (error) throw error;
        return data;
    }

    async deleteCar(id) {
        const { error } = await this.client
            .from(this.tableName)
            .delete()
            .eq('id', id);

        if (error) throw error;
        return true;
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
