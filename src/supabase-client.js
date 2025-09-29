const { createClient } = require('@supabase/supabase-js');
const config = require('./config');

class SupabaseClient {
    constructor() {
        this.client = createClient(
            config.supabase.url,
            config.supabase.serviceKey
        );
    }

    async getAllTransports() {
        try {
            const { data, error } = await this.client
                .from('transports_testdb')
                .select('*');
            
            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error fetching Supabase transports:', error);
            throw error;
        }
    }

    async createTransport(transport) {
        try {
            const { data, error } = await this.client
                .from('transports_testdb')
                .insert(transport)
                .select()
                .single();
            
            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error creating Supabase transport:', error);
            throw error;
        }
    }

    async updateTransport(id, updates) {
        try {
            const { data, error } = await this.client
                .from('transports_testdb')
                .update(updates)
                .eq('id', id)
                .select()
                .single();
            
            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error updating Supabase transport:', error);
            throw error;
        }
    }

    async deleteTransport(id) {
        try {
            const { error } = await this.client
                .from('transports_testdb')
                .delete()
                .eq('id', id);
            
            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error deleting Supabase transport:', error);
            throw error;
        }
    }

    async findByAirtableId(airtableId) {
        try {
            const { data, error } = await this.client
                .from('transports_testdb')
                .select('*')
                .eq('airtable_id', airtableId)
                .single();
            
            if (error && error.code !== 'PGRST116') throw error; // PGRST116 = not found
            return data;
        } catch (error) {
            console.error('Error finding by Airtable ID:', error);
            return null;
        }
    }
}

module.exports = SupabaseClient;