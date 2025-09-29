const Airtable = require('airtable');
const config = require('./config');

class AirtableClient {
    constructor() {
        this.base = new Airtable({ apiKey: config.airtable.token })
            .base(config.airtable.baseId);
        this.table = this.base(config.airtable.tableName);
    }

    async getAllRecords() {
        try {
            const records = await this.table.select().all();
            
            // DEBUG: Log the raw data
            console.log('=== RAW AIRTABLE DATA ===');
            records.forEach((record, i) => {
                console.log(`Record ${i}:`, {
                    id: record.id,
                    fields: record.fields,
                    fieldNames: Object.keys(record.fields)
                });
            });
            console.log('========================');
            
            return records.map(record => ({
                airtable_id: record.id,
                ...record.fields,
                last_modified: record._rawJson.createdTime
            }));
        } catch (error) {
            console.error('Error fetching Airtable records:', error);
            throw error;
        }
    }

    async createRecord(data) {
        try {
            const record = await this.table.create(data);
            return {
                airtable_id: record.id,
                ...record.fields
            };
        } catch (error) {
            console.error('Error creating Airtable record:', error);
            throw error;
        }
    }

    async updateRecord(recordId, data) {
        try {
            const record = await this.table.update(recordId, data);
            return {
                airtable_id: record.id,
                ...record.fields
            };
        } catch (error) {
            console.error('Error updating Airtable record:', error);
            throw error;
        }
    }

    async deleteRecord(recordId) {
        try {
            await this.table.destroy(recordId);
            return true;
        } catch (error) {
            console.error('Error deleting Airtable record:', error);
            throw error;
        }
    }
}

module.exports = AirtableClient;