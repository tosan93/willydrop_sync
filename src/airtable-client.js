const Airtable = require('airtable');
const config = require('./config');

class AirtableClient {
    constructor() {
        if (!config.airtable.token || !config.airtable.baseId) {
            throw new Error('Airtable credentials are missing. Check your environment configuration.');
        }

        this.base = new Airtable({ apiKey: config.airtable.token }).base(config.airtable.baseId);
        this.table = this.base(config.airtable.tableName);
    }

    async getAllRecords() {
        const records = await this.table.select().all();

        return records.map(record => ({
            airtable_id: record.id,
            ...record.fields,
            last_modified: record._rawJson?.createdTime
        }));
    }

    async createRecord(data) {
        const record = await this.table.create(data);
        return {
            airtable_id: record.id,
            ...record.fields
        };
    }

    async updateRecord(recordId, data) {
        const record = await this.table.update(recordId, data);
        return {
            airtable_id: record.id,
            ...record.fields
        };
    }

    async deleteRecord(recordId) {
        await this.table.destroy(recordId);
        return true;
    }
}

module.exports = AirtableClient;
