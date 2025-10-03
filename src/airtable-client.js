const https = require('https');
const Airtable = require('airtable');
const config = require('./config');

const AIRTABLE_API_HOST = 'api.airtable.com';
const IGNORED_WRITE_KEYS = new Set(['airtable_id', 'last_modified', 'raw_fields', 'raw_fields_by_id']);

class AirtableClient {
    constructor(options = {}) {
        const token = options.token ?? config.airtable.token;
        const baseId = options.baseId ?? config.airtable.baseId;
        const hasExplicitTableId = Object.prototype.hasOwnProperty.call(options, 'tableId');
        const tableId = hasExplicitTableId ? options.tableId : (config.airtable.tableId ?? null);
        const hasExplicitTableName = Object.prototype.hasOwnProperty.call(options, 'tableName');
        const tableName = hasExplicitTableName && options.tableName ? options.tableName : config.airtable.tableName;
        const hasExplicitFieldMapping = Object.prototype.hasOwnProperty.call(options, 'fieldMapping');
        const fieldMapping = hasExplicitFieldMapping ? (options.fieldMapping || {}) : (config.airtable.fieldMapping ?? {});

        if (!token || !baseId) {
            throw new Error('Airtable credentials are missing. Check your environment configuration.');
        }

        this.apiKey = token;
        this.baseId = baseId;
        this.tableId = tableId || null;
        this.tableName = tableName;
        this.fieldMapping = fieldMapping;

        this.fieldIdByKey = {};
        this.fieldNameByKey = {};
        this.keysWithMappings = new Set();

        Object.entries(this.fieldMapping).forEach(([key, value]) => {
            if (!value) {
                return;
            }

            if (typeof value === 'string') {
                this.fieldIdByKey[key] = value;
            } else if (typeof value === 'object') {
                if (typeof value.id === 'string') {
                    this.fieldIdByKey[key] = value.id;
                }
                if (typeof value.name === 'string') {
                    this.fieldNameByKey[key] = value.name;
                }
            }

            if (this.fieldIdByKey[key]) {
                this.keysWithMappings.add(key);
            }
        });

        this.usingFieldMap = this.keysWithMappings.size > 0;
        this.candidateNamesCache = new Map();
        this.recordFieldsByIdCache = new Map();

        const tableIdentifier = this.tableId || this.tableName;
        if (!tableIdentifier) {
            throw new Error('Airtable table name or ID must be configured.');
        }

        this.base = new Airtable({ apiKey: this.apiKey }).base(this.baseId);
        this.table = this.base(tableIdentifier);
    }
    getCandidateNames(key) {
        if (this.candidateNamesCache.has(key)) {
            return this.candidateNamesCache.get(key);
        }

        const configuredName = this.fieldNameByKey[key];
        const candidates = [configuredName, key]
            .filter(name => typeof name === 'string' && name.length > 0);

        const uniqueCandidates = [...new Set(candidates)];
        this.candidateNamesCache.set(key, uniqueCandidates);
        return uniqueCandidates;
    }

    async getAllRecords() {
        const recordsPromise = this.table.select().all();

        if (!this.usingFieldMap) {
            const records = await recordsPromise;
            return Promise.all(records.map(record => this.normalizeRecord(record)));
        }

        const [records, recordsById] = await Promise.all([
            recordsPromise,
            this.table.select({ returnFieldsByFieldId: true }).all()
        ]);

        const fieldsByIdLookup = new Map(recordsById.map(record => [record.id, record.fields]));
        return Promise.all(records.map(record => this.normalizeRecord(record, fieldsByIdLookup)));
    }

    async createRecord(data) {
        const { preferred, fallback } = this.prepareWritableFields(data);

        try {
            const record = await this.table.create(preferred);
            return this.normalizeRecord(record);
        } catch (error) {
            if (this.shouldRetryWithIds(error) && Object.keys(fallback).length > 0) {
                const record = await this.table.create(fallback);
                return this.normalizeRecord(record);
            }
            throw error;
        }
    }

    async updateRecord(recordId, data) {
        const { preferred, fallback } = this.prepareWritableFields(data);
        const hasPreferred = Object.keys(preferred).length > 0;
        const hasFallback = Object.keys(fallback).length > 0;

        if (!hasPreferred && !hasFallback) {
            const record = await this.table.find(recordId);
            return this.normalizeRecord(record);
        }

        if (!hasPreferred && hasFallback) {
            const record = await this.table.update(recordId, fallback);
            return this.normalizeRecord(record);
        }

        try {
            const record = await this.table.update(recordId, preferred);
            return this.normalizeRecord(record);
        } catch (error) {
            if (this.shouldRetryWithIds(error) && hasFallback) {
                const record = await this.table.update(recordId, fallback);
                return this.normalizeRecord(record);
            }
            throw error;
        }
    }

    async deleteRecord(recordId) {
        await this.table.destroy(recordId);
        return true;
    }

    async normalizeRecord(record, fieldsByIdLookup = null) {
        const baseData = {
            airtable_id: record.id,
            last_modified: record._rawJson?.createdTime,
            raw_fields: { ...record.fields }
        };

        if (!this.usingFieldMap) {
            return {
                ...baseData,
                ...record.fields
            };
        }

        const normalized = {
            ...baseData,
            ...record.fields
        };

        const fieldsNeedingFallback = [];

        this.keysWithMappings.forEach(key => {
            const candidateNames = this.getCandidateNames(key);
            let value;

            for (const name of candidateNames) {
                if (Object.prototype.hasOwnProperty.call(record.fields, name)) {
                    value = record.fields[name];
                    break;
                }
            }

            if (value !== undefined) {
                normalized[key] = value;
                return;
            }

            const fieldId = this.fieldIdByKey[key];
            if (fieldId) {
                fieldsNeedingFallback.push({ key, fieldId });
            }
        });

        if (fieldsNeedingFallback.length === 0) {
            return normalized;
        }

        let fieldsById = null;

        if (fieldsNeedingFallback.length > 0) {
            if (fieldsByIdLookup && fieldsByIdLookup.has(record.id)) {
                fieldsById = fieldsByIdLookup.get(record.id);
            } else {
                fieldsById = await this.fetchRecordFieldsById(record.id);
            }
        }

        if (fieldsById) {
            fieldsNeedingFallback.forEach(({ key, fieldId }) => {
                if (Object.prototype.hasOwnProperty.call(fieldsById, fieldId)) {
                    normalized[key] = fieldsById[fieldId];
                }
            });

            if (Object.keys(fieldsById).length > 0) {
                normalized.raw_fields_by_id = fieldsById;
            }
        }

        return normalized;
    }

    prepareWritableFields(data) {
        const preferred = {};
        const fallback = {};

        Object.entries(data || {}).forEach(([key, value]) => {
            if (IGNORED_WRITE_KEYS.has(key) || value === undefined) {
                return;
            }

            const candidateNames = this.getCandidateNames(key);
            const fieldId = this.fieldIdByKey[key];

            let assigned = false;
            for (const name of candidateNames) {
                if (!Object.prototype.hasOwnProperty.call(preferred, name)) {
                    preferred[name] = value;
                    assigned = true;
                    break;
                }
            }

            if (!assigned) {
                if (!this.usingFieldMap) {
                    preferred[key] = value;
                    assigned = true;
                } else if (fieldId) {
                    fallback[fieldId] = value;
                } else {
                    preferred[key] = value;
                    assigned = true;
                }
            }

            if (this.usingFieldMap && fieldId) {
                fallback[fieldId] = value;
            }
        });

        return { preferred, fallback };
    }

    shouldRetryWithIds(error) {
        if (!this.usingFieldMap) {
            return false;
        }

        if (!error || typeof error !== 'object') {
            return false;
        }

        const message = error.message || '';
        return error.statusCode === 422 && (error.error === 'UNKNOWN_FIELD_NAME' || /Unknown field name/.test(message));
    }

    async fetchRecordFieldsById(recordId) {
        if (!this.usingFieldMap) {
            return null;
        }

        if (this.recordFieldsByIdCache.has(recordId)) {
            return this.recordFieldsByIdCache.get(recordId);
        }

        const tableSegment = this.tableId ? this.tableId : encodeURIComponent(this.tableName);
        const encodedRecordId = encodeURIComponent(recordId);
        const path = `/v0/${this.baseId}/${tableSegment}/${encodedRecordId}?returnFieldsByFieldId=true`;

        try {
            const payload = await this.performRequest('GET', path);
            const fields = payload && payload.fields ? payload.fields : {};
            this.recordFieldsByIdCache.set(recordId, fields);
            return fields;
        } catch (error) {
            return null;
        }
    }

    performRequest(method, path, body) {
        return new Promise((resolve, reject) => {
            const options = {
                method,
                hostname: AIRTABLE_API_HOST,
                path,
                headers: {
                    Authorization: `Bearer ${this.apiKey}`,
                    'Content-Type': 'application/json'
                }
            };

            const req = https.request(options, res => {
                let responseBody = '';
                res.setEncoding('utf8');

                res.on('data', chunk => {
                    responseBody += chunk;
                });

                res.on('end', () => {
                    if (res.statusCode >= 200 && res.statusCode < 300) {
                        if (!responseBody) {
                            resolve({});
                            return;
                        }

                        try {
                            resolve(JSON.parse(responseBody));
                        } catch (error) {
                            reject(error);
                        }
                        return;
                    }

                    const error = new Error(responseBody || `Request failed with status ${res.statusCode}`);
                    error.statusCode = res.statusCode;
                    reject(error);
                });
            });

            req.on('error', reject);

            if (body) {
                req.write(JSON.stringify(body));
            }

            req.end();
        });
    }
}

module.exports = AirtableClient;

