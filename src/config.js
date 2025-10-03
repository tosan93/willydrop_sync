const fs = require('fs');
const path = require('path');
require('dotenv').config();

function isObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value);
}

function normalizeMapping(raw) {
    if (!isObject(raw)) {
        return {};
    }

    return Object.entries(raw).reduce((acc, [key, value]) => {
        if (!key) {
            return acc;
        }

        if (typeof value === 'string') {
            acc[key] = { id: value.trim(), name: null };
            return acc;
        }

        if (isObject(value)) {
            const id = typeof value.id === 'string'
                ? value.id.trim()
                : (typeof value.fieldId === 'string' ? value.fieldId.trim() : null);

            if (!id) {
                return acc;
            }

            const name = typeof value.name === 'string'
                ? value.name.trim()
                : (typeof value.fieldName === 'string' ? value.fieldName.trim() : null);

            acc[key] = { id, name: name || null };
        }

        return acc;
    }, {});
}

function normalizeMappingCollection(raw) {
    if (!isObject(raw)) {
        return {};
    }

    const entries = Object.entries(raw);
    const hasTableNamespaces = entries.some(([key, value]) =>
        isObject(value) && Object.keys(value).some(innerKey => typeof value[innerKey] === 'string' || isObject(value[innerKey]))
    );

    if (raw.cars || raw.locations || hasTableNamespaces) {
        return entries.reduce((acc, [tableKey, value]) => {
            acc[tableKey] = normalizeMapping(value);
            return acc;
        }, {});
    }

    return {
        cars: normalizeMapping(raw)
    };
}

function parseAirtableFieldMapping(raw) {
    if (!raw) {
        return {};
    }

    return raw
        .split(',')
        .map(entry => entry.trim())
        .filter(Boolean)
        .reduce((acc, entry) => {
            const match = entry.match(/^([^=:]+)\s*[:=]\s*(.+)$/);
            if (!match) {
                return acc;
            }

            const key = match[1].trim();
            const value = match[2].trim();

            if (!key || !value) {
                return acc;
            }

            const parts = value.split('|').map(part => part.trim());
            const idPart = parts[0];
            const namePart = parts[1] || null;

            if (!idPart) {
                return acc;
            }

            acc[key] = { id: idPart, name: namePart };
            return acc;
        }, {});
}

function resolveCandidatePaths() {
    const fileFromEnv = process.env.AIRTABLE_FIELD_MAP_FILE;
    const candidates = [];

    if (fileFromEnv) {
        const absolute = path.isAbsolute(fileFromEnv)
            ? fileFromEnv
            : path.resolve(process.cwd(), fileFromEnv);
        candidates.push(absolute);
    }

    candidates.push(path.join(__dirname, 'airtable-field-map.js'));
    candidates.push(path.join(__dirname, 'airtable-field-map.json'));

    return candidates;
}

function loadMappingFromFile() {
    const candidates = resolveCandidatePaths();

    for (const filePath of candidates) {
        if (!fs.existsSync(filePath)) {
            continue;
        }

        try {
            const resolvedPath = require.resolve(filePath);
            delete require.cache[resolvedPath];
            return require(resolvedPath);
        } catch (error) {
            if (error.code === 'MODULE_NOT_FOUND') {
                continue;
            }

            throw error;
        }
    }

    return {};
}

function loadAirtableFieldMappings() {
    const fileMappings = normalizeMappingCollection(loadMappingFromFile());
    const envMapping = parseAirtableFieldMapping(process.env.AIRTABLE_FIELD_MAP);

    return {
        cars: {
            ...(fileMappings.cars || {}),
            ...envMapping
        },
        locations: fileMappings.locations || {}
    };
}

const airtableMappings = loadAirtableFieldMappings();

module.exports = {
    supabase: {
        url: process.env.SUPABASE_URL,
        serviceKey: process.env.SUPABASE_SERVICE_KEY,
        tableName: process.env.SUPABASE_CARS_TABLE || 'cars',
        locationsTableName: process.env.SUPABASE_LOCATIONS_TABLE || 'locations'
    },
    airtable: {
        token: process.env.AIRTABLE_TOKEN,
        baseId: process.env.AIRTABLE_BASE_ID,
        tableId: process.env.AIRTABLE_TABLE_ID,
        tableName: process.env.AIRTABLE_TABLE_NAME || 'Cars',
        fieldMapping: airtableMappings.cars,
        locations: {
            tableId: process.env.AIRTABLE_LOCATIONS_TABLE_ID,
            tableName: process.env.AIRTABLE_LOCATIONS_TABLE_NAME || 'locations',
            fieldMapping: airtableMappings.locations
        }
    },
    sync: {
        intervalMinutes: parseInt(process.env.SYNC_INTERVAL_MINUTES, 10) || 2
    }
};
