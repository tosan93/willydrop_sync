const fs = require('fs');
const path = require('path');
require('dotenv').config();

const DEFAULT_ENVIRONMENT = 'dev';
const ACTIVE_ENVIRONMENT = (process.env.SYNC_ENV || process.env.NODE_ENV || DEFAULT_ENVIRONMENT)
    .toString()
    .trim()
    .toLowerCase();
const ACTIVE_ENVIRONMENT_UPPER = ACTIVE_ENVIRONMENT.toUpperCase();

function resolveEnvVar(baseName, options = {}) {
    if (!baseName) {
        return options.fallback;
    }

    const {
        altNames = [],
        fallback = undefined,
        includeBase = true,
        trim = true
    } = options;

    const candidates = [];

    const appendCandidate = value => {
        if (typeof value === 'string' && value.length > 0 && !candidates.includes(value)) {
            candidates.push(value);
        }
    };

    appendCandidate(`${baseName}_${ACTIVE_ENVIRONMENT_UPPER}`);
    appendCandidate(`${baseName}_${ACTIVE_ENVIRONMENT}`);
    appendCandidate(`${ACTIVE_ENVIRONMENT_UPPER}_${baseName}`);
    appendCandidate(`${ACTIVE_ENVIRONMENT}_${baseName}`);

    altNames.forEach(entry => {
        if (typeof entry === 'function') {
            appendCandidate(entry(ACTIVE_ENVIRONMENT_UPPER, ACTIVE_ENVIRONMENT));
            return;
        }
        if (typeof entry === 'string') {
            appendCandidate(
                entry
                    .replace(/\{ENV\}/g, ACTIVE_ENVIRONMENT_UPPER)
                    .replace(/\{env\}/g, ACTIVE_ENVIRONMENT)
            );
        }
    });

    if (includeBase) {
        appendCandidate(baseName);
    }

    for (const candidate of candidates) {
        if (candidate && Object.prototype.hasOwnProperty.call(process.env, candidate)) {
            const value = process.env[candidate];
            if (value === undefined || value === null) {
                continue;
            }
            const stringValue = String(value);
            if (!trim || stringValue.trim().length > 0) {
                return trim ? stringValue.trim() : stringValue;
            }
        }
    }

    return fallback;
}

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

function normalizeMappingCollection(raw, env) {
    if (!isObject(raw)) {
        return {};
    }

    let scopedRaw = raw;
    if (env && typeof raw[env] === 'object' && raw[env] !== null) {
        scopedRaw = raw[env];
    } else if (typeof raw.default === 'object' && raw.default !== null && !raw.cars) {
        scopedRaw = raw.default;
    }

    if (!isObject(scopedRaw)) {
        return {};
    }

    const entries = Object.entries(scopedRaw);
    const hasTableNamespaces = entries.some(([key, value]) =>
        isObject(value) && Object.keys(value).some(innerKey => typeof value[innerKey] === 'string' || isObject(value[innerKey]))
    );

    if (scopedRaw.cars || scopedRaw.locations || hasTableNamespaces) {
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

function resolveCandidatePaths(env) {
    const envUpper = env.toUpperCase();
    const envLower = env.toLowerCase();

    const fileFromEnv = resolveEnvVar('AIRTABLE_FIELD_MAP_FILE', {
        altNames: [
            'AIRTABLE_FIELD_MAP_FILE_{ENV}',
            '{ENV}_AIRTABLE_FIELD_MAP_FILE'
        ]
    });

    const candidates = [];

    if (fileFromEnv) {
        const absolute = path.isAbsolute(fileFromEnv)
            ? fileFromEnv
            : path.resolve(process.cwd(), fileFromEnv);
        candidates.push(absolute);
    }

    [
        path.join(__dirname, `airtable-field-map.${envLower}.js`),
        path.join(__dirname, `airtable-field-map.${envLower}.json`),
        path.join(__dirname, `airtable-field-map.${envUpper}.js`),
        path.join(__dirname, `airtable-field-map.${envUpper}.json`),
        path.join(__dirname, 'airtable-field-map.js'),
        path.join(__dirname, 'airtable-field-map.json')
    ].forEach(candidate => {
        if (!candidates.includes(candidate)) {
            candidates.push(candidate);
        }
    });

    return candidates;
}

function loadMappingFromFile(env) {
    const candidates = resolveCandidatePaths(env);

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

function loadAirtableFieldMappings(env) {
    const fileMappings = normalizeMappingCollection(loadMappingFromFile(env), env);
    const inlineMapping = resolveEnvVar('AIRTABLE_FIELD_MAP', {
        altNames: ['AIRTABLE_FIELD_MAP_{ENV}'],
        fallback: ''
    });
    const envMapping = parseAirtableFieldMapping(inlineMapping);

    return {
        cars: {
            ...(fileMappings.cars || {}),
            ...envMapping
        },
        locations: fileMappings.locations || {},
        companies: fileMappings.companies || {},
        loads: fileMappings.loads || {},
        users: fileMappings.users || {},
        bookings: fileMappings.bookings || {},
        requests: fileMappings.requests || {}
    };
}

const airtableMappings = loadAirtableFieldMappings(ACTIVE_ENVIRONMENT);

module.exports = {
    environment: ACTIVE_ENVIRONMENT,
    supabase: {
        url: resolveEnvVar('SUPABASE_URL', {
            altNames: ['SUPABASE_{ENV}_URL', '{ENV}_SUPABASE_URL']
        }),
        serviceKey: resolveEnvVar('SUPABASE_SERVICE_KEY', {
            altNames: ['SUPABASE_{ENV}_SERVICE_KEY', '{ENV}_SUPABASE_SERVICE_KEY']
        }),
        tableName: resolveEnvVar('SUPABASE_CARS_TABLE', { fallback: 'cars' }),
        locationsTableName: resolveEnvVar('SUPABASE_LOCATIONS_TABLE', { fallback: 'locations' }),
        companiesTableName: resolveEnvVar('SUPABASE_COMPANIES_TABLE', { fallback: 'companies' }),
        loadsTableName: resolveEnvVar('SUPABASE_LOADS_TABLE', { fallback: 'loads' }),
        loadCarsTableName: resolveEnvVar('SUPABASE_LOAD_CARS_TABLE', { fallback: 'load_cars' }),
        usersTableName: resolveEnvVar('SUPABASE_USERS_TABLE', { fallback: 'users' }),
        bookingsTableName: resolveEnvVar('SUPABASE_BOOKINGS_TABLE', { fallback: 'bookings' }),
        requestsTableName: resolveEnvVar('SUPABASE_REQUESTS_TABLE', { fallback: 'requests' })
    },
    airtable: {
        token: resolveEnvVar('AIRTABLE_TOKEN', {
            altNames: ['AIRTABLE_{ENV}_TOKEN', '{ENV}_AIRTABLE_TOKEN']
        }),
        baseId: resolveEnvVar('AIRTABLE_BASE_ID', {
            altNames: ['AIRTABLE_{ENV}_BASE_ID', '{ENV}_AIRTABLE_BASE_ID']
        }),
        tableId: resolveEnvVar('AIRTABLE_TABLE_ID', {
            altNames: ['AIRTABLE_{ENV}_TABLE_ID', '{ENV}_AIRTABLE_TABLE_ID']
        }),
        tableName: resolveEnvVar('AIRTABLE_TABLE_NAME', {
            altNames: ['AIRTABLE_{ENV}_TABLE_NAME', '{ENV}_AIRTABLE_TABLE_NAME'],
            fallback: 'cars'
        }),
        fieldMapping: airtableMappings.cars,
        locations: {
            tableId: resolveEnvVar('AIRTABLE_LOCATIONS_TABLE_ID', {
                altNames: ['AIRTABLE_{ENV}_LOCATIONS_TABLE_ID', '{ENV}_AIRTABLE_LOCATIONS_TABLE_ID']
            }),
            tableName: resolveEnvVar('AIRTABLE_LOCATIONS_TABLE_NAME', {
                altNames: ['AIRTABLE_{ENV}_LOCATIONS_TABLE_NAME', '{ENV}_AIRTABLE_LOCATIONS_TABLE_NAME'],
                fallback: 'locations'
            }),
            fieldMapping: airtableMappings.locations
        },
        companies: {
            tableId: resolveEnvVar('AIRTABLE_COMPANIES_TABLE_ID', {
                altNames: ['AIRTABLE_{ENV}_COMPANIES_TABLE_ID', '{ENV}_AIRTABLE_COMPANIES_TABLE_ID']
            }),
            tableName: resolveEnvVar('AIRTABLE_COMPANIES_TABLE_NAME', {
                altNames: ['AIRTABLE_{ENV}_COMPANIES_TABLE_NAME', '{ENV}_AIRTABLE_COMPANIES_TABLE_NAME'],
                fallback: 'companies'
            }),
            fieldMapping: airtableMappings.companies
        },
        loads: {
            tableId: resolveEnvVar('AIRTABLE_LOADS_TABLE_ID', {
                altNames: ['AIRTABLE_{ENV}_LOADS_TABLE_ID', '{ENV}_AIRTABLE_LOADS_TABLE_ID']
            }),
            tableName: resolveEnvVar('AIRTABLE_LOADS_TABLE_NAME', {
                altNames: ['AIRTABLE_{ENV}_LOADS_TABLE_NAME', '{ENV}_AIRTABLE_LOADS_TABLE_NAME'],
                fallback: 'Loads'
            }),
            fieldMapping: airtableMappings.loads
        },
        users: {
            tableId: resolveEnvVar('AIRTABLE_USERS_TABLE_ID', {
                altNames: ['AIRTABLE_{ENV}_USERS_TABLE_ID', '{ENV}_AIRTABLE_USERS_TABLE_ID']
            }),
            tableName: resolveEnvVar('AIRTABLE_USERS_TABLE_NAME', {
                altNames: ['AIRTABLE_{ENV}_USERS_TABLE_NAME', '{ENV}_AIRTABLE_USERS_TABLE_NAME'],
                fallback: 'users'
            }),
            fieldMapping: airtableMappings.users
        },
        bookings: {
            tableId: resolveEnvVar('AIRTABLE_BOOKINGS_TABLE_ID', {
                altNames: ['AIRTABLE_{ENV}_BOOKINGS_TABLE_ID', '{ENV}_AIRTABLE_BOOKINGS_TABLE_ID']
            }),
            tableName: resolveEnvVar('AIRTABLE_BOOKINGS_TABLE_NAME', {
                altNames: ['AIRTABLE_{ENV}_BOOKINGS_TABLE_NAME', '{ENV}_AIRTABLE_BOOKINGS_TABLE_NAME'],
                fallback: 'bookings'
            }),
            fieldMapping: airtableMappings.bookings || {}
        },
        requests: {
            tableId: resolveEnvVar('AIRTABLE_REQUESTS_TABLE_ID', {
                altNames: ['AIRTABLE_{ENV}_REQUESTS_TABLE_ID', '{ENV}_AIRTABLE_REQUESTS_TABLE_ID']
            }),
            tableName: resolveEnvVar('AIRTABLE_REQUESTS_TABLE_NAME', {
                altNames: ['AIRTABLE_{ENV}_REQUESTS_TABLE_NAME', '{ENV}_AIRTABLE_REQUESTS_TABLE_NAME'],
                fallback: 'requests'
            }),
            fieldMapping: airtableMappings.requests || {}
        }
    },
    sync: {
        intervalMinutes: parseInt(process.env.SYNC_INTERVAL_MINUTES, 10) || 2,
        syncToleranceMs: parseInt(process.env.SYNC_TOLERANCE_MS, 10) || 5000,
        airtableSyncToleranceMs: parseInt(process.env.AIRTABLE_SYNC_TOLERANCE_MS, 10) || 5000
    }
};
