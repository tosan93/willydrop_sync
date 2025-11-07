/**
 * Helper entry-point so `AIRTABLE_FIELD_MAP_FILE` can point to a single module.
 * Each environment-specific map lives in its own file:
 *   - ./airtable-field-map.dev.js
 *   - ./airtable-field-map.live.js
 */
let devMapping = {};
let liveMapping = {};

try {
    // eslint-disable-next-line global-require, import/no-unresolved
    devMapping = require('./airtable-field-map.dev');
} catch (error) {
    devMapping = {};
}

try {
    // eslint-disable-next-line global-require, import/no-unresolved
    liveMapping = require('./airtable-field-map.live');
} catch (error) {
    liveMapping = {};
}

module.exports = {
    dev: devMapping,
    live: liveMapping,
    default: devMapping
};
