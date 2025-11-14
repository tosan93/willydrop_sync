/**
 * Airtable field mapping for the LIVE base.
 * Populate the `id` fields with the production Airtable field IDs before enabling the live sync.
 * Structure matches the grouped layout used in the DEV map.
 */
module.exports = {
    cars: {
        // value fields
        external_id: { id: 'fldSBdvplVAPVaEXv', name: 'external_id' },
        make: { id: 'fldSCCRUieVQNgch5', name: 'make' },
        model: { id: 'flddTQ0jy9EdkpR08', name: 'model' },
        vin: { id: 'fldlwiuHKqgr4WA38', name: 'vin' },
        license_plate: { id: 'fldIYciYMrZrT1CZN', name: 'license_plate' },
        status: { id: 'fld6j5JWmgsHBdVn2', name: 'car_status' },
        earliest_availability_date: { id: 'fldyRZYegFljx3PzY', name: 'earliest_availability_date' },
        pick_up_date: { id: 'fldggJ6mT1gdD1SLH', name: 'pickup_date' },
        special_instructions: { id: 'fldWiKzZYtY9SDXKD', name: 'special_instructions' },
        carrier_rate: { id: 'flda4sOWXWYjrO0sF', name: 'carrier_rate' },
        customer_rate: { id: 'fldCxcAmUO2TohuCI', name: 'customer_rate' },
        delivery_date_actual: { id: 'fldA35MI0qgyesQ4I', name: 'delivery_date_actual' },
        delivery_date_customer_view: { id: 'fld4IQjiyaXb9TMbN', name: 'delivery_date_customer_view' },
        delivery_date_quoted: { id: 'fldc6fKxpuW0BlHgt', name: 'delivery_date_quoted' },
        distance: { id: 'fldpTS0vXLuZz9zt3', name: 'distance' },
        car_specific_comments: { id: 'fldWiKzZYtY9SDXKD', name: 'special_instructions' },
        DD: { id: 'fldmWGNFNEgo7zHla', name: 'DD' },
        availability_request_status: { id: 'fldviI6Ai8AaQCDii', name: 'availability_request_status' },
        preferred_delivery_date: { id: 'fldim8EVpDQlICAWK', name: 'preferred_delivery_date' },
        priority: { id: 'fldQvM8mQclJZh7Y4', name: 'urgency_level' },

        // fkey relations
        customer_id: { id: 'fldqim04HZTesttdf', name: 'customer_id' },
        pickup_location_id: { id: 'fldXjGb6nUvNWjtnJ', name: 'pickup_location_id' },
        delivery_location_id: { id: 'fldM1VHDwN9NUHVSp', name: 'delivery_location_id' },
        request_id: { id: 'fldv1JN7eAanSuNW7', name: 'requests' },

        // special fields
        supabase_id: { id: 'fldr8FMyLq4M6z1hY', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' },
        airtable_id_name_label: { id: 'fld5qsgyr0nDZq73s', name: 'id_cars' }
    },
    locations: {
        // value fields
        address_line1: { id: 'fldVl8bczklyUJzka', name: 'address_line1' },
        address_line2: { id: 'fld6xwtNl2PAcZsBz', name: 'address_line2' },
        city: { id: 'fldErPPujdGM1kXTx', name: 'city' },
        postal_code: { id: 'fldFVrjVWrRfSEoIg', name: 'postal_code' },
        country_code: { id: 'fldtO1KwUkl2lNVo0', name: 'country_code' },
        latitude: { id: 'fldP8XWDIERm9kc70', name: 'latitude' },
        longitude: { id: 'fldMfEYeGHai0FdYo', name: 'longitude' },
        created_at: { id: 'fldhUrfF42ngtswCX', name: 'created_at' },
        opening_hours: { id: 'fldYgrdy95cZBiloI', name: 'opening_hours' },
        category: { id: 'flduutYrnKlFrGopI', name: 'google_place_category' },
        advance_notice: { id: 'fldwUhnAv4jmmmZ63', name: 'advanced_notice_period' },
        phone: { id: 'fld1V3jF9id2RY6AK', name: 'phone_number' },
        email: { id: 'fldRjwIfdmZBkuczF', name: 'email' },
        contact_person: { id: 'fldlP1BcUQSLwX1B6', name: 'contact_person' },
        airtable_category: { id: 'fldgraVSSlYYBDUc5', name: 'google_place_category' },
        google_places_id: { id: 'fldMh3ZBDV2Go2mGo', name: 'google_places_id' },
        website: { id: 'fldZWOtyfZHH5eLM8', name: 'website' },

        // relations
        companies: { id: 'fld1knCOHHpR8uQsK', name: 'companies' },

        // special fields
        supabase_id: { id: 'fld95BhzFLnRYNdrw', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' },
        airtable_id_name_label: { id: 'fldJ3QXZIWgilPaoT', name: 'id_locations' }
    },
    companies: {
        // value fields
        name: { id: 'fldo5mmDp47EcvmnY', name: 'company_name' },
        type: { id: 'fldpme2DZWHG83boo', name: 'type' },
        contact_person: { id: 'fldlBCZ7evxjlwJpI', name: 'contact_person' },
        phone: { id: 'fldhahVyfDegCs5u2', name: 'phone' },
        email: { id: 'fld3kv751mKVyJwOE', name: 'email' },
        vat_number: { id: 'fldMYKBzZ36e9tdI3', name: 'vat_number' },
        regional_coverage: { id: 'fldvDjTUApZnEMfEL', name: 'regional_coverage' },
        legal_documents: { id: 'fldm38BbtKD4TErKB', name: 'legal_documents' },

        // special fields
        supabase_id: { id: 'fldpPwikw6RtK0IEb', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' },
        airtable_id_name_label: { id: 'fldGEgZo8LkRP6iTW', name: 'id_companies' }
    },
    loads: {
        // value fields
        load_number: { id: 'fldNWC8NDSvzPwl0l', name: 'id_loads' },
        total_distance_km: { id: 'fldgcheD5SivspjrQ', name: 'total_distance_km' },
        estimated_duration_hours: { id: 'fldJX5ApUUN5yziJP', name: 'estimated_duration_hours' },
        load_status: { id: 'fldELOEMnb5OJVAQw', name: 'load_status' },
        transport_rate: { id: 'fldsGE6jSaa2e1i0o', name: 'proposed_rate' },
        created_at: { id: 'fld4UhzTxSeEEYXuF', name: 'created_at' },
        updated_at: { id: 'fldBz87R8XCxa4FXr', name: 'updated_at' },
        carrier_quote: { id: 'fldsGE6jSaa2e1i0o', name: 'proposed_rate' },
        car_specific_comments: { id: null, name: 'car_specific_comments' },

        // linked fields
        carrier_id: { id: 'fldT1WDvvLudL1ByU', name: 'carrier_id' },
        load_cars: { id: 'fldo9MhACxZtcF97f', name: 'load_cars' }, // Linked to Cars; SB -> AT only via load_cars rows with is_assigned = true

        // special fields
        supabase_id: { id: 'fldFgtqw2TXJPyAHr', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' },
        airtable_id_name_label: { id: 'fldNWC8NDSvzPwl0l', name: 'id_loads' },
        driver_info: { id: 'fldRthEuVPwQs5hkz', name: 'driver_info' },
        truck_trailer_info: { id: 'fldSjrNn6SlKdOQzR', name: 'truck_trailer_info' }
    },
    users: {
        // value fields
        email: { id: 'fld8hEGIgneOTaCTr', name: 'email' },
        is_active: { id: 'fldVWJL8i7SMxJXjH', name: 'is_active' },
        created_at: { id: 'fldD1rXkJcJnMhRdt', name: 'created_at' },
        // role: { id: 'fldkucElMyJSyRVcS', name: 'role' },

        // linked fields
        company_id: { id: 'fldMtLqzTum9S9mav', name: 'company_id' },

        // special fields
        supabase_id: { id: 'fldRZxLBxOyBV3loe', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' },
        airtable_id_name_label: { id: 'fldgrNIjSzpJzonwt', name: 'id_users' }
    },
    bookings: {
        // value fields
        quoted_price: { id: 'fldErCX3FxkjJtt0x', name: 'quoted_price' },
        final_price: { id: 'fldQnaHGa2OzoSVIC', name: 'final_price' },
        margin_percentage: { id: 'fldXyjgsfQRNNKcRn', name: 'margin_percentage' },
        status: { id: 'fldMjlz2HnXFLfUhr', name: 'bookings_status' },
        quoted_at: { id: 'fld7eXuExUOknfZzj', name: 'quoted_at' },
        confirmed_at: { id: 'fld80snnxyfx0MEr1', name: 'confirmed_at' },
        truck_trailer_info: { id: 'fldmsTIeF5XDzy3uh', name: 'truck_trailer_info' },
        driver_info: { id: 'fldgP9fpWl8P9NKK3', name: 'driver_info' },
        availability_request_status: { id: 'fld8DzEPoYdTbgYWh', name: 'availability_request_status' },

        // linked fields
        load_id: { id: 'fldu0HPDS8X8V5n2D', name: 'load_id' },
        carrier_id: { id: 'fldDxyJqPstbwN8ap', name: 'carrier_id' },

        // special fields
        supabase_id: { id: 'fld3rhFVYMEC72ZaG', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' },
        airtable_id_name_label: { id: 'fld0BuCtWfnfhzg6E', name: 'id_bookings' }
    },
    requests: {
        // linked fields - airtable link special treatment
        customer_id: { id: 'fldrEE1UQjpHyU158', name: 'customer_id' }, // Linked to Companies
        // special fields
        supabase_id: { id: 'flddTbOVnrSdCr0eF', name: 'supabase_id' },
        airtable_id: { id: null, name: 'airtable_id' }, // Derived from Airtable record id (AT -> SB only)
        airtable_id_name_label: { id: 'fldlzkwcYqq3CpLxz', name: 'id_requests' }
    }
};
