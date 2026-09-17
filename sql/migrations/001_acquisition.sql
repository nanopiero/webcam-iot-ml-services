-- Migration 001. Apply once to a dedicated ML database with psql -v ON_ERROR_STOP=1 -f.
-- No destructive reset or production deployment is performed by this file.
BEGIN;

CREATE TABLE schema_migration (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE registry_settings (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    partition_count integer NOT NULL CHECK (partition_count > 0),
    partition_hash text NOT NULL CHECK (partition_hash = 'sha256')
);
INSERT INTO registry_settings VALUES (true, 50, 'sha256');

CREATE TABLE derived_stream (
    derived_stream_id text PRIMARY KEY,
    network_id text NOT NULL,
    metadata jsonb NOT NULL,
    metadata_fingerprint text NOT NULL,
    metadata_timestamp timestamptz NOT NULL,
    current_generation integer,
    current_profile_id integer,
    status_override text CHECK (status_override IN ('blacklist', 'greylist', 'whitelist')),
    operational_status text NOT NULL DEFAULT 'whitelist'
        CHECK (operational_status IN ('blacklist', 'greylist', 'whitelist')),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Normalize profiles so all panoramic slices share one profile number.
-- The writer must serialize discovery per parent stream (SELECT FOR UPDATE)
-- before allocating profile_id. A recurring fingerprint reuses its profile.
CREATE TABLE processing_profile (
    derived_stream_id text NOT NULL REFERENCES derived_stream,
    generation_version integer NOT NULL CHECK (generation_version >= 0),
    profile_id integer NOT NULL CHECK (profile_id >= 0),
    profile_fingerprint text NOT NULL,
    source_width integer NOT NULL CHECK (source_width > 0),
    source_height integer NOT NULL CHECK (source_height > 0),
    colour_mode text NOT NULL,
    colour_depth integer CHECK (colour_depth > 0),
    derived_width integer NOT NULL CHECK (derived_width > 0),
    derived_height integer NOT NULL CHECK (derived_height > 0),
    PRIMARY KEY (derived_stream_id, generation_version, profile_id),
    UNIQUE (derived_stream_id, generation_version, profile_fingerprint)
);

CREATE TABLE processing_stream (
    processing_stream_id text PRIMARY KEY,
    derived_stream_id text NOT NULL,
    generation_version integer NOT NULL,
    profile_id integer NOT NULL,
    slice_index integer NOT NULL CHECK (slice_index >= 0),
    slice_left integer NOT NULL CHECK (slice_left >= 0),
    slice_right integer NOT NULL CHECK (slice_right > slice_left),
    process_snow boolean NOT NULL DEFAULT true,
    process_visibility boolean NOT NULL DEFAULT true,
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    FOREIGN KEY (derived_stream_id, generation_version, profile_id)
        REFERENCES processing_profile,
    UNIQUE (derived_stream_id, generation_version, profile_id, slice_index)
);

INSERT INTO schema_migration (version) VALUES (1);
COMMIT;
