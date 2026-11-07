-- Template registry (M3): one row per template, one per immutable version.

CREATE TABLE templates (
    id          text PRIMARY KEY,
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE template_versions (
    template_id  text NOT NULL REFERENCES templates (id),
    version      int NOT NULL CHECK (version > 0),
    status       text NOT NULL CHECK (status IN ('draft', 'active', 'deprecated', 'retired')),
    spec         jsonb NOT NULL,
    token_counts jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (template_id, version)
);

-- The snapshot refresher's served-versions query (LoadServed).
CREATE INDEX template_versions_served_idx ON template_versions (template_id, version)
    WHERE status IN ('active', 'deprecated');
