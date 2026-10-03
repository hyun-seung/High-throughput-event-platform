CREATE TABLE IF NOT EXISTS phone_carrier_mappings (
    phone_number varchar(11) PRIMARY KEY CHECK (phone_number ~ '^010[0-9]{8}$'),
    carrier varchar(3) NOT NULL CHECK (carrier IN ('SKT', 'KT', 'LGU')),
    updated_at timestamptz NOT NULL DEFAULT now()
);
