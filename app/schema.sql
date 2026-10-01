-- Applied idempotently at startup (under an advisory lock, see app/migrate.py).

CREATE TABLE IF NOT EXISTS shows (
    id             uuid PRIMARY KEY,
    name           text        NOT NULL,
    price_paise    bigint      NOT NULL CHECK (price_paise >= 0),   -- integer minor units, never float
    per_user_limit integer     NOT NULL CHECK (per_user_limit > 0),
    total_seats    integer     NOT NULL CHECK (total_seats > 0),
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- One row per physical seat. The (show_id, label) primary key is what makes a seat a
-- unique thing: there is exactly one row whose status can ever say who owns A12.
CREATE TABLE IF NOT EXISTS seats (
    show_id        uuid        NOT NULL REFERENCES shows(id),
    label          text        NOT NULL,
    ordinal        integer     NOT NULL,          -- display order as given at creation
    status         text        NOT NULL DEFAULT 'available'
                               CHECK (status IN ('available', 'held', 'confirmed')),
    reservation_id uuid,
    user_id        text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (show_id, label),
    -- A seat is owned iff it is not available. Makes "confirmed but ownerless" or
    -- "available but still pointing at a reservation" unrepresentable.
    CONSTRAINT seat_owner_consistent CHECK (
        (status = 'available' AND reservation_id IS NULL AND user_id IS NULL) OR
        (status <> 'available' AND reservation_id IS NOT NULL AND user_id IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS seats_by_reservation ON seats (reservation_id) WHERE reservation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS reservations (
    id              uuid PRIMARY KEY,
    show_id         uuid        NOT NULL REFERENCES shows(id),
    user_id         text        NOT NULL,
    seats           text[]      NOT NULL,
    amount_paise    bigint      NOT NULL CHECK (amount_paise >= 0),
    status          text        NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    idempotency_key text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    cancelled_at    timestamptz
);
CREATE INDEX IF NOT EXISTS reservations_by_show_user ON reservations (show_id, user_id);

-- Per (show, user) running count of seats currently owned. Updated with a guarded
-- increment so the limit check and the increment are one atomic statement.
CREATE TABLE IF NOT EXISTS user_quota (
    show_id    uuid    NOT NULL REFERENCES shows(id),
    user_id    text    NOT NULL,
    seats_held integer NOT NULL DEFAULT 0 CHECK (seats_held >= 0),
    PRIMARY KEY (show_id, user_id)
);

-- Idempotency keys are scoped to (authenticated user, show): user B can never replay or
-- collide with user A's key, and reusing "k1" on a different show is a new request.
-- A key is bound only to a *successful* reservation (written in the same transaction);
-- a declined attempt rolls back and leaves no key, so its retry is re-evaluated.
-- The stored response lets a retry get byte-for-byte the original answer.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    user_id        text        NOT NULL,
    show_id        uuid        NOT NULL,
    key            text        NOT NULL,
    request_hash   text        NOT NULL,
    status_code    integer,
    response       json,             -- json (not jsonb) keeps replayed bodies byte-identical
    reservation_id uuid,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, show_id, key)
);
