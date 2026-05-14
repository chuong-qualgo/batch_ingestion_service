-- =============================================================================
-- Demo source table: orders
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.orders (
    id            SERIAL PRIMARY KEY,
    customer_id   INT          NOT NULL,
    product_code  VARCHAR(50)  NOT NULL,
    quantity      INT          NOT NULL DEFAULT 1,
    unit_price    NUMERIC(10,2) NOT NULL,
    total_price   NUMERIC(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'confirmed', 'shipped', 'cancelled')),
    region        VARCHAR(50)  NOT NULL DEFAULT 'APAC',
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Index on checkpoint column for efficient incremental reads
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON public.orders (updated_at);

-- =============================================================================
-- Pipeline checkpoint store
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.checkpoints (
    dag_id        TEXT PRIMARY KEY,
    checkpoint_to TEXT        NOT NULL,
    updated_at    TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Trigger: keep updated_at current on every row update
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_orders_updated_at ON public.orders;
CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON public.orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
