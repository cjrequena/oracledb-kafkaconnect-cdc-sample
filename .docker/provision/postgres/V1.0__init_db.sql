-- Create ORDERS table
CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    order_date      DATE DEFAULT CURRENT_DATE NOT NULL,
    status          VARCHAR(20) DEFAULT 'PENDING' NOT NULL,
    total_amount    NUMERIC(10, 2) NOT NULL,
    currency        VARCHAR(3) DEFAULT 'USD' NOT NULL,
    notes           VARCHAR(500),
    created_at      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Create CHANGE_TRACKING table
CREATE TABLE change_tracking (
    id                  BIGSERIAL PRIMARY KEY,
    table_name          VARCHAR(100) NOT NULL,
    type                VARCHAR(20) NOT NULL,
    data_content_type   VARCHAR(100),
    data                JSONB CHECK (jsonb_typeof(data) = 'object'),
    data_base64         TEXT,
    created_at          TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger function for ORDERS change tracking
CREATE OR REPLACE FUNCTION trg_orders_change_tracking_fn()
RETURNS TRIGGER AS $$
DECLARE
    v_action_type TEXT;
    v_json_data   JSONB;
    v_data_base64 TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_action_type := 'INSERT';
        v_json_data := jsonb_build_object(
            'id', NEW.id,
            'customer_id', NEW.customer_id,
            'order_date', to_char(NEW.order_date, 'YYYY-MM-DD"T"HH24:MI:SS'),
            'status', NEW.status,
            'total_amount', NEW.total_amount,
            'currency', NEW.currency,
            'notes', NEW.notes,
            'created_at', to_char(NEW.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')
        );

    ELSIF TG_OP = 'UPDATE' THEN
        v_action_type := 'UPDATE';
        v_json_data := jsonb_build_object(
            'id', NEW.id,
            'customer_id', NEW.customer_id,
            'order_date', to_char(NEW.order_date, 'YYYY-MM-DD"T"HH24:MI:SS'),
            'status', NEW.status,
            'total_amount', NEW.total_amount,
            'currency', NEW.currency,
            'notes', NEW.notes,
            'created_at', to_char(NEW.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')
        );

    ELSIF TG_OP = 'DELETE' THEN
        v_action_type := 'DELETE';
        v_json_data := jsonb_build_object(
            'id', OLD.id,
            'customer_id', OLD.customer_id,
            'order_date', to_char(OLD.order_date, 'YYYY-MM-DD"T"HH24:MI:SS'),
            'status', OLD.status,
            'total_amount', OLD.total_amount,
            'currency', OLD.currency,
            'notes', OLD.notes,
            'created_at', to_char(OLD.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')
        );
    END IF;

    -- Encode JSON as base64
    v_data_base64 := encode(convert_to(v_json_data::text, 'UTF8'), 'base64');

    -- Insert into change_tracking table
    INSERT INTO change_tracking (
        table_name,
        type,
        data_content_type,
        data,
        data_base64
    )
    VALUES (
        'ORDERS',
        v_action_type,
        'application/json',
        v_json_data,
        v_data_base64
    );

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trg_orders_change_tracking
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
EXECUTE FUNCTION trg_orders_change_tracking_fn();
