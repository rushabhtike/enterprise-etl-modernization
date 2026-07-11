USE retail_db;

-----------------------------------------------------
-- MASTER DATA
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_MASTER_DATA_CUSTOMERS')
BEGIN
    ALTER TABLE master_data.customers
    ADD CONSTRAINT PK_MASTER_DATA_CUSTOMERS
    PRIMARY KEY (customer_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_MASTER_DATA_SUPPLIERS')
BEGIN
    ALTER TABLE master_data.suppliers
    ADD CONSTRAINT PK_MASTER_DATA_SUPPLIERS
    PRIMARY KEY (supplier_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_MASTER_DATA_PRODUCTS')
BEGIN
    ALTER TABLE master_data.products
    ADD CONSTRAINT PK_MASTER_DATA_PRODUCTS
    PRIMARY KEY (product_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_MASTER_DATA_STORES')
BEGIN
    ALTER TABLE master_data.stores
    ADD CONSTRAINT PK_MASTER_DATA_STORES
    PRIMARY KEY (store_id);
END;

-----------------------------------------------------
-- SALES
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_SALES_ORDERS')
BEGIN
    ALTER TABLE sales.orders
    ADD CONSTRAINT PK_SALES_ORDERS
    PRIMARY KEY (order_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_SALES_ORDER_ITEMS')
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT PK_SALES_ORDER_ITEMS
    PRIMARY KEY (order_item_id);
END;

-----------------------------------------------------
-- INVENTORY
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_INVENTORY')
BEGIN
    ALTER TABLE inventory.inventory
    ADD CONSTRAINT PK_INVENTORY
    PRIMARY KEY (inventory_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_INVENTORY_TRANSACTIONS')
BEGIN
    ALTER TABLE inventory.inventory_transactions
    ADD CONSTRAINT PK_INVENTORY_TRANSACTIONS
    PRIMARY KEY (transaction_id);
END;

-----------------------------------------------------
-- HR
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_HR_EMPLOYEES')
BEGIN
    ALTER TABLE hr.employees
    ADD CONSTRAINT PK_HR_EMPLOYEES
    PRIMARY KEY (employee_id);
END;

-----------------------------------------------------
-- AUDIT
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_AUDIT_ETL_RUN_HISTORY')
BEGIN
    ALTER TABLE audit.etl_run_history
    ADD CONSTRAINT PK_AUDIT_ETL_RUN_HISTORY
    PRIMARY KEY (run_id);
END;


