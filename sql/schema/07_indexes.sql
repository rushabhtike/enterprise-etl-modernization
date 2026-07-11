USE retail_db;

-----------------------------------------------------
-- MASTER DATA
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CUSTOMERS_EMAIL')
BEGIN
    CREATE INDEX IX_CUSTOMERS_EMAIL
    ON master_data.customers(email);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PRODUCTS_SUPPLIER_ID')
BEGIN
    CREATE INDEX IX_PRODUCTS_SUPPLIER_ID
    ON master_data.products(supplier_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PRODUCTS_CATEGORY')
BEGIN
    CREATE INDEX IX_PRODUCTS_CATEGORY
    ON master_data.products(category);
END;

-----------------------------------------------------
-- SALES
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ORDERS_CUSTOMER_ID')
BEGIN
    CREATE INDEX IX_ORDERS_CUSTOMER_ID
    ON sales.orders(customer_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ORDERS_STORE_ID')
BEGIN
    CREATE INDEX IX_ORDERS_STORE_ID
    ON sales.orders(store_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ORDERS_ORDER_DATE')
BEGIN
    CREATE INDEX IX_ORDERS_ORDER_DATE
    ON sales.orders(order_date);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ORDER_ITEMS_ORDER_ID')
BEGIN
    CREATE INDEX IX_ORDER_ITEMS_ORDER_ID
    ON sales.order_items(order_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ORDER_ITEMS_PRODUCT_ID')
BEGIN
    CREATE INDEX IX_ORDER_ITEMS_PRODUCT_ID
    ON sales.order_items(product_id);
END;

-----------------------------------------------------
-- INVENTORY
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_INVENTORY_STORE_PRODUCT')
BEGIN
    CREATE INDEX IX_INVENTORY_STORE_PRODUCT
    ON inventory.inventory(store_id, product_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_INVENTORY_TXN_PRODUCT')
BEGIN
    CREATE INDEX IX_INVENTORY_TXN_PRODUCT
    ON inventory.inventory_transactions(product_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_INVENTORY_TXN_STORE')
BEGIN
    CREATE INDEX IX_INVENTORY_TXN_STORE
    ON inventory.inventory_transactions(store_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_INVENTORY_TXN_TIMESTAMP')
BEGIN
    CREATE INDEX IX_INVENTORY_TXN_TIMESTAMP
    ON inventory.inventory_transactions(transaction_timestamp);
END;

-----------------------------------------------------
-- HR
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EMPLOYEES_STORE_ID')
BEGIN
    CREATE INDEX IX_EMPLOYEES_STORE_ID
    ON hr.employees(store_id);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EMPLOYEES_MANAGER_ID')
BEGIN
    CREATE INDEX IX_EMPLOYEES_MANAGER_ID
    ON hr.employees(manager_id);
END;