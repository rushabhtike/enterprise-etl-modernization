USE retail_db;

-----------------------------------------------------
-- Products -> Suppliers
-----------------------------------------------------

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_PRODUCTS_SUPPLIERS'
)
BEGIN
    ALTER TABLE master_data.products
    ADD CONSTRAINT FK_PRODUCTS_SUPPLIERS
    FOREIGN KEY (supplier_id)
    REFERENCES master_data.suppliers(supplier_id);
END;
-----------------------------------------------------
-- Orders
-----------------------------------------------------

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_ORDERS_CUSTOMERS'
)
BEGIN
    ALTER TABLE sales.orders
    ADD CONSTRAINT FK_ORDERS_CUSTOMERS
    FOREIGN KEY (customer_id)
    REFERENCES master_data.customers(customer_id);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_ORDERS_STORES'
)
BEGIN
    ALTER TABLE sales.orders
    ADD CONSTRAINT FK_ORDERS_STORES
    FOREIGN KEY (store_id)
    REFERENCES master_data.stores(store_id);
END;
-----------------------------------------------------
-- Order Items
-----------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_ORDER_ITEMS_ORDERS'
)
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT FK_ORDER_ITEMS_ORDERS
    FOREIGN KEY (order_id)
    REFERENCES sales.orders(order_id);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_ORDER_ITEMS_PRODUCTS'
)
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT FK_ORDER_ITEMS_PRODUCTS
    FOREIGN KEY (product_id)
    REFERENCES master_data.products(product_id);
END;
-----------------------------------------------------
-- Inventory
-----------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_INVENTORY_PRODUCTS'
)
BEGIN
    ALTER TABLE inventory.inventory
    ADD CONSTRAINT FK_INVENTORY_PRODUCTS
    FOREIGN KEY (product_id)
    REFERENCES master_data.products(product_id);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_INVENTORY_STORES'
)
BEGIN
    ALTER TABLE inventory.inventory
    ADD CONSTRAINT FK_INVENTORY_STORES
    FOREIGN KEY (store_id)
    REFERENCES master_data.stores(store_id);
END;

-----------------------------------------------------
-- Inventory Transactions
-----------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_INV_TXN_PRODUCTS'
)
BEGIN
    ALTER TABLE inventory.inventory_transactions
    ADD CONSTRAINT FK_INV_TXN_PRODUCTS
    FOREIGN KEY (product_id)
    REFERENCES master_data.products(product_id);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_INV_TXN_STORES'
)
BEGIN
    ALTER TABLE inventory.inventory_transactions
    ADD CONSTRAINT FK_INV_TXN_STORES
    FOREIGN KEY (store_id)
    REFERENCES master_data.stores(store_id);
END;

-----------------------------------------------------
-- Employees
-----------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_EMPLOYEES_STORES'
)
BEGIN
    ALTER TABLE hr.employees
    ADD CONSTRAINT FK_EMPLOYEES_STORES
    FOREIGN KEY (store_id)
    REFERENCES master_data.stores(store_id);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'FK_EMPLOYEES_MANAGER'
)
BEGIN
    ALTER TABLE hr.employees
    ADD CONSTRAINT FK_EMPLOYEES_MANAGER
    FOREIGN KEY (manager_id)
    REFERENCES hr.employees(employee_id);
END;