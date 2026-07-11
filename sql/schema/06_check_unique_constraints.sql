USE retail_db;

-----------------------------------------------------
-- MASTER_DATA.CUSTOMERS
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name='UQ_MASTER_DATA_CUSTOMERS_EMAIL')
BEGIN
    ALTER TABLE master_data.customers
    ADD CONSTRAINT UQ_MASTER_DATA_CUSTOMERS_EMAIL
    UNIQUE (email);
END;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name='CK_MASTER_DATA_CUSTOMERS_GENDER')
BEGIN
    ALTER TABLE master_data.customers
    ADD CONSTRAINT CK_MASTER_DATA_CUSTOMERS_GENDER
    CHECK (gender IN ('M','F','O'))
END;

-----------------------------------------------------
-- MASTER_DATA.PRODUCTS
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_MASTER_DATA_PRODUCTS_NAME_BRAND')
BEGIN
    ALTER TABLE master_data.products
    ADD CONSTRAINT UQ_MASTER_DATA_PRODUCTS_NAME_BRAND
    UNIQUE (product_name, brand);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_MASTER_DATA_PRODUCTS_UNIT_PRICE')
BEGIN
    ALTER TABLE master_data.products
    ADD CONSTRAINT CK_MASTER_DATA_PRODUCTS_UNIT_PRICE
    CHECK (unit_price >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_MASTER_DATA_PRODUCTS_COST_PRICE')
BEGIN
    ALTER TABLE master_data.products
    ADD CONSTRAINT CK_MASTER_DATA_PRODUCTS_COST_PRICE
    CHECK (cost_price >= 0);
END;


-----------------------------------------------------
-- MASTER_DATA.STORES
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_MASTER_DATA_STORES_NAME')
BEGIN
    ALTER TABLE master_data.stores
    ADD CONSTRAINT UQ_MASTER_DATA_STORES_NAME
    UNIQUE (store_name);
END;

-----------------------------------------------------
-- MASTER_DATA.SUPPLIERS
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'UQ_MASTER_DATA_SUPPLIERS_EMAIL')
BEGIN
    ALTER TABLE master_data.suppliers
    ADD CONSTRAINT UQ_MASTER_DATA_SUPPLIERS_EMAIL
    UNIQUE (email);
END;


-----------------------------------------------------
-- SALES.ORDERS
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDERS_TOTAL')
BEGIN
    ALTER TABLE sales.orders
    ADD CONSTRAINT CK_SALES_ORDERS_TOTAL
    CHECK (order_total >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDERS_STATUS')
BEGIN
    ALTER TABLE sales.orders
    ADD CONSTRAINT CK_SALES_ORDERS_STATUS
    CHECK (order_status IN (
        'PENDING',
        'PROCESSING',
        'SHIPPED',
        'DELIVERED',
        'CANCELLED'
    ));
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDERS_PAYMENT')
BEGIN
    ALTER TABLE sales.orders
    ADD CONSTRAINT CK_SALES_ORDERS_PAYMENT
    CHECK (payment_method IN (
        'CASH',
        'CREDIT_CARD',
        'DEBIT_CARD',
        'UPI',
        'NET_BANKING'
    ));
END;

-----------------------------------------------------
-- SALES.ORDER_ITEMS
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDER_ITEMS_QUANTITY')
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT CK_SALES_ORDER_ITEMS_QUANTITY
    CHECK (quantity > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDER_ITEMS_UNIT_PRICE')
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT CK_SALES_ORDER_ITEMS_UNIT_PRICE
    CHECK (unit_price >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDER_ITEMS_DISCOUNT')
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT CK_SALES_ORDER_ITEMS_DISCOUNT
    CHECK (discount_amount >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SALES_ORDER_ITEMS_LINE_TOTAL')
BEGIN
    ALTER TABLE sales.order_items
    ADD CONSTRAINT CK_SALES_ORDER_ITEMS_LINE_TOTAL
    CHECK (line_total >= 0);
END;

-----------------------------------------------------
-- INVENTORY.INVENTORY
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_INVENTORY_QOH')
BEGIN
    ALTER TABLE inventory.inventory
    ADD CONSTRAINT CK_INVENTORY_QOH
    CHECK (quantity_on_hand >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_INVENTORY_REORDER_LEVEL')
BEGIN
    ALTER TABLE inventory.inventory
    ADD CONSTRAINT CK_INVENTORY_REORDER_LEVEL
    CHECK (reorder_level >= 0);
END;

-----------------------------------------------------
-- INVENTORY.INVENTORY_TRANSACTIONS
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_INVENTORY_TXN_TYPE')
BEGIN
    ALTER TABLE inventory.inventory_transactions
    ADD CONSTRAINT CK_INVENTORY_TXN_TYPE
    CHECK (transaction_type IN (
        'SALE',
        'PURCHASE',
        'RETURN',
        'ADJUSTMENT'
    ));
END;

-----------------------------------------------------
-- HR.EMPLOYEES
-----------------------------------------------------

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_HR_EMPLOYEES_SALARY')
BEGIN
    ALTER TABLE hr.employees
    ADD CONSTRAINT CK_HR_EMPLOYEES_SALARY
    CHECK (salary >= 0);
END;