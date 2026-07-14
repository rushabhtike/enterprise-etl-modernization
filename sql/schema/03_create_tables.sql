USE retail_db;

--------------------------
--MASTER DATA
--------------------------

CREATE TABLE master_data.customers
(
    customer_id INT IDENTITY(1,1) NOT NULL,
    first_name  NVARCHAR(100) NOT NULL,
    last_name   NVARCHAR(100) NOT NULL,
    email       NVARCHAR(255) NOT NULL,
    phone       NVARCHAR(25) NULL,
    date_of_birth DATE,
    gender      CHAR(1),
    
    city NVARCHAR(100),
    state NVARCHAR(100),
    country NVARCHAR(100),

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0
);

--------------------------------------------------------

CREATE TABLE master_data.suppliers
(
    supplier_id INT IDENTITY(1,1) NOT NULL,

    supplier_name NVARCHAR(255) NOT NULL,
    contact_name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(25),
    country NVARCHAR(100),

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0
);

--------------------------------------------------------

CREATE TABLE master_data.products
(
    product_id INT IDENTITY(1,1) NOT NULL,

    supplier_id INT NOT NULL,

    product_name NVARCHAR(255) NOT NULL,
    category NVARCHAR(100),
    subcategory NVARCHAR(100),
    brand NVARCHAR(100),

    unit_price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2) NOT NULL,

    is_active BIT NOT NULL DEFAULT 1,

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0
);

--------------------------------------------------------

CREATE TABLE master_data.stores
(
    store_id INT IDENTITY(1,1) NOT NULL,

    store_name NVARCHAR(255) NOT NULL,
    city NVARCHAR(100),
    state NVARCHAR(100),
    country NVARCHAR(100),

    store_type NVARCHAR(50),

    opened_date DATE,

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0
);

CREATE TABLE sales.orders
(
    order_id INT IDENTITY(1,1) NOT NULL,

    customer_id INT NOT NULL,
    store_id INT NOT NULL,

    order_date DATETIME2 NOT NULL,

    order_status NVARCHAR(30) NOT NULL,

    payment_method NVARCHAR(30) NOT NULL,

    order_total DECIMAL(12,2) NOT NULL,

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0

);


CREATE TABLE sales.order_items
(
    order_item_id INT IDENTITY(1,1) NOT NULL,

    order_id INT NOT NULL,

    product_id INT NOT NULL,

    quantity INT NOT NULL,

    unit_price DECIMAL(10,2) NOT NULL,

    discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0,

    line_total DECIMAL(12,2) NOT NULL,

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0

);


CREATE TABLE inventory.inventory
(
    inventory_id INT IDENTITY(1,1) NOT NULL,

    store_id INT NOT NULL,
    product_id INT NOT NULL,

    quantity_on_hand INT NOT NULL,

    reorder_level INT NOT NULL,

    last_updated DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0

);


CREATE TABLE inventory.inventory_transactions
(
    transaction_id INT IDENTITY(1,1) NOT NULL,

    store_id INT NOT NULL,

    product_id INT NOT NULL,

    transaction_type NVARCHAR(20) NOT NULL,

    quantity INT NOT NULL,

    transaction_timestamp DATETIME2 NOT NULL,

    reference_id INT,

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0

);


CREATE TABLE hr.employees
(
    employee_id INT IDENTITY(1,1) NOT NULL,

    store_id INT NOT NULL,

    first_name NVARCHAR(100) NOT NULL,

    last_name NVARCHAR(100) NOT NULL,

    email NVARCHAR(255),

    phone NVARCHAR(25),

    job_title NVARCHAR(100),

    hire_date DATE NOT NULL,

    salary DECIMAL(12,2),

    manager_id INT,

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(50) NOT NULL DEFAULT 'SQLSERVER',
    is_deleted BIT NOT NULL DEFAULT 0

);


CREATE TABLE audit.etl_run_history
(
    run_id BIGINT IDENTITY(1,1) NOT NULL,

    pipeline_name NVARCHAR(100) NOT NULL,

    source_system NVARCHAR(50) NOT NULL,

    source_table NVARCHAR(200) NOT NULL,

    target_table NVARCHAR(200) NOT NULL,

    load_type NVARCHAR(20) NOT NULL,

    batch_id NVARCHAR(100) NOT NULL,

    rows_read BIGINT,

    rows_written BIGINT,

    rows_rejected BIGINT,

    start_timestamp DATETIME2 NOT NULL,

    end_timestamp DATETIME2,

    status NVARCHAR(20) NOT NULL,

    error_message NVARCHAR(MAX),

    created_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()

);