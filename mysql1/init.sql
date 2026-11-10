CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

USE inventory;

CREATE TABLE customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name  VARCHAR(100) NOT NULL,
  email      VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  status      VARCHAR(20) NOT NULL DEFAULT 'NEW',
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);

INSERT INTO customers (first_name, last_name, email) VALUES
  ('Somchai', 'Jaidee', 'somchai@example.com'),
  ('Suda',    'Rakthai', 'suda@example.com'),
  ('Anan',    'Wongsa',  'anan@example.com');

INSERT INTO orders (customer_id, amount, status) VALUES
  (1, 1500.00, 'NEW'),
  (2,  320.50, 'PAID');
