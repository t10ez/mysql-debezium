# Debezium MySQL1 → MySQL2: ทำมือทีละ step

ทุก step มี 3 ส่วน: **ทำ** (คำสั่ง) · **ทำไม** · **เช็ค** (ต้องเห็นอะไรก่อนไป step ถัดไป)

```
mysql1 → binlog → Debezium source → Kafka → JDBC sink → mysql2
```

ต้องมี: Docker, `docker-compose`, `curl`, `jq`

---

## Step 1 — สร้าง folder

```sh
mkdir -p ~/Workspace/t10e/debezium-mysql-lab/{mysql1,mysql2,connectors}
cd ~/Workspace/t10e/debezium-mysql-lab
```

---

## Step 2 — เตรียม mysql1 (source)

สร้าง `mysql1/init.sql`:

```sql
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
```

**ทำไม**: Debezium ทำตัวเป็น replica → user ต้องมี `REPLICATION SLAVE/CLIENT` (อ่าน binlog) + `SELECT, RELOAD` (snapshot) ไม่ต้องมีสิทธิ์เขียน

---

## Step 3 — เตรียม mysql2 (target)

สร้าง `mysql2/init.sql`:

```sql
CREATE USER IF NOT EXISTS 'sink'@'%' IDENTIFIED BY 'sink';
GRANT ALL PRIVILEGES ON inventory_replica.* TO 'sink'@'%';
FLUSH PRIVILEGES;
```

**ทำไม**: sink connector สร้าง table เองจาก schema ที่ Debezium ส่งมา — ไม่ต้องสร้างล่วงหน้า

---

## Step 4 — `docker-compose.yml`

```yaml
name: debezium-lab

services:
  mysql1:
    image: mysql:8.4
    container_name: mysql1
    command:
      - --server-id=1
      - --log-bin=mysql-bin
      - --binlog-format=ROW
      - --binlog-row-image=FULL
      - --gtid-mode=ON
      - --enforce-gtid-consistency=ON
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: inventory
    ports: ["3306:3306"]
    volumes:
      - ./mysql1/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-proot"]
      interval: 5s
      retries: 20

  mysql2:
    image: mysql:8.4
    container_name: mysql2
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: inventory_replica
    ports: ["3307:3306"]
    volumes:
      - ./mysql2/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-proot"]
      interval: 5s
      retries: 20

  kafka:
    image: apache/kafka:3.9.1
    container_name: kafka
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 5s
      retries: 20

  connect:
    image: quay.io/debezium/connect:3.3
    container_name: connect
    depends_on:
      kafka: { condition: service_healthy }
      mysql1: { condition: service_healthy }
      mysql2: { condition: service_healthy }
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: connect-cluster
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      CONNECT_CONSUMER_METADATA_MAX_AGE_MS: "10000"
    ports: ["8083:8083"]

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: kafka-ui
    depends_on: [kafka, connect]
    environment:
      KAFKA_CLUSTERS_0_NAME: lab
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: connect
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://connect:8083
    ports: ["8080:8080"]
```

**ทำไม**:
- mysql1: `binlog-format=ROW` + `binlog-row-image=FULL` → binlog เก็บค่าทุก column ทั้ง before/after (Debezium บังคับ)
- kafka: KRaft mode ไม่ต้องมี ZooKeeper
- connect: image Debezium มี MySQL source + JDBC sink + MySQL driver ในตัว
- `CONNECT_CONSUMER_METADATA_MAX_AGE_MS=10000` → sink เห็น topic ใหม่ใน 10 วิ (default 5 นาที)

---

## Step 5 — Start

```sh
docker-compose up -d
docker-compose ps
```

**เช็ค**: mysql1, mysql2, kafka = `healthy`, connect = `Up`. รอ Connect พร้อม:

```sh
until curl -sf localhost:8083/connectors; do sleep 2; done; echo
# []
```

เช็ค binlog ที่ mysql1:

```sh
docker exec mysql1 mysql -uroot -proot -e "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image','gtid_mode')"
# log_bin ON / binlog_format ROW / binlog_row_image FULL / gtid_mode ON
```

เช็ค plugin มีครบ:

```sh
curl -s localhost:8083/connector-plugins | jq -r '.[].class' | grep -E 'MySql|Jdbc'
# io.debezium.connector.jdbc.JdbcSinkConnector
# io.debezium.connector.mysql.MySqlConnector
```

---

## Step 6 — สร้าง source connector

`connectors/mysql1-source.json`:

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "tasks.max": "1",
  "database.hostname": "mysql1",
  "database.port": "3306",
  "database.user": "debezium",
  "database.password": "dbz",
  "database.server.id": "184054",
  "topic.prefix": "mysql1",
  "database.include.list": "inventory",
  "table.include.list": "inventory.customers,inventory.orders",
  "snapshot.mode": "initial",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
  "schema.history.internal.kafka.topic": "schema-history.mysql1"
}
```

```sh
curl -s -X PUT -H 'Content-Type: application/json' \
  --data @connectors/mysql1-source.json \
  localhost:8083/connectors/mysql1-source/config | jq .name
```

**ทำไม**:
| Property | |
|---|---|
| `database.server.id` | Debezium ต่อเป็น replica → id ห้ามซ้ำ server อื่น |
| `topic.prefix` | topic = `mysql1.inventory.customers` |
| `snapshot.mode=initial` | copy ข้อมูลเดิมก่อน (`op=r`) แล้วอ่าน binlog ต่อ |
| `schema.history.internal.kafka.topic` | เก็บ DDL ไว้ decode binlog — ห้ามลบ |

**เช็ค**:

```sh
curl -s localhost:8083/connectors/mysql1-source/status | jq '.connector.state, .tasks[].state'
# "RUNNING" "RUNNING"

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep mysql1
# mysql1.inventory.customers
# mysql1.inventory.orders
# schema-history.mysql1
```

---

## Step 7 — ดู event ใน Kafka (ก่อนมี sink)

```sh
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic mysql1.inventory.customers \
  --from-beginning --property print.key=true --max-messages 3
```

หรือเปิด http://localhost:8080 → Topics → `mysql1.inventory.customers` → Messages

**เช็ค**: 3 message, `op: "r"`, `before: null`, `after: {...}`

ลองแก้แล้วดู event ใหม่ (terminal อื่นเปิด consumer ทิ้งไว้ ไม่ใส่ `--max-messages`):

```sh
docker exec mysql1 mysql -uroot -proot inventory -e "UPDATE customers SET last_name='Boonmee' WHERE id=2"
```

→ เห็น `op: "u"` มีทั้ง `before` และ `after`

> ตอนนี้ event อยู่ใน Kafka แต่ยังไม่มีใครอ่าน — mysql2 ยังว่าง

---

## Step 8 — สร้าง sink connector

`connectors/mysql2-sink.json`:

```json
{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "tasks.max": "1",
  "topics.regex": "mysql1\\.inventory\\.(customers|orders)",
  "connection.url": "jdbc:mysql://mysql2:3306/inventory_replica",
  "connection.username": "sink",
  "connection.password": "sink",
  "insert.mode": "upsert",
  "delete.enabled": "true",
  "primary.key.mode": "record_key",
  "schema.evolution": "basic",
  "collection.name.format": "${topic}",
  "transforms": "unwrap,route",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.tombstone.handling.mode": "tombstone",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "mysql1\\.inventory\\.(.*)",
  "transforms.route.replacement": "$1"
}
```

```sh
curl -s -X PUT -H 'Content-Type: application/json' \
  --data @connectors/mysql2-sink.json \
  localhost:8083/connectors/mysql2-sink/config | jq .name
```

**ทำไม**:
| Property | |
|---|---|
| `insert.mode=upsert` | `INSERT ... ON DUPLICATE KEY UPDATE` → replay ซ้ำได้ |
| `primary.key.mode=record_key` | PK = key ของ Kafka message (= PK ต้นทาง) |
| `delete.enabled=true` | tombstone → `DELETE` |
| `schema.evolution=basic` | sink สร้าง table เองและ ADD COLUMN เมื่อ schema เปลี่ยน |
| `unwrap` | แกะ Debezium envelope → แถวแบนก่อนเขียน DB |
| `route` | topic `mysql1.inventory.customers` → `customers` (ชื่อ table) |
| `collection.name.format=${topic}` | ชื่อ table = topic หลัง route |

> **กับดัก**: ถ้าใช้ `unwrap` แต่ `collection.name.format=${source.table}` → sink **ข้ามทุก record เงียบๆ** สถานะยัง RUNNING มีแค่ WARN `No resolvable table name` ใน `docker logs connect`

**เช็ค**:

```sh
curl -s localhost:8083/connectors/mysql2-sink/status | jq '.connector.state, .tasks[].state'
# "RUNNING" "RUNNING"

docker exec mysql2 mysql -uroot -proot inventory_replica -e "SELECT id,first_name,last_name,email FROM customers"
```

ต้องเห็น 3 แถว, id=2 `last_name=Boonmee` (event จาก Step 7 ที่ค้างใน Kafka ถูก consume แล้ว)

---

## Step 9 — ทดสอบ INSERT / UPDATE / DELETE

ตั้ง alias ให้พิมพ์สั้น:

```sh
alias m1='docker exec -i mysql1 mysql -uroot -proot inventory -e'
alias m2='docker exec -i mysql2 mysql -uroot -proot inventory_replica -e'
```

```sh
m1 "INSERT INTO customers (first_name,last_name,email) VALUES ('Mana','Deejai','mana@example.com')"
m2 "SELECT id,first_name,last_name FROM customers"               # แถวใหม่ปรากฏ

m1 "UPDATE customers SET last_name='Srisuk' WHERE email='mana@example.com'"
m2 "SELECT id,last_name FROM customers WHERE email='mana@example.com'"   # Srisuk

m1 "DELETE FROM customers WHERE email='mana@example.com'"
m2 "SELECT COUNT(*) FROM customers WHERE email='mana@example.com'"   # 0
```

---

## Step 10 — ทดสอบ schema change

```sh
m1 "ALTER TABLE customers ADD COLUMN phone VARCHAR(20) NULL"
m2 "SHOW COLUMNS FROM customers LIKE 'phone'"      # ยังไม่มี — DDL ไปแค่ schema-history

m1 "UPDATE customers SET phone='0812345678' WHERE id=1"
m2 "SELECT id, phone FROM customers WHERE id=1"     # มีแล้ว — sink ADD COLUMN ตอนเจอ event ที่มี field phone
```

---

## Step 11 — ทดสอบ sink ล่ม (lag)

```sh
curl -X PUT localhost:8083/connectors/mysql2-sink/pause
m1 "INSERT INTO customers (first_name,last_name,email) VALUES ('Piti','Boonmee','piti@example.com')"
m2 "SELECT COUNT(*) FROM customers WHERE email='piti@example.com'"   # 0 — ค้างใน Kafka

docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group connect-mysql2-sink                               # คอลัมน์ LAG > 0

curl -X PUT localhost:8083/connectors/mysql2-sink/resume
sleep 5
m2 "SELECT COUNT(*) FROM customers WHERE email='piti@example.com'"   # 1, LAG กลับเป็น 0
```

---

## Step 12 — ทดสอบ connect ล่ม (offset)

```sh
docker stop connect
m1 "INSERT INTO customers (first_name,last_name,email) VALUES ('Kanya','Thongdee','kanya@example.com')"
docker start connect
until curl -sf localhost:8083/connectors >/dev/null; do sleep 2; done; sleep 10
m2 "SELECT COUNT(*) FROM customers WHERE email='kanya@example.com'"   # 1
```

**ทำไม**: source อ่าน binlog ต่อจาก offset ใน topic `connect_offsets` → change ระหว่างล่มไม่หาย (ตราบที่ binlog ยังไม่ถูก purge)

---

## Step 13 — เทียบ 2 ฝั่ง (reconcile)

```sh
Q="SELECT id, MD5(CONCAT_WS('|', id, first_name, last_name, email)) h FROM customers ORDER BY id"
diff <(docker exec mysql1 mysql -uroot -proot -N inventory -e "$Q" 2>/dev/null) \
     <(docker exec mysql2 mysql -uroot -proot -N inventory_replica -e "$Q" 2>/dev/null) \
  && echo "ตรงกันทุกแถว"
```

---

## Debug

```sh
curl -s localhost:8083/connectors/mysql2-sink/status | jq -r '.tasks[].trace'   # error stack
docker logs connect --tail 100 | grep -E 'WARN|ERROR'
curl -X POST 'localhost:8083/connectors/mysql2-sink/restart?includeTasks=true&onlyFailed=true'
curl -X DELETE localhost:8083/connectors/mysql2-sink                             # ลบ connector
```

## ลบ lab

```sh
docker-compose down -v
```
