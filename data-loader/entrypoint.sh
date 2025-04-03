#!/bin/sh
set -e

echo "🟢 Generating/updating users.csv..."
python /app/insert_users.py

echo "✅ File is ready in shared volume!"

echo "🟢 Waiting for MySQL to be available..."
until mysql -h mysql -uuser -ppassword -e "SELECT 1" &>/dev/null; do
    echo "⏳ MySQL is not available yet, waiting..."
    sleep 2
done
echo "✅ MySQL is available!"

echo "🟢 Creating table 'users' in test_db if not exists..."
mysql -h mysql -uroot -proot test_db -e "CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    date_of_birth DATE NOT NULL
) ENGINE=InnoDB;"
echo "✅ Table 'users' is ready!"

echo "🟢 Preparing MySQL settings..."
mysql -h mysql -uroot -proot test_db -e "SET GLOBAL innodb_lock_wait_timeout = 300;"
mysql -h mysql -uroot -proot test_db -e "SET GLOBAL innodb_flush_log_at_trx_commit = 2;"

echo "🟢 Importing data into MySQL using LOAD DATA INFILE..."
mysql -h mysql -uroot -proot test_db -e "
    LOAD DATA INFILE '/var/lib/mysql-files/users.csv'
    INTO TABLE users
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS (name, email, date_of_birth);"

echo "✅ Data imported successfully!"

touch /app/shared/ready.lock
echo "✅ Data import finished! Created ready.lock."

exec "$@"