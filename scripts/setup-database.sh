#!/bin/bash

# Setup script for initializing the MySQL database schema and migrations

echo "🔧 Setting up the MySQL database..."

# Load configuration
source ../lambda/shared/config.ts

# Check if the database connection details are set
if [ -z "$DB_HOST" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DB_NAME" ]; then
    echo "❌ Database connection details are not set in the config. Please check your configuration."
    exit 1
fi

# Execute the SQL schema and migrations
echo "📦 Applying schema and migrations to the database..."

# Connect to the MySQL database and execute the schema
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < ../sql/schema.sql

# Apply migrations
for migration in ../sql/migrations/*.sql; do
    echo "📜 Applying migration: $(basename "$migration")"
    mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < "$migration"
done

echo "✅ Database setup completed successfully!"