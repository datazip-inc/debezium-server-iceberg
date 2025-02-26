# Simple Iceberg Writer

A lightweight service that receives data via gRPC and writes it to Apache Iceberg tables.

## Overview

This project provides a simple gRPC server that accepts data in a JSON format and writes it to Apache Iceberg tables. 
It handles schema evolution, upserts, and batch processing.

## Key Features

- **gRPC-based API**: Send data easily via a simple protocol
- **Automatic Schema Creation**: Tables are created on-the-fly if they don't exist
- **Schema Evolution**: New fields are automatically added to existing tables
- **Upsert Support**: Update existing records by primary key
- **Batch Processing**: Process multiple records efficiently

## Configuration

The service is configured via a JSON string passed as an argument when starting the application. Example:

```json
{
  "jdbc.password": "my_password",
  "s3.path-style-access": "true",
  "jdbc.user": "my_user",
  "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
  "upsert": "true",
  "table-namespace": "public",
  "catalog-name": "iceberg",
  "warehouse": "s3://test-snowfl/shubham/iceberg_equality_test2",
  "uri": "jdbc_db_url",
  "glue.region": "ap-south-1",
  "s3.secret-access-key": "xxx",
  "s3.access-key-id": "xxx",
  "upsert-keep-deletes": "true",
  "write.format.default": "parquet",
  "table-prefix": "olakecdc_"
}
```

## Usage

### Starting the Server

```bash
java -jar simple-iceberg-writer.jar '{"catalog-name":"iceberg","table-namespace":"public","warehouse":"s3://my-bucket/my-path",...}'
```

The server will start and listen for incoming gRPC requests on port 50051 by default.

### Sending Data

Data is sent via the gRPC API in the following JSON format:

```json
{
  "destination_table": "my_table",
  "key": { "id": 123 },
  "value": { 
    "id": 123, 
    "name": "John Doe", 
    "email": "john@example.com",
    "__op": "c",
    "__source_ts_ms": 1625176800000
  }
}
```

Where:
- `destination_table`: The name of the table to write to (will be created if it doesn't exist)
- `key`: The primary key(s) for the record (used for upserts)
- `value`: The actual data to write
  - `__op`: Operation type (c=create/insert, u=update, d=delete, r=read)
  - `__source_ts_ms`: Timestamp in milliseconds (used for conflict resolution)

## Operation Types

- `c` or `i`: Insert a new record
- `u`: Update an existing record
- `d`: Delete a record
- `r`: Read (special operation similar to insert)

## Requirements

- Java 17 or higher
- Apache Iceberg dependencies
- gRPC dependencies

## Building

```bash
mvn clean package
```

The build will create a runnable jar file at `target/simple-iceberg-writer-1.0.0.jar`.

## License

Apache License 2.0 