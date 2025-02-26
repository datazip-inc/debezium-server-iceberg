/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.Set;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

/**
 * Utility class for Iceberg operations
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {
    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }
    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                       Schema schema, String writeFormat) {
    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }

    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperty(FORMAT_VERSION, "2")
        .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
        .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
        .create();
  }

  private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    try {
      Set<String> identifierFieldNames = schema.identifierFieldNames();
      if (identifierFieldNames != null && !identifierFieldNames.isEmpty()) {
        for (String fieldName : identifierFieldNames) {
          if (fieldName != null && !fieldName.isEmpty()) {
            sob = sob.asc(fieldName);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to get identifier field names: {}", e.getMessage());
    }
    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    return FileFormat.valueOf(icebergTable.properties()
        .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    return new GenericAppenderFactory(icebergTable.schema());
  }

  public static OutputFileFactory getTableOutputFileFactory(Table icebergTable, FileFormat format) {
    int partitionId = IcebergUtil.partitionId();
    return OutputFileFactory.builderFor(icebergTable, partitionId, 1L)
        .defaultSpec(icebergTable.spec())
        .format(format)
        .build();
  }

  public static int partitionId() {
    return Math.abs(UUID.randomUUID().hashCode());
  }
}
