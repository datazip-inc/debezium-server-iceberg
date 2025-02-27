/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.debezium.DebeziumException;
import io.debezium.server.iceberg.RecordConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;

  public IcebergTableOperator() {
    createIdentifierFields = true;
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = true;
    allowFieldAddition = true;
    upsert = true;
    cdcOpField = "__op";
    cdcSourceTsMsField = "__source_ts_ms";
  }

  public IcebergTableOperator(boolean upsert_records) {
    createIdentifierFields = true;
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = upsert_records;
    allowFieldAddition = true;
    upsert = upsert_records;
    cdcOpField = "__op";
    cdcSourceTsMsField = "__source_ts_ms";
  }

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY = ImmutableMap.of(Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-dedup-column", defaultValue = "__source_ts_ms")
  String cdcSourceTsMsField;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-op-field", defaultValue = "__op")
  String cdcOpField;
  @ConfigProperty(name = "debezium.sink.iceberg.allow-field-addition", defaultValue = "true")
  boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.iceberg.create-identifier-fields", defaultValue = "true")
  boolean createIdentifierFields;
  @Inject
  IcebergTableWriterFactory writerFactory;

  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;

  protected List<RecordConverter> deduplicateBatch(List<RecordConverter> events) {

    ConcurrentHashMap<JsonNode, RecordConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e -> {
          if (e.key() == null || e.key().isNull()) {
            throw new DebeziumException("Cannot deduplicate data with null key! destination:'" + e.destination() + "' event: '" + e.value().toString() + "'");
          }

      try {
        // deduplicate using key(PK)
        deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
          if (this.compareByTsThenOp(oldValue, newValue) <= 0) {
            return newValue;
          } else {
            return oldValue;
          }
        });
      } catch (Exception ex) {
        throw new DebeziumException("Failed to deduplicate events", ex);
      }
        }
    );

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   * <p>
   * Forex ample a record can be updated multiple times in the source. for example insert followed by update and
   * delete. for this case we need to only pick last change event for the row.
   * <p>
   * Its used when `upsert` feature enabled (when the consumer operating non append mode) which means it should not add
   * duplicate records to target table.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(RecordConverter lhs, RecordConverter rhs) {

    int result = Long.compare(lhs.cdcSourceTsMsValue(cdcSourceTsMsField), rhs.cdcSourceTsMsValue(cdcSourceTsMsField));

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = CDC_OPERATION_PRIORITY.getOrDefault(lhs.cdcOpValue(cdcOpField), -1)
          .compareTo(
              CDC_OPERATION_PRIORITY.getOrDefault(rhs.cdcOpValue(cdcOpField), -1)
          );
    }

    return result;
  }

  /**
   * If given schema contains new fields compared to target table schema then it adds new fields to target iceberg
   * table.
   * <p>
   * Its used when allow field addition feature is enabled.
   *
   * @param icebergTable
   * @param newSchema
   */
  private void applyFieldAddition(Table icebergTable, Schema newSchema) {
    Schema existingSchema = icebergTable.schema();
    
    // Create a modified schema that preserves existing types for incompatible type changes
    List<org.apache.iceberg.types.Types.NestedField> modifiedFields = new ArrayList<>();
    boolean schemaModified = false;
    
    // First, add all existing fields to the modified schema
    for (org.apache.iceberg.types.Types.NestedField existingField : existingSchema.columns()) {
      modifiedFields.add(existingField);
    }
    
    // Then, check for new fields or type changes
    for (org.apache.iceberg.types.Types.NestedField newField : newSchema.columns()) {
      String fieldName = newField.name();
      org.apache.iceberg.types.Types.NestedField existingField = existingSchema.findField(fieldName);
      
      // If field doesn't exist in the current schema, add it
      if (existingField == null) {
        modifiedFields.add(newField);
        schemaModified = true;
        LOGGER.info("Adding new field '{}' of type {}", fieldName, newField.type());
      } 
      // If field exists with a different type
      else if (!existingField.type().equals(newField.type())) {
        // Check if we should keep the existing type to avoid schema evolution errors
        if (canSafelyUseExistingType(existingField.type(), newField.type())) {
          LOGGER.info("Preserving existing type {} for field '{}' instead of changing to {}", 
              existingField.type(), fieldName, newField.type());
          // We keep the existing field, which is already in modifiedFields
        } else {
          LOGGER.warn("Type change detected for field '{}': {} -> {}. This may cause schema evolution errors.", 
              fieldName, existingField.type(), newField.type());
          // We'll try to use the new type, but this might fail during schema update
          // Remove the existing field from modifiedFields and add the new one
          modifiedFields.removeIf(f -> f.name().equals(fieldName));
          modifiedFields.add(newField);
          schemaModified = true;
        }
      }
    }
    
    // Only update the schema if there are changes
    if (schemaModified) {
      try {
        // Create a new schema with the modified fields
        Schema modifiedSchema = new Schema(modifiedFields, newSchema.identifierFieldIds());
        
        // Apply the schema update
        UpdateSchema us = icebergTable.updateSchema();
        us = us.unionByNameWith(modifiedSchema)
               .setIdentifierFields(modifiedSchema.identifierFieldNames());
        
        LOGGER.warn("Extending schema of {}", icebergTable.name());
        us.commit();
      } catch (Exception e) {
        LOGGER.error("Failed to update schema: {}", e.getMessage(), e);
        throw new RuntimeException("Failed to update schema: " + e.getMessage(), e);
      }
    }
  }
  
  /**
   * Determines if we can safely use the existing column type instead of changing to the new type.
   * This helps avoid schema evolution errors for incompatible type changes.
   * 
   * @param existingType The existing column type in the table
   * @param newType The new type from incoming data
   * @return true if we can safely use the existing type, false otherwise
   */
  private boolean canSafelyUseExistingType(org.apache.iceberg.types.Type existingType, org.apache.iceberg.types.Type newType) {
    // If types are the same, we can use the existing type
    if (existingType.equals(newType)) {
      return true;
    }
    
    LOGGER.debug("Checking type compatibility: existing={}, new={}", existingType, newType);
    
    // Check for specific type combinations that are problematic in Iceberg
    if (existingType.isPrimitiveType() && newType.isPrimitiveType()) {
      org.apache.iceberg.types.Type.TypeID existingTypeId = existingType.typeId();
      org.apache.iceberg.types.Type.TypeID newTypeId = newType.typeId();
      
      LOGGER.debug("Comparing primitive types: existing={}, new={}", existingTypeId, newTypeId);
      
      // Handle int -> float/double (not allowed by Iceberg)
      if (existingTypeId == org.apache.iceberg.types.Type.TypeID.INTEGER &&
          (newTypeId == org.apache.iceberg.types.Type.TypeID.FLOAT || 
           newTypeId == org.apache.iceberg.types.Type.TypeID.DOUBLE)) {
        LOGGER.info("Detected int -> float/double change, will preserve int type and convert values");
        // We'll use the existing int type and rely on TypeConverter to handle the conversion
        return true;
      }
      
      // Handle long -> float/double (not allowed by Iceberg)
      if (existingTypeId == org.apache.iceberg.types.Type.TypeID.LONG &&
          (newTypeId == org.apache.iceberg.types.Type.TypeID.FLOAT || 
           newTypeId == org.apache.iceberg.types.Type.TypeID.DOUBLE)) {
        LOGGER.info("Detected long -> float/double change, will preserve long type and convert values");
        // We'll use the existing long type and rely on TypeConverter to handle the conversion
        return true;
      }
      
      // For other primitive type changes, check if Iceberg allows the promotion
      boolean compatible = io.debezium.server.iceberg.TypeConverter.isTypeChangeCompatible(existingTypeId, newTypeId);
      LOGGER.debug("Type change compatibility check: {} -> {} = {}", existingTypeId, newTypeId, compatible);
      return compatible;
    }
    
    // For complex types, we generally can't safely convert
    LOGGER.debug("Complex type change detected, cannot safely convert");
    return false;
  }

  /**
   * Adapts a record to match the table schema, handling type conversions as needed.
   * This ensures that incoming data with different types can be safely written to
   * the existing table schema.
   *
   * @param record The record wrapper to adapt
   * @param tableSchema The target table schema
   */
  private void adaptRecordToTableSchema(RecordWrapper record, Schema tableSchema) {
    if (record == null) {
      return;
    }
    
    LOGGER.debug("Adapting record to table schema");
    
    // Process each field in the table schema
    for (org.apache.iceberg.types.Types.NestedField field : tableSchema.columns()) {
      String fieldName = field.name();
      Object fieldValue = record.getField(fieldName);
      
      // Skip null values
      if (fieldValue == null) {
        continue;
      }
      
      // Get the expected type from the table schema
      org.apache.iceberg.types.Type expectedType = field.type();
      
      LOGGER.debug("Field '{}': value='{}' (type={}) expected type={}", 
          fieldName, fieldValue, fieldValue.getClass().getName(), expectedType);
      
      // Check if the value's type matches the expected type
      if (!isValueTypeCompatible(fieldValue, expectedType)) {
        LOGGER.info("Type mismatch for field '{}': value='{}' (type={}) expected type={}", 
            fieldName, fieldValue, fieldValue.getClass().getName(), expectedType);
        
        try {
          // Try to convert the value to the expected type
          Object convertedValue = convertValueToExpectedType(fieldValue, expectedType, fieldName);
          if (convertedValue != null) {
            // Set the converted value back to the record
            record.setField(fieldName, convertedValue);
            LOGGER.info("Converted value for field '{}' from {} ({}) to {} ({})", 
                fieldName, fieldValue, fieldValue.getClass().getSimpleName(), 
                convertedValue, convertedValue.getClass().getSimpleName());
          }
        } catch (Exception e) {
          // Log the error but continue with other fields
          LOGGER.error("Failed to convert value for field '{}': {} - {}", 
              fieldName, e.getMessage(), e.getClass().getName());
          // If the field is required, we might want to throw an exception here
          if (!field.isOptional()) {
            throw new RuntimeException("Failed to convert required field '" + fieldName + 
                "' value '" + fieldValue + "' to type " + expectedType, e);
          }
        }
      }
    }
  }
  
  /**
   * Checks if a value's type is compatible with the expected Iceberg type.
   *
   * @param value The value to check
   * @param expectedType The expected Iceberg type
   * @return true if compatible, false otherwise
   */
  private boolean isValueTypeCompatible(Object value, org.apache.iceberg.types.Type expectedType) {
    if (value == null) {
      return true;
    }
    
    org.apache.iceberg.types.Type.TypeID typeId = expectedType.typeId();
    
    switch (typeId) {
      case INTEGER:
        return value instanceof Integer;
      case LONG:
        return value instanceof Long;
      case FLOAT:
        return value instanceof Float;
      case DOUBLE:
        return value instanceof Double;
      case BOOLEAN:
        return value instanceof Boolean;
      case STRING:
        return value instanceof String;
      case DECIMAL:
        return value instanceof java.math.BigDecimal;
      case TIMESTAMP:
        return value instanceof java.time.OffsetDateTime;
      case UUID:
        return value instanceof java.util.UUID;
      case BINARY:
        return value instanceof java.nio.ByteBuffer;
      default:
        // For complex types, we'd need more sophisticated checks
        return true;
    }
  }
  
  /**
   * Converts a value to the expected Iceberg type.
   *
   * @param value The value to convert
   * @param expectedType The expected Iceberg type
   * @param fieldName Field name for logging
   * @return The converted value
   */
  private Object convertValueToExpectedType(Object value, org.apache.iceberg.types.Type expectedType, String fieldName) {
    if (value == null) {
      return null;
    }
    
    org.apache.iceberg.types.Type.TypeID typeId = expectedType.typeId();
    LOGGER.debug("Converting value '{}' of type {} to {}", value, value.getClass().getName(), typeId);
    
    switch (typeId) {
      case INTEGER:
        if (value instanceof Float) {
          float floatVal = (Float) value;
          LOGGER.debug("Attempting to convert float {} to int", floatVal);
          if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToInt(floatVal)) {
            LOGGER.info("Successfully converted float {} to int {}", floatVal, (int) floatVal);
            return (int) floatVal;
          } else {
            LOGGER.warn("Cannot safely convert float {} to int - would lose precision", floatVal);
          }
        } else if (value instanceof Double) {
          double doubleVal = (Double) value;
          LOGGER.debug("Attempting to convert double {} to int", doubleVal);
          if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToInt(doubleVal)) {
            LOGGER.info("Successfully converted double {} to int {}", doubleVal, (int) doubleVal);
            return (int) doubleVal;
          } else {
            LOGGER.warn("Cannot safely convert double {} to int - would lose precision", doubleVal);
          }
        } else if (value instanceof Long) {
          long longVal = (Long) value;
          LOGGER.debug("Attempting to convert long {} to int", longVal);
          if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToInt(longVal)) {
            LOGGER.info("Successfully converted long {} to int {}", longVal, (int) longVal);
            return (int) longVal;
          } else {
            LOGGER.warn("Cannot safely convert long {} to int - value out of range", longVal);
          }
        } else if (value instanceof String) {
          try {
            LOGGER.debug("Attempting to convert string '{}' to int", value);
            return Integer.parseInt((String) value);
          } catch (NumberFormatException e) {
            // Try parsing as double first, then convert to int if possible
            try {
              double parsed = Double.parseDouble((String) value);
              LOGGER.debug("Parsed string '{}' as double {}, checking if can convert to int", value, parsed);
              if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToInt(parsed)) {
                LOGGER.info("Successfully converted string '{}' to int {}", value, (int) parsed);
                return (int) parsed;
              } else {
                LOGGER.warn("Cannot safely convert string '{}' (parsed as {}) to int - would lose precision", value, parsed);
              }
            } catch (NumberFormatException e2) {
              LOGGER.warn("Cannot parse string '{}' as a number", value);
            }
          }
        }
        break;
        
      case LONG:
        if (value instanceof Integer) {
          return ((Integer) value).longValue();
        } else if (value instanceof Float) {
          float floatVal = (Float) value;
          if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToLong(floatVal)) {
            return (long) floatVal;
          }
        } else if (value instanceof Double) {
          double doubleVal = (Double) value;
          if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToLong(doubleVal)) {
            return (long) doubleVal;
          }
        } else if (value instanceof String) {
          try {
            return Long.parseLong((String) value);
          } catch (NumberFormatException e) {
            // Try parsing as double first, then convert to long if possible
            double parsed = Double.parseDouble((String) value);
            if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToLong(parsed)) {
              return (long) parsed;
            }
          }
        }
        break;
        
      case FLOAT:
        if (value instanceof Integer) {
          return ((Integer) value).floatValue();
        } else if (value instanceof Long) {
          return ((Long) value).floatValue();
        } else if (value instanceof Double) {
          double doubleVal = (Double) value;
          if (io.debezium.server.iceberg.TypeConverter.canSafelyConvertToFloat(doubleVal)) {
            return (float) doubleVal;
          }
        } else if (value instanceof String) {
          return Float.parseFloat((String) value);
        }
        break;
        
      case DOUBLE:
        if (value instanceof Integer) {
          return ((Integer) value).doubleValue();
        } else if (value instanceof Long) {
          return ((Long) value).doubleValue();
        } else if (value instanceof Float) {
          return ((Float) value).doubleValue();
        } else if (value instanceof String) {
          return Double.parseDouble((String) value);
        }
        break;
        
      case STRING:
        // Almost anything can be converted to string
        return value.toString();
        
      case BOOLEAN:
        if (value instanceof String) {
          String strVal = ((String) value).toLowerCase();
          if (strVal.equals("true") || strVal.equals("false")) {
            return Boolean.parseBoolean(strVal);
          } else if (strVal.equals("1") || strVal.equals("0")) {
            return strVal.equals("1");
          }
        } else if (value instanceof Number) {
          int numVal = ((Number) value).intValue();
          if (numVal == 1 || numVal == 0) {
            return numVal == 1;
          }
        }
        break;
        
      // For other types, we'd need more sophisticated conversions
    }
    
    throw new RuntimeException("Cannot convert value '" + value + "' of type " + 
        value.getClass().getSimpleName() + " to " + typeId + " for field '" + fieldName + "'");
  }

  /**
   * Adds list of events to iceberg table.
   * <p>
   * If field addition enabled then it groups list of change events by their schema first. Then adds new fields to
   * iceberg table if there is any. And then follows with adding data to the table.
   * <p>
   * New fields are detected using CDC event schema, since events are grouped by their schemas it uses single
   * event to find-out schema for the whole list of events.
   *
   * @param icebergTable
   * @param events
   */
  public void addToTable(Table icebergTable, List<RecordConverter> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!allowFieldAddition) {
      // if field additions not enabled add set of events to table
      addToTablePerSchema(icebergTable, events);
    } else {
      // First, pre-adapt all records to match the existing schema
      // This helps us identify if we can handle type conversions without schema evolution
      List<RecordConverter> adaptedEvents = new ArrayList<>();
      
      for (RecordConverter event : events) {
        try {
          // Convert the event to a record
          RecordWrapper record = (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) 
              ? event.convert(icebergTable.schema(), cdcOpField) 
              : event.convertAsAppend(icebergTable.schema());
          
          // Try to adapt the record to the existing schema
          adaptRecordToTableSchema(record, icebergTable.schema());
          
          // If adaptation succeeded, add to the adapted events list
          adaptedEvents.add(event);
        } catch (Exception e) {
          // If adaptation failed, log and continue with the original event
          LOGGER.warn("Failed to adapt record to existing schema: {}", e.getMessage());
          adaptedEvents.add(event);
        }
      }
      
      // Group events by schema
      Map<RecordConverter.SchemaConverter, List<RecordConverter>> eventsGroupedBySchema =
          adaptedEvents.stream()
              .collect(Collectors.groupingBy(RecordConverter::schemaConverter));
      LOGGER.debug("Batch got {} records with {} different schema!!", events.size(), eventsGroupedBySchema.keySet().size());

      for (Map.Entry<RecordConverter.SchemaConverter, List<RecordConverter>> schemaEvents : eventsGroupedBySchema.entrySet()) {
        // extend table schema if new fields found
        applyFieldAddition(icebergTable, schemaEvents.getValue().get(0).icebergSchema(createIdentifierFields));
        // add set of events to table
        addToTablePerSchema(icebergTable, schemaEvents.getValue());
      }
    }
  }

  /**
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private void addToTablePerSchema(Table icebergTable, List<RecordConverter> events) {
    int maxRetries = 5;
    int retryDelayMs = 2000;
    

    // TODO: Still the concurrent write exception is happening even if we run on upsert false or true. Need to fix
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      // Initialize a task writer for each attempt
      BaseTaskWriter<Record> writer = writerFactory2.create(icebergTable);
      try {
        // Refresh table state before attempting write and commit
        icebergTable.refresh();
        
        // Write all events
        for (RecordConverter e : events) {
          try {
            final RecordWrapper record = (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) 
                ? e.convert(icebergTable.schema(), cdcOpField) 
                : e.convertAsAppend(icebergTable.schema());
            
            // Ensure the record matches the table schema (handles type conversions)
            adaptRecordToTableSchema(record, icebergTable.schema());
            
            writer.write(record);
          } catch (Exception ex) {
            LOGGER.error("Failed to process record: {}", ex.getMessage(), ex);
            throw new RuntimeException("Failed to process record: " + ex.getMessage(), ex);
          }
        }

        WriteResult files = writer.complete();
        
        if (files.deleteFiles().length > 0) {
          RowDelta newRowDelta = icebergTable.newRowDelta();
          Arrays.stream(files.dataFiles()).forEach(newRowDelta::addRows);
          Arrays.stream(files.deleteFiles()).forEach(newRowDelta::addDeletes);
          newRowDelta.commit();
        } else {
          AppendFiles appendFiles = icebergTable.newAppend();
          Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
          appendFiles.commit();
        }
        
        LOGGER.info("Successfully committed {} events on attempt {}", events.size(), attempt);
        return;
        
      } catch (org.apache.iceberg.exceptions.CommitFailedException e) {
        String errorMessage = e.getMessage();
        LOGGER.warn("Commit attempt {} failed: {}", attempt, errorMessage);
        
        try {
          writer.abort();
        } catch (IOException abortEx) {
          LOGGER.warn("Failed to abort writer on attempt {}", attempt, abortEx);
        }
        
        if (attempt == maxRetries) {
          LOGGER.error("Failed to commit after {} attempts. Last error: {}", maxRetries, errorMessage);
          throw new DebeziumException("Failed to commit after " + maxRetries + " attempts", e);
        }
        
        try {
          LOGGER.info("Waiting {} ms before retry attempt {}", retryDelayMs, attempt + 1);
          Thread.sleep(retryDelayMs);
          // Exponential backoff with a maximum of 10 seconds
          retryDelayMs = Math.min(retryDelayMs * 2, 10000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new DebeziumException("Retry interrupted", ie);
        }
        
      } catch (IOException ex) {
        try {
          writer.abort();
        } catch (IOException e) {
          LOGGER.warn("Failed to abort writer", e);
        }
        throw new DebeziumException("Failed to write data to table: " + icebergTable.name(), ex);
      } catch (Exception ex) {
        try {
          writer.abort();
        } catch (IOException e) {
          LOGGER.warn("Failed to abort writer", e);
        }
        LOGGER.error("Unexpected error: {}", ex.getMessage(), ex);
        throw new DebeziumException("Unexpected error processing data for table: " + icebergTable.name(), ex);
      } finally {
        try {
          writer.close();
        } catch (IOException e) {
          LOGGER.warn("Failed to close writer", e);
        }
      }
    }
  }
}
