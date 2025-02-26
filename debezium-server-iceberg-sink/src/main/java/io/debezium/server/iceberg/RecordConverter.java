/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.server.iceberg.tableoperator.Operation;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Converts JSON event to Iceberg GenericRecord. Extracts event schema and key fields.
 * Converts event schema to Iceberg Schema.
 */
public class RecordConverter {

  protected static final ObjectMapper mapper = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(RecordConverter.class);
  public static final List<String> TS_MS_FIELDS = List.of("__ts_ms", "__source_ts_ms");
  static final boolean eventsAreUnwrapped = true;
  protected final String destination;
  protected final byte[] valueData;
  protected final byte[] keyData;
  private JsonNode value;
  private JsonNode key;

  public RecordConverter(String destination, byte[] valueData, byte[] keyData) {
    this.destination = destination;
    this.valueData = valueData;
    this.keyData = keyData;
  }

  public JsonNode key() {
    if (key == null && keyData != null) {
      try {
        key = mapper.readTree(keyData);
      } catch (IOException e) {
        throw new RuntimeException("Error deserializing key data", e);
      }
    }
    return key;
  }

  public JsonNode value() {
    if (value == null && valueData != null) {
      try {
        value = mapper.readTree(valueData);
      } catch (IOException e) {
        throw new RuntimeException("Error deserializing value data", e);
      }
    }
    return value;
  }

  public Long cdcSourceTsMsValue(String cdcSourceTsMsField) {
    final JsonNode element = value().get(cdcSourceTsMsField);
    if (element == null) {
      throw new RuntimeException("Field '" + cdcSourceTsMsField + "' not found in JSON object: " + value());
    }

    try {
      return element.asLong();
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error converting field '" + cdcSourceTsMsField + "' value '" + element + "' to Long: " + e.getMessage(), e);
    }
  }

  public Operation cdcOpValue(String cdcOpField) {
    if (!value().has(cdcOpField)) {
      throw new RuntimeException("The value for field `" + cdcOpField + "` is missing. " +
          "This field is required when updating or deleting data, when running in upsert mode."
      );
    }

    final String opFieldValue = value().get(cdcOpField).asText("c");

    switch (opFieldValue) {
      case "u": 
        return Operation.UPDATE;
      case "d": 
        return Operation.DELETE;
      case "r": 
        return Operation.READ;
      case "c": 
        return Operation.INSERT;
      case "i": 
        return Operation.INSERT;
      default:
        throw new RuntimeException("Unexpected `" + cdcOpField + "=" + opFieldValue + "` operation value received, expecting one of ['u','d','r','c', 'i']");
    }
  }

  public SchemaConverter schemaConverter() {
    try {
      return new SchemaConverter(mapper.readTree(valueData).get("schema"), keyData == null ? null : mapper.readTree(keyData).get("schema"));
    } catch (IOException e) {
      throw new RuntimeException("Failed to get event schema", e);
    }
  }

  /**
   * Checks if the current message represents a schema change event.
   * Schema change events are identified by the presence of "ddl", "databaseName", and "tableChanges" fields.
   *
   * @return True if it's a schema change event, false otherwise.
   */
  private boolean isSchemaChangeEvent() {
    return value().has("ddl") && value().has("databaseName") && value().has("tableChanges");
  }

  /**
   * Converts the JSON schema to an Iceberg schema.
   *
   * @param createIdentifierFields Whether to include identifier fields in the Iceberg schema.
   *                               Identifier fields are typically used for primary keys and are
   *                               required for upsert/merge operations.  They should be *excluded*
   *                               for schema change topic messages to ensure append-only mode.
   * @return The Iceberg schema.
   */
  public Schema icebergSchema(boolean createIdentifierFields) {
    // Check if the message is a schema change event (DDL statement).
    // Schema change events are identified by the presence of "ddl", "databaseName", and "tableChanges" fields.
    if (isSchemaChangeEvent()) {
      LOGGER.warn("Schema change topic detected. Creating Iceberg schema without identifier fields for append-only mode.");
      return schemaConverter().icebergSchema(false); // Force no identifier fields for schema changes
    }

    // For normal events, use the provided createIdentifierFields parameter
    return schemaConverter().icebergSchema(createIdentifierFields);
  }

  public String destination() {
    return destination;
  }

  public RecordWrapper convertAsAppend(Schema schema) {
    GenericRecord record = RecordConverter.convert(schema.asStruct(), value());
    return new RecordWrapper(record, Operation.INSERT);
  }

  public RecordWrapper convert(Schema schema, String cdcOpField) {
    GenericRecord record = RecordConverter.convert(schema.asStruct(), value());
    return new RecordWrapper(record, cdcOpValue(cdcOpField));
  }

  private static GenericRecord convert(Types.StructType tableFields, JsonNode data) {
    GenericRecord record = GenericRecord.create(tableFields);

    for (Types.NestedField field : tableFields.fields()) {
      String fieldName = field.name();
      JsonNode node = data.get(fieldName);
      if (node != null && !node.isNull()) {
        record.setField(fieldName, RecordConverter.jsonValToIcebergVal(field, node));
      }
    }
    return record;
  }

  private static Object jsonValToIcebergVal(Types.NestedField field, JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }

    try {
      switch (field.type().typeId()) {
        case BOOLEAN:
          return node.asBoolean();
        case INTEGER:
          if (node.isTextual()) {
            return Integer.parseInt(node.asText());
          }
          return node.asInt();
        case LONG:
          if (node.isTextual()) {
            return Long.parseLong(node.asText());
          }
          return node.asLong();
        case FLOAT:
          if (node.isTextual()) {
            return Float.parseFloat(node.asText());
          }
          return node.floatValue();
        case DOUBLE:
          if (node.isTextual()) {
            return Double.parseDouble(node.asText());
          }
          return node.asDouble();
        case STRING:
          return node.asText();
        case TIMESTAMP:
          if (node.isNumber()) {
            final OffsetDateTime time = OffsetDateTime.ofInstant(Instant.ofEpochMilli(node.asLong()), ZoneOffset.UTC);
            return time;
          } else if (node.isTextual()) {
            if (node.asText().isEmpty()) {
              return null;
            }
            if (node.asText().chars().allMatch(Character::isDigit)) {
              final OffsetDateTime time = OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(node.asText())), ZoneOffset.UTC);
              return time;
            }
            final OffsetDateTime time = OffsetDateTime.parse(node.asText());
            return time;
          }
          return OffsetDateTime.parse(node.asText());
        case FIXED:
        case BINARY:
          if (node.isBinary()) {
            return ByteBuffer.wrap(node.binaryValue());
          } else if (node.isValueNode() && node.isTextual()) {
            return ByteBuffer.wrap(node.asText().getBytes());
          } else {
            return ByteBuffer.wrap(node.toString().getBytes());
          }
        case DECIMAL:
          if (node.isValueNode() && node.isTextual()) {
            return new java.math.BigDecimal(node.asText());
          } else if (node.isValueNode() && node.isNumber()) {
            return java.math.BigDecimal.valueOf(node.asDouble());
          }
          return null;
        case UUID:
          return UUID.fromString(node.asText());
        case LIST:
          List<Object> convertedList = new ArrayList<>();
          Types.NestedField elementField = Types.NestedField.of(
              1000, // Arbitrary ID for the list element
              false,
              "element",
              ((Types.ListType) field.type()).elementType()
          );
          for (final JsonNode jsonArrayItem : node) {
            Object convertedElement = jsonValToIcebergVal(elementField, jsonArrayItem);
            convertedList.add(convertedElement);
          }
          return convertedList;
        case MAP:
          Map<String, Object> convertedMap = new HashMap<>();
          Types.NestedField valueField = Types.NestedField.of(
              1001, // Arbitrary ID for the map value
              true,
              "value",
              ((Types.MapType) field.type()).valueType()
          );
          node.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode val = entry.getValue();
            convertedMap.put(key, jsonValToIcebergVal(valueField, val));
          });
          return convertedMap;
        case STRUCT:
          return convert((Types.StructType) field.type(), node);
        default:
          return node.asText();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to convert field '{}' with value '{}' to Iceberg type '{}'. Error: '{}'", field.name(), node, field.type().typeId(), e.getMessage());
      throw new RuntimeException("Failed to convert field '" + field.name() + "' with value '" + node.toString() + "' to Iceberg type '" + field.type().toString() + "' !");
    }
  }

  public static class SchemaConverter {
    private final JsonNode valueSchema;
    private final JsonNode keySchema;

    SchemaConverter(JsonNode valueSchema, JsonNode keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    protected JsonNode valueSchema() {
      return valueSchema;
    }

    protected JsonNode keySchema() {
      return keySchema;
    }

    /**
     * Convert a debezium field to iceberg field
     */
    private static RecordSchemaData debeziumFieldToIcebergField(JsonNode fieldSchema, String fieldName, RecordSchemaData schemaData, JsonNode keySchemaNode) {
      String fieldType = fieldSchema.get("type").asText().toUpperCase();
      if (!schemaData.getFields().containsKey(fieldName)) {
        Types.NestedField nestedField;
        if (fieldName.equalsIgnoreCase("__commit_offset") || fieldName.equalsIgnoreCase("__commit_timestamp")) {
          // int64 as LONG
          nestedField = Types.NestedField.optional(schemaData.getFields().size() + 1, fieldName, Types.LongType.get());
        } else if (TS_MS_FIELDS.contains(fieldName)) {
          // timestamp as TIMESTAMP
          nestedField = Types.NestedField.optional(schemaData.getFields().size() + 1, fieldName, Types.TimestampType.withZone());
        } else if (fieldName.startsWith("__") && (fieldType.equals("STRING") || fieldType.equals("BYTES"))) {
          // remaining metadata fields as string
          nestedField = Types.NestedField.optional(schemaData.getFields().size() + 1, fieldName, Types.StringType.get());
        } else {
          nestedField = Types.NestedField.optional(schemaData.getFields().size() + 1, fieldName, RecordConverter.SchemaConverter.icebergPrimitiveField(fieldName, fieldType));
        }
        schemaData.getFields().put(fieldName, nestedField);
        // check if current field is PK field
        schemaData.getIdFields().remove(fieldName);
        if (keySchemaNode != null) {
          JsonNode keyFieldNode = findNodeFieldByName(fieldName, keySchemaNode);
          if (keyFieldNode != null) {
            schemaData.getIdFields().add(fieldName);
          }
        }
      }
      return schemaData;
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueSchema(), keySchema());
    }

    private static JsonNode getNodeFieldsArray(JsonNode node) {
      if (node.has("fields")) {
        return node.get("fields");
      } else if (node.has("schema")) {
        return RecordConverter.SchemaConverter.getNodeFieldsArray(node.get("schema"));
      }
      return null;
    }

    private static JsonNode findNodeFieldByName(String fieldName, JsonNode node) {
      JsonNode fieldsNode = RecordConverter.SchemaConverter.getNodeFieldsArray(node);
      if (fieldsNode != null) {
        for (final JsonNode fieldSchema : fieldsNode) {
          if (fieldSchema.get("field").asText().equals(fieldName)) {
            return fieldSchema;
          }
        }
      }
      return null;
    }

    /**
     * Get iceberg schema fields from debezium schema
     */
    private static RecordSchemaData icebergSchemaFields(JsonNode schemaNode, JsonNode keySchemaNode, RecordSchemaData schemaData) {
      JsonNode fieldsNode = RecordConverter.SchemaConverter.getNodeFieldsArray(schemaNode);
      if (fieldsNode != null) {
        for (final JsonNode fieldSchema : fieldsNode) {
          RecordConverter.SchemaConverter.debeziumFieldToIcebergField(fieldSchema, fieldSchema.get("field").asText(), schemaData, keySchemaNode);
        }
      }
      return schemaData;
    }

    private Schema icebergSchema(boolean createIdentifierFields) {
      // fallback to empty schema
      if (valueSchema() == null) {
        return new Schema(new ArrayList<>());
      }
      // final schema
      RecordSchemaData schemaData = RecordConverter.SchemaConverter.icebergSchemaFields(valueSchema, keySchema(), new RecordSchemaData());
      // Get all iceberg schema fields from key schema
      if (keySchema() != null) {
        RecordConverter.SchemaConverter.icebergSchemaFields(keySchema(), null, schemaData);
      }

      // Convert fields from map to list
      List<Types.NestedField> fieldsList = new ArrayList<>(schemaData.getFields().values());

      // For older Iceberg versions (pre-0.11.0), we may need to directly construct the Schema
      // This approach attempts to be compatible with different Iceberg versions
      Schema schema;
      
      try {
        // Try to create schema with identifier fields directly if supported
        if (createIdentifierFields && !schemaData.getIdFields().isEmpty()) {
          try {
            // Try using the constructor with identifier fields if available
            java.lang.reflect.Constructor<Schema> constructor = 
                Schema.class.getConstructor(List.class, List.class);
            schema = constructor.newInstance(fieldsList, 
                new ArrayList<>(schemaData.getIdFields()));
          } catch (NoSuchMethodException e) {
            // If that constructor isn't available, use the basic one
            schema = new Schema(fieldsList);
            LOGGER.warn("Identifier fields not supported in this Iceberg version: {}", e.getMessage());
          }
        } else {
          // Just create a basic schema without identifier fields
          schema = new Schema(fieldsList);
        }
      } catch (Exception e) {
        // Fallback to basic schema if anything fails
        schema = new Schema(fieldsList);
        LOGGER.warn("Failed to create schema with identifier fields: {}", e.getMessage());
      }

      return schema;
    }

    private static Type icebergPrimitiveField(String fieldName, String fieldType) {
      switch (fieldType) {
        case "INT8":
        case "INT16":
        case "INT32":
          return Types.IntegerType.get();
        case "INT64":
          return Types.LongType.get();
        case "FLOAT":
        case "FLOAT32":
          return Types.FloatType.get();
        case "FLOAT64":
        case "DOUBLE":
          return Types.DoubleType.get();
        case "BOOLEAN":
          return Types.BooleanType.get();
        case "STRING":
          return Types.StringType.get();
        case "BYTES":
          return Types.BinaryType.get();
        case "TIMESTAMP":
          return Types.TimestampType.withZone();
        default:
          LOGGER.error("Field Type not determined for field: '{}' with type: '{}' hence stored as string", fieldName, fieldType);
          return Types.StringType.get();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecordConverter.SchemaConverter)) {
        return false;
      }
      RecordConverter.SchemaConverter that = (RecordConverter.SchemaConverter) o;
      return Objects.equals(valueSchema(), that.valueSchema()) && Objects.equals(keySchema(), that.keySchema());
    }
  }
}
