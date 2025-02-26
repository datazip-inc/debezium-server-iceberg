package io.debezium.server.iceberg;

import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains schema data for Iceberg tables
 */
public class RecordSchemaData {
    public final Map<String, Types.NestedField> fields;
    public final Set<String> idFields;
    private int nextId;

    public RecordSchemaData() {
        this.fields = new HashMap<>();
        this.idFields = new HashSet<>();
        this.nextId = 1;
    }

    public RecordSchemaData(Map<String, Types.NestedField> fields, Set<String> idFields, int nextId) {
        this.fields = fields;
        this.idFields = idFields;
        this.nextId = nextId;
    }

    public RecordSchemaData copyKeepIdentifierFields() {
        return new RecordSchemaData(new HashMap<>(), this.idFields, this.nextId);
    }

    public int getNextId() {
        return nextId++;
    }

    public Map<String, Types.NestedField> getFields() {
        return fields;
    }

    public Set<String> getIdFields() {
        return idFields;
    }
}
