/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.iceberg.tableoperator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.debezium.server.iceberg.RecordConverter;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
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
 */
public class IcebergTableOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  private static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY = 
      ImmutableMap.of(Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  
  private String cdcSourceTsMsField = "__source_ts_ms";
  private String cdcOpField = "__op";
  private boolean allowFieldAddition = true;
  private boolean createIdentifierFields = true;
  private boolean upsert = true;
  private IcebergTableWriterFactory writerFactory;

  public IcebergTableOperator() {
    createIdentifierFields = true;
    writerFactory = new IcebergTableWriterFactory();
    writerFactory.keepDeletes = true;
    writerFactory.upsert = true;
    allowFieldAddition = true;
    upsert = true;
    cdcOpField = "__op";
    cdcSourceTsMsField = "__source_ts_ms";
  }

  public IcebergTableOperator(boolean upsert_records) {
    createIdentifierFields = true;
    writerFactory = new IcebergTableWriterFactory();
    writerFactory.keepDeletes = true;
    writerFactory.upsert = upsert_records;
    allowFieldAddition = true;
    upsert = upsert_records;
    cdcOpField = "__op";
    cdcSourceTsMsField = "__source_ts_ms";
  }

  protected List<RecordConverter> deduplicateBatch(List<RecordConverter> events) {
    ConcurrentHashMap<JsonNode, RecordConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e -> {
      if (e.key() == null || e.key().isNull()) {
        throw new RuntimeException("Cannot deduplicate data with null key! destination:'" + e.destination() + "' event: '" + e.value().toString() + "'");
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
        LOGGER.error("Failed to deduplicate events Caused by: {}", ex.getMessage());
        throw new RuntimeException(ex);
      }
    });

    return new ArrayList<>(deduplicatedEvents.values());
  }

  private int compareByTsThenOp(RecordConverter lhs, RecordConverter rhs) {
    // Define cdc fields
    final String cdcTsField = cdcSourceTsMsField;
    try {
      // compare by timestamp
      long lhsTs = lhs.cdcSourceTsMsValue(cdcTsField);
      long rhsTs = rhs.cdcSourceTsMsValue(cdcTsField);
      int tsCompare = Long.compare(lhsTs, rhsTs);
      if (tsCompare != 0) {
        return tsCompare;
      }

      // If timestamps are equal, compare by operation type priority
      // (op priority DELETE > UPDATE > READ > INSERT)
      final String cdcOpField = this.cdcOpField;
      Operation lhsOp = lhs.cdcOpValue(cdcOpField);
      Operation rhsOp = rhs.cdcOpValue(cdcOpField);
      return Integer.compare(CDC_OPERATION_PRIORITY.get(lhsOp), CDC_OPERATION_PRIORITY.get(rhsOp));
    } catch (Exception e) {
      // if any error occurs due to missing fields, etc.,
      // keep the existing message as-is, i.e. return a negative number
      LOGGER.warn("Failed to compare CDC events for deduplication: {}", e.getMessage(), e);
      return -1;
    }
  }

  private void applyFieldAddition(Table icebergTable, Schema newSchema) {
    if (!allowFieldAddition) {
      return;
    }

    // Using UpdateSchema, add new columns to the schema
    UpdateSchema updateSchema = icebergTable.updateSchema();

    for (org.apache.iceberg.types.Types.NestedField newCol : newSchema.columns()) {
      if (icebergTable.schema().findField(newCol.name().toLowerCase()) == null) {
        LOGGER.warn("Adding new column: {} to Table: {}", newCol.name(), icebergTable.name());
        updateSchema = updateSchema.addColumn(newCol.name(), newCol.type());
      }
    }

    // Apply the schema changes if any changes were made
    boolean hasChanges = false;
    for (org.apache.iceberg.types.Types.NestedField newCol : newSchema.columns()) {
      if (icebergTable.schema().findField(newCol.name().toLowerCase()) == null) {
        hasChanges = true;
        break;
      }
    }
    if (!hasChanges) {
      return;
    }

    try {
      updateSchema.commit();
    } catch (Exception e) {
      LOGGER.error("Failed to add new columns to table schema! Message: {}", e.getMessage());
    }
  }

  public void addToTable(Table icebergTable, List<RecordConverter> events) {
    List<RecordConverter> uniqueEvents = events;
    if (upsert) {
      uniqueEvents = deduplicateBatch(events);
    }

    try {
      LOGGER.info("Adding to table = {}", icebergTable);
      // Determine whether to use upsert(merge) or append
      if (upsert) {
        // For upsert, process records by schema (due to schema evolution)
        addToTablePerSchema(icebergTable, uniqueEvents);
      } else {
        // For append only, just convert all records to appends
        Schema schema = icebergTable.schema();
        
        List<RecordWrapper> data = uniqueEvents.stream()
            .map(e -> e.convertAsAppend(schema))
            .collect(Collectors.toList());

        // Get the writer for this schema
        BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);
        // Write all records
        for (RecordWrapper record : data) {
          writer.write(record);
        }

        // Commit files
        WriteResult result = writer.complete();
        AppendFiles appendFiles = icebergTable.newAppend();
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        appendFiles.commit();
        writer.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to write to table: " + icebergTable.name(), e);
    }
  }

  private void addToTablePerSchema(Table icebergTable, List<RecordConverter> events) {
    try {
      // Implement logic for each event
      Schema schema = icebergTable.schema();
      RecordConverter sampleEvent = events.get(0);
      
      // Check for schema evolution
      if (allowFieldAddition) {
        Schema newSchema = sampleEvent.icebergSchema(createIdentifierFields);
        applyFieldAddition(icebergTable, newSchema);
      }

      // Process all the events and convert to RecordWrapper objects
      List<RecordWrapper> recordsToWrite = new ArrayList<>();
      for (RecordConverter event : events) {
        RecordWrapper wrapper = event.convert(schema, cdcOpField);
        recordsToWrite.add(wrapper);
      }

      // Init operation counters
      Map<Operation, Long> opCounts = recordsToWrite.stream()
          .collect(Collectors.groupingBy(RecordWrapper::op, Collectors.counting()));
      
      LOGGER.info("Record counts by operation: {}", opCounts);

      // Get writer for the main schema
      BaseTaskWriter<Record> writer = writerFactory.create(icebergTable);

      // Create a RowDelta to handle inserts, updates, and deletes
      RowDelta rowDelta = icebergTable.newRowDelta();

      // Process each record based on operation type
      for (RecordWrapper wrapper : recordsToWrite) {
        switch (wrapper.op()) {
          case INSERT:
          case READ:
          case UPDATE:
            writer.write(wrapper);
            break;
          case DELETE:
            if (writerFactory.keepDeletes) {
              writer.write(wrapper);
            } else {
              // If we're not keeping deletes, you would need deletion file logic here
              // This is simplified in this version
              // For a real implementation, you would create position delete files
              LOGGER.info("Delete operation for record: {}", wrapper);
            }
            break;
        }
      }

      // Complete writing and get the results
      WriteResult result = writer.complete();
      
      // Add data files to the rowDelta
      Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
      
      // Commit changes
      rowDelta.commit();
      writer.close();
    } catch (IOException e) {
      LOGGER.error("Error writing to table: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to write to table: " + icebergTable.name(), e);
    }
  }
}
