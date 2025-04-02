package com.example.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.*
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.hadoop.HadoopOutputFile
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.parquet.schema.MessageType
import org.apache.iceberg.exceptions.AlreadyExistsException

private val logger = LoggerFactory.getLogger("com.example.iceberg.IcebergDemo")

private val WAREHOUSE_LOCATION = "warehouse"

private fun createLocalCatalog(): HadoopCatalog {
    val catalog = HadoopCatalog()
    val hadoopConf = Configuration().apply {
        set("fs.defaultFS", "file:///")
        set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    }
    catalog.setConf(hadoopConf)
    catalog.initialize("demo", mapOf("warehouse" to WAREHOUSE_LOCATION))
    return catalog
}

private fun createTable(catalog: Catalog): Table {
    val tableId = TableIdentifier.of("default", "demo_table")
    
    return try {
        // Try to create a new table
        val schema = Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get())
        )
        catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), mapOf())
    } catch (e: AlreadyExistsException) {
        // If table already exists, load it
        catalog.loadTable(tableId)
    }
}

private fun writeParquetFile(records: List<Record>, outputFile: OutputFile, schema: Schema, properties: Map<String, String>) {
    val writer = Parquet.write(outputFile)
        .schema(schema)
        .setAll(properties)
        .createWriterFunc { messageType -> 
            GenericParquetWriter.buildWriter(messageType) as ParquetValueWriter<Record>
        }
        .build<Record>()

    try {
        records.forEach { record ->
            writer.add(record)
        }
    } finally {
        writer.close()
    }
}

private fun readParquetFile(inputFile: InputFile, schema: Schema): List<Record> {
    val reader = Parquet.read(inputFile)
        .project(schema)
        .createReaderFunc { messageType -> 
            GenericParquetReaders.buildReader(schema, messageType) as ParquetValueReader<Record>
        }
        .build<Record>()

    val records = mutableListOf<Record>()
    try {
        reader.forEach { record ->
            records.add(record)
        }
    } finally {
        reader.close()
    }
    return records
}

private fun insertData(table: Table, hadoopConf: Configuration) {
    // Create sample records
    val records = listOf(
        GenericRecord.create(table.schema()).apply {
            setField("id", 1L)
            setField("name", "John Doe")
            setField("age", 30)
        },
        GenericRecord.create(table.schema()).apply {
            setField("id", 2L)
            setField("name", "Jane Smith")
            setField("age", 25)
        },
        GenericRecord.create(table.schema()).apply {
            setField("id", 3L)
            setField("name", "Bob Johnson")
            setField("age", 35)
        }
    )

    // Write records to a Parquet file
    val dataFile = "${table.location()}/data/data-${UUID.randomUUID()}.parquet"
    val outputFile = HadoopOutputFile.fromPath(Path(dataFile), hadoopConf)
    writeParquetFile(records, outputFile, table.schema(), table.properties())

    // Create a data file
    val dataFileObj = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(dataFile)
        .withFileSizeInBytes(File(dataFile).length())
        .withRecordCount(records.size.toLong())
        .build()

    // Start a transaction
    val transaction = table.newTransaction()

    // Append the data file to the table
    transaction.newFastAppend()
        .appendFile(dataFileObj)
        .commit()

    // Commit the transaction
    transaction.commitTransaction()
}

private fun insertMoreData(table: Table, hadoopConf: Configuration) {
    // Create sample records
    val records = listOf(
        GenericRecord.create(table.schema()).apply {
            setField("id", 4L)
            setField("name", "Alice Brown")
            setField("age", 28)
        },
        GenericRecord.create(table.schema()).apply {
            setField("id", 5L)
            setField("name", "Charlie Wilson")
            setField("age", 32)
        }
    )

    // Write records to a Parquet file
    val dataFile = "${table.location()}/data/data-${UUID.randomUUID()}.parquet"
    val outputFile = HadoopOutputFile.fromPath(Path(dataFile), hadoopConf)
    writeParquetFile(records, outputFile, table.schema(), table.properties())

    // Create a data file
    val dataFileObj = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(dataFile)
        .withFileSizeInBytes(File(dataFile).length())
        .withRecordCount(records.size.toLong())
        .build()

    // Start a transaction
    val transaction = table.newTransaction()

    // Append the data file to the table
    transaction.newFastAppend()
        .appendFile(dataFileObj)
        .commit()

    // Commit the transaction
    transaction.commitTransaction()
}

private fun queryData(table: Table, hadoopConf: Configuration) {
    logger.info("Querying data from table:")
    table.newScan()
        .planFiles()
        .forEach { fileScanTask ->
            val inputFile = HadoopInputFile.fromLocation(fileScanTask.file().path().toString(), hadoopConf)
            val records = readParquetFile(inputFile, table.schema())
            records.forEach { record ->
                logger.info("Record: id={}, name={}, age={}",
                    record.getField("id"),
                    record.getField("name"),
                    record.getField("age"))
            }
        }
}

private fun showTableHistory(table: Table) {
    logger.info("Table history:")
    table.history().forEach { historyEntry ->
        logger.info("Snapshot ID: {}, Timestamp: {}", historyEntry.snapshotId(), historyEntry.timestampMillis())
    }
}

fun main() {
    try {
        // Create Hadoop configuration
        val hadoopConf = Configuration().apply {
            set("fs.defaultFS", "file:///")
        }

        // Create a local catalog
        val catalog = createLocalCatalog()

        // Create the table
        val table = createTable(catalog)

        // Insert initial data
        insertData(table, hadoopConf)

        // Insert more data
        insertMoreData(table, hadoopConf)

        // Query the data
        queryData(table, hadoopConf)

        // Show table history
        showTableHistory(table)

    } catch (e: Exception) {
        e.printStackTrace()
    }
}