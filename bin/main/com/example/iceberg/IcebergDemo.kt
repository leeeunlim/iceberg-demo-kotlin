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

/**
 * Apache Iceberg를 HadoopCatalog와 함께 사용하는 데모
 */
class IcebergDemo {
    companion object {
        // 웨어하우스 위치
        private const val WAREHOUSE_LOCATION = "warehouse"

        /**
         * 로컬 HadoopCatalog를 생성합니다.
         */
        fun createLocalCatalog(): HadoopCatalog {
            val catalog = HadoopCatalog()
            val hadoopConf = Configuration().apply {
                set("fs.defaultFS", "file:///")
                set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            }
            catalog.setConf(hadoopConf)
            catalog.initialize("demo", mapOf("warehouse" to WAREHOUSE_LOCATION))
            return catalog
        }

        /**
         * 테이블을 생성하거나 기존 테이블을 로드합니다.
         */
        fun createTable(catalog: Catalog): Table {
            val tableId = TableIdentifier.of("default", "demo_table")
            
            return try {
                // 테이블 생성 시도
                val schema = Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.required(3, "age", Types.IntegerType.get())
                )
                catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), mapOf())
            } catch (e: AlreadyExistsException) {
                // 테이블이 이미 존재하면 로드
                catalog.loadTable(tableId)
            }
        }

        /**
         * Parquet 파일에 레코드를 쓰는 유틸리티 함수
         */
        fun writeParquetFile(records: List<Record>, outputFile: OutputFile, schema: Schema, properties: Map<String, String>) {
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

        /**
         * Parquet 파일에서 레코드를 읽는 유틸리티 함수
         */
        fun readParquetFile(inputFile: InputFile, schema: Schema): List<Record> {
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

        /**
         * 테이블에 초기 데이터를 삽입합니다.
         */
        fun insertData(table: Table, hadoopConf: Configuration) {
            // 샘플 레코드 생성
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

            // Parquet 파일에 레코드 쓰기
            val dataFile = "${table.location()}/data/data-${UUID.randomUUID()}.parquet"
            val outputFile = HadoopOutputFile.fromPath(Path(dataFile), hadoopConf)
            writeParquetFile(records, outputFile, table.schema(), table.properties())

            // 데이터 파일 생성
            val dataFileObj = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(dataFile)
                .withFileSizeInBytes(File(dataFile).length())
                .withRecordCount(records.size.toLong())
                .build()

            // 트랜잭션 시작
            val transaction = table.newTransaction()

            // 테이블에 데이터 파일 추가
            transaction.newFastAppend()
                .appendFile(dataFileObj)
                .commit()

            // 트랜잭션 커밋
            transaction.commitTransaction()
        }

        /**
         * 테이블에 추가 데이터를 삽입합니다.
         */
        fun insertMoreData(table: Table, hadoopConf: Configuration) {
            // 샘플 레코드 생성
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

            // Parquet 파일에 레코드 쓰기
            val dataFile = "${table.location()}/data/data-${UUID.randomUUID()}.parquet"
            val outputFile = HadoopOutputFile.fromPath(Path(dataFile), hadoopConf)
            writeParquetFile(records, outputFile, table.schema(), table.properties())

            // 데이터 파일 생성
            val dataFileObj = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(dataFile)
                .withFileSizeInBytes(File(dataFile).length())
                .withRecordCount(records.size.toLong())
                .build()

            // 트랜잭션 시작
            val transaction = table.newTransaction()

            // 테이블에 데이터 파일 추가
            transaction.newFastAppend()
                .appendFile(dataFileObj)
                .commit()

            // 트랜잭션 커밋
            transaction.commitTransaction()
        }

        /**
         * 테이블에서 데이터를 쿼리합니다.
         */
        fun queryData(table: Table, hadoopConf: Configuration) {
            logger.info("테이블에서 데이터 쿼리:")
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

        /**
         * 테이블 히스토리를 표시합니다.
         */
        fun showTableHistory(table: Table) {
            logger.info("테이블 히스토리:")
            table.history().forEach { historyEntry ->
                logger.info("Snapshot ID: {}, Timestamp: {}", historyEntry.snapshotId(), historyEntry.timestampMillis())
            }
        }

        /**
         * 데모를 실행하는 메인 함수
         */
        fun runExample() {
            try {
                // Hadoop 설정 생성
                val hadoopConf = Configuration().apply {
                    set("fs.defaultFS", "file:///")
                }

                // 로컬 카탈로그 생성
                val catalog = createLocalCatalog()

                // 테이블 생성
                val table = createTable(catalog)

                // 초기 데이터 삽입
                insertData(table, hadoopConf)

                // 추가 데이터 삽입
                insertMoreData(table, hadoopConf)

                // 데이터 쿼리
                queryData(table, hadoopConf)

                // 테이블 히스토리 표시
                showTableHistory(table)

            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}

/**
 * HadoopCatalog 데모를 실행하는 메인 함수
 */
fun main() {
    IcebergDemo.runExample()
}