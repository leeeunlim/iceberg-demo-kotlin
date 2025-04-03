package com.example.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.*
import org.apache.iceberg.aws.AwsClientFactories
import org.apache.iceberg.aws.AwsClientFactory
import org.apache.iceberg.aws.AwsProperties
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.hadoop.HadoopOutputFile
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import java.net.URI
import org.apache.iceberg.exceptions.AlreadyExistsException

private val logger = LoggerFactory.getLogger("com.example.iceberg.RestCatalogDemo")

/**
 * 이 예제는 시뮬레이션된 REST 카탈로그 방식을 보여줍니다.
 * 실제 REST Catalog 사용을 위해서는 REST 서버가 필요하며, 앱에 따라 서로 다른 구현이 필요할 수 있습니다.
 * 
 * 이 예제는 실제 REST Catalog 서버가 없으므로 HadoopCatalog로 대체하여 작동 방식을 시뮬레이션합니다.
 */
class RestCatalogDemo {
    companion object {
        private const val WAREHOUSE_LOCATION = "warehouse/rest"
        private const val REST_URI = "http://localhost:8181"
        
        /**
         * REST Catalog를 위한 카탈로그를 생성합니다.
         * 
         * 주의: 실제 환경에서는 REST 서버에 맞는 RESTCatalog 구현체를 사용해야 합니다.
         * 이 예제에서는 HadoopCatalog를 대체 사용합니다.
         */
        private fun createRestCatalog(): Catalog {
            logger.info("REST Catalog 시뮬레이션 생성 중...")
            
            // 실제 환경에서는 REST Catalog 구현체를 사용해야 합니다.
            // 여기서는 HadoopCatalog를 대체 사용합니다.
            val catalog = HadoopCatalog()
            val hadoopConf = Configuration().apply {
                set("fs.defaultFS", "file:///")
                set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            }
            catalog.setConf(hadoopConf)
            
            // 카탈로그 초기화
            catalog.initialize("rest-catalog", mapOf("warehouse" to WAREHOUSE_LOCATION))
            
            logger.info("REST Catalog 시뮬레이션이 생성되었습니다. (실제로는 HadoopCatalog 사용)")
            return catalog
        }
        
        /**
         * REST Catalog를 사용하여 테이블을 생성하거나 로드합니다.
         */
        private fun createOrLoadTable(catalog: Catalog): Table {
            val tableId = TableIdentifier.of("default", "rest_demo_table")
            
            return try {
                // 테이블 스키마 생성
                val schema = Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.required(3, "age", Types.IntegerType.get())
                )
                
                // 테이블 생성 시도
                logger.info("테이블 생성 시도: {}", tableId)
                catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), 
                    mapOf("write.parquet.compression-codec" to "zstd"))
            } catch (e: AlreadyExistsException) {
                // 테이블이 이미 존재하면 로드
                logger.info("테이블이 이미 존재합니다. 로드 중: {}", tableId)
                catalog.loadTable(tableId)
            }
        }
        
        /**
         * Parquet 파일에 레코드를 작성합니다.
         */
        private fun writeParquetFile(records: List<GenericRecord>, outputFile: OutputFile, schema: Schema, properties: Map<String, String>) {
            val writer = Parquet.write(outputFile)
                .schema(schema)
                .setAll(properties)
                .createWriterFunc { messageType -> 
                    GenericParquetWriter.buildWriter(messageType) as ParquetValueWriter<GenericRecord>
                }
                .build<GenericRecord>()

            try {
                records.forEach { record ->
                    writer.add(record)
                }
            } finally {
                writer.close()
            }
        }
        
        /**
         * Parquet 파일에서 레코드를 읽습니다.
         */
        private fun readParquetFile(inputFile: InputFile, schema: Schema): List<GenericRecord> {
            val reader = Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc { messageType -> 
                    GenericParquetReaders.buildReader(schema, messageType) as ParquetValueReader<GenericRecord>
                }
                .build<GenericRecord>()

            val records = mutableListOf<GenericRecord>()
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
         * 테이블에 데이터를 삽입합니다.
         */
        private fun insertData(table: Table, hadoopConf: Configuration) {
            logger.info("테이블에 데이터 삽입 중...")
            
            // 샘플 레코드 생성
            val records = listOf(
                GenericRecord.create(table.schema()).apply {
                    setField("id", 101L)
                    setField("name", "Sarah Lee")
                    setField("age", 29)
                },
                GenericRecord.create(table.schema()).apply {
                    setField("id", 102L)
                    setField("name", "Michael Park")
                    setField("age", 34)
                }
            )

            // Parquet 파일에 레코드 작성
            val dataFile = "${table.location()}/data/restdata-${UUID.randomUUID()}.parquet"
            val outputFile = HadoopOutputFile.fromPath(Path(dataFile), hadoopConf)
            writeParquetFile(records, outputFile, table.schema(), table.properties())

            // 데이터 파일 객체 생성
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
            logger.info("데이터 삽입 완료")
        }
        
        /**
         * 테이블 데이터를 쿼리합니다.
         */
        private fun queryData(table: Table, hadoopConf: Configuration) {
            logger.info("REST 카탈로그 테이블에서 데이터 쿼리:")
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
        private fun showTableHistory(table: Table) {
            logger.info("테이블 히스토리:")
            table.history().forEach { historyEntry ->
                logger.info("Snapshot ID: {}, Timestamp: {}", 
                    historyEntry.snapshotId(), 
                    historyEntry.timestampMillis())
            }
        }
        
        /**
         * REST Catalog 예제를 실행합니다.
         * 
         * 주의: 이 예제는 시뮬레이션된 것이며, 실제 REST 서버는 필요하지 않습니다.
         * 실제 REST 서버를 사용하는 경우 적절한 REST 클라이언트와 인증 구성이 필요합니다.
         */
        fun runExample() {
            try {
                logger.info("REST Catalog 데모 시작...")
                
                // Hadoop 설정 생성
                val hadoopConf = Configuration().apply {
                    set("fs.defaultFS", "file:///")
                }

                // REST Catalog 생성
                val catalog = createRestCatalog()
                logger.info("REST Catalog가 성공적으로 생성되었습니다.")

                // 테이블 생성 또는 로드
                val table = createOrLoadTable(catalog)
                logger.info("테이블이 성공적으로 생성되거나 로드되었습니다: {}", table.name())

                // 데이터 삽입
                insertData(table, hadoopConf)

                // 데이터 쿼리
                queryData(table, hadoopConf)

                // 테이블 히스토리 표시
                showTableHistory(table)
                
                logger.info("REST Catalog 데모 완료!")

            } catch (e: Exception) {
                logger.error("REST Catalog 데모 실행 중 오류 발생", e)
            }
        }
    }
}

/**
 * REST Catalog 예제를 직접 실행하는 메인 함수
 */
fun main() {
    RestCatalogDemo.runExample()
} 