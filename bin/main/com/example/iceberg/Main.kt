package com.example.iceberg

import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("com.example.iceberg.Main")

/**
 * 메인 애플리케이션 클래스
 * 
 * 이 예제는 두 가지 서로 다른 카탈로그 구현을 보여줍니다:
 * 1. HadoopCatalog: 로컬 파일 시스템 기반 카탈로그 (기본값)
 * 2. RESTCatalog: REST API를 통해 접근하는 카탈로그 (시뮬레이션)
 * 
 * 환경 변수 CATALOG_TYPE을 통해 사용할 카탈로그를 선택할 수 있습니다:
 * - "hadoop" (기본값): HadoopCatalog
 * - "rest": 시뮬레이션된 REST Catalog
 */
fun main(args: Array<String>) {
    val catalogType = System.getenv("CATALOG_TYPE") ?: "hadoop"
    
    logger.info("선택된 카탈로그 유형: {}", catalogType)
    
    when (catalogType.lowercase()) {
        "rest" -> {
            logger.info("REST Catalog 데모를 실행합니다...")
            logger.info("(이 예제에서는 실제 REST 서버 없이 시뮬레이션합니다)")
            try {
                RestCatalogDemo.runExample()
            } catch (e: Exception) {
                logger.error("REST Catalog 데모 실행 중 오류 발생: {}", e.message)
                e.printStackTrace()
            }
        }
        else -> {
            logger.info("HadoopCatalog 데모를 실행합니다...")
            try {
                // IcebergDemo 클래스의 runExample 메서드 호출
                IcebergDemo.runExample()
            } catch (e: Exception) {
                logger.error("HadoopCatalog 데모 실행 중 오류 발생: {}", e.message)
                e.printStackTrace()
            }
        }
    }
} 