# Iceberg Demo (Kotlin)

이 프로젝트는 Apache Iceberg를 사용하여 데이터 레이크를 구축하는 데모 애플리케이션입니다. Kotlin으로 작성되었으며, Gradle을 빌드 도구로 사용합니다.

## 기능

- Iceberg 테이블 생성 및 관리
- Parquet 형식으로 데이터 저장
- 데이터 삽입 및 조회
- 테이블 스냅샷 관리
- 두 가지 카탈로그 구현 (HadoopCatalog와 시뮬레이션된 REST Catalog)

## 카탈로그 구현 비교

이 데모는 두 가지 카탈로그 구현을 포함합니다:

### 1. HadoopCatalog (기본값)

- 로컬 파일 시스템 기반 카탈로그
- 별도의 서버 설정 없이 단순하게 사용 가능
- 단일 애플리케이션이나 개발/테스트 환경에 적합

### 2. 시뮬레이션된 REST Catalog

- REST API 방식을 시뮬레이션한 카탈로그
- 실제 프로덕션 환경에서는 REST 서버가 필요
- 이 데모에서는 REST 서버 없이 작동하도록 시뮬레이션

## 요구사항

- JDK 11 이상
- Gradle 8.0 이상

## 빌드 및 실행

1. 프로젝트 클론:
```bash
git clone [repository-url]
cd iceberg-demo-kotlin
```

2. 프로젝트 빌드:
```bash
./gradlew build
```

3. 애플리케이션 실행 (기본 HadoopCatalog 사용):
```bash
./gradlew run
```

4. 시뮬레이션된 REST Catalog 사용:
```bash
CATALOG_TYPE=rest ./gradlew run
```

## 프로젝트 구조

```
iceberg-demo-kotlin/
├── src/
│   └── main/
│       └── kotlin/
│           └── com/
│               └── example/
│                   └── iceberg/
│                       ├── Main.kt                # 메인 진입점
│                       ├── IcebergDemo.kt         # HadoopCatalog 구현
│                       └── RestCatalogDemo.kt     # REST Catalog 시뮬레이션
├── build.gradle.kts
└── README.md
```

## 의존성

- Apache Iceberg 1.3.1
- Apache Iceberg AWS 1.3.1 (REST 클라이언트 구현체)
- Apache Parquet 1.13.1
- Apache Hadoop 3.3.6
- Kotlin 1.9.22
- Jackson (JSON 처리) 2.15.2

## 참고 사항

실제 REST Catalog를 사용하려면 REST 서버가 필요합니다. 이 데모에서는 실제 REST 서버 없이 작동 방식을 시뮬레이션하고 있습니다. 