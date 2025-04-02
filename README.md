# Iceberg Demo (Kotlin)

이 프로젝트는 Apache Iceberg를 사용하여 데이터 레이크를 구축하는 데모 애플리케이션입니다. Kotlin으로 작성되었으며, Gradle을 빌드 도구로 사용합니다.

## 기능

- Iceberg 테이블 생성 및 관리
- Parquet 형식으로 데이터 저장
- 데이터 삽입 및 조회
- 테이블 스냅샷 관리

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

3. 애플리케이션 실행:
```bash
java -jar build/libs/iceberg-demo-kotlin-1.0-SNAPSHOT.jar
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
│                       └── IcebergDemo.kt
├── build.gradle.kts
└── README.md
```

## 의존성

- Apache Iceberg 1.4.3
- Apache Parquet 1.13.1
- Apache Hadoop 3.3.6
- Kotlin 1.9.22 