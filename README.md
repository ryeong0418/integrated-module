# Integrated technology Group WAS&DB Data Analysis

Exem Was/DB 통합 분석 데이터 분석기

## Required Software

- python >= 3.8.10
- postgresql >= 13.3

## Settings

- 설치한 postgresql에 분석 모듈에서 사용할 Database 생성
- 분석에 사용될 연동 정보 설정 (/resource/config/config-prod.json)

## Install
### 파이썬 가상환경 생성 및 라이브러리 설치

- Windows
```shell
/0_install-offline.bat (offline 환경)
/0_install-online.bat (online 환경)
```

- UNIX
```shell
/bin/0_install-offline.sh (offline 환경)
/bin/0_install-online.sh (online 환경)
```

## Usage

1. Initialize (분석 모듈 Table 생성 & 메타 정보 저장)
```shell
/1_initialize.bat (Windows)
/bin/1_initialize.sh (UNIX)
```

2. Extractor (분석 데이터 추출 및 저장)
```shell
/2_extractor.bat {시작날짜} {기간} (Windows)
/bin/2_extractor.sh {시작날짜} {기간} (UNIX)
```

3. Summarizer (분석 데이터 취합 및 저장, 시작 날짜에 대한 데이터 summary)
```shell
/3_summarizer.bat {시작날짜} (Windows)
/bin/3_summarizer.sh {시작날짜} (UNIX)
```

4. SqlTextMerge (Was/DB Sql Text Match)
```shell
/4_sqltextmerge.bat {시작날짜} {기간} (Windows)
/bin/4_sqltextmerge.sh {시작날짜} {기간} (UNIX)
```

5. Visualization (분석 데이터 추출)
```shell
/5_visualization.bat (Windows)
/bin/5_visualization.sh (UNIX)
```

6. Scheduler (분석 데이터 추출/취합/Sql Text Match 동작 스케쥴러 (하루전) - Background Execute)
```shell
/6_batch.bat (Windows)
/bin/6_batch.sh (UNIX)
```

## Extra Function

- DB export/import
```shell
파이썬 가상환경 활성화 후
python -m src.common.file_export (export)
python -m src.common.file_export --proc insert (import)
```
