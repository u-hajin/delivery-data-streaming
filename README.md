# Delivery Data Streaming

## 프로젝트 소개

Kafka, flink를 사용해 실시간으로 발생하는 배달 주문 데이터를 처리하고 저장합니다. Sink operation을 사용해 데이터는 postgreSQL, elasticsearch에 저장되며, kibana에서 시각화합니다.

- ’실시간 배달 주문 데이터 처리’를 주제로 데이터 생성, 변환, 저장, 시각화 진행
- Kafka, flink를 사용해 실시간 데이터 처리 및 집계 연산 적용
- Sink operation을 사용해 postgresql, elasticsearch에 저장, kibana에서 시각화
- Kafka topic에 데이터가 발행될 때마다 자동으로 변환을 거쳐 DB 테이블에 저장, 시각화하는 파이프라인 구축

## 프로젝트 목적

‘배달 주문’은 대규모의 데이터를 지속적으로 생성하는 대표적인 분야이므로 실시간 데이터 처리에 유용한 주제입니다.  또한 배달 주문 데이터는 시간, 위치, 가격 등 여러 차원의 정보를 포함하므로 복합적인 데이터 처리, 분석이 가능합니다. 
따라서 ‘실시간 배달 주문 데이터’를 주제로 데이터 생성, 수집, 변환, 저장, 시각화 파이프라인을 생성해보면서 실시간 데이터 스트리밍 기술에 대한 이해를 높이고자 했습니다.

## 시스템 구조

<img width="800" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/46ddc1aa-a3f5-4d71-9f92-fc2b6067b2e0">

## 구현 설명

- 환경 일관성 유지와 확장을 위한 Docker compose
  - 사용 기술들을 컨테이너로 구성, 관리해 어떤 환경에서도 일관성 유지
  - 각 컨테이너를 독립적으로 관리하면서 새로운 기술을 도입 시 빠르게 확장이 가능

  
- 유연한 아키텍처, 장애 대응을 위한 Kafka
  - Kafka를 사용해 데이터 생성 시스템과 데이터 처리 시스템 분리
  - Kafka 브로커 3개를 두어 replication factor를 설정해 파티션을 복제하고, 브로커 장애 시 리더 재선출을 통해 대응 가능


- 사용자 정의 집계 연산, 안정성을 위한 Flink
  - Flink에서 제공하는 map, key by, reduce operation을 활용해 실시간 데이터에 집계 연산 적용
  - 체크 포인팅을 지원하는 sink를 통해 데이터 중복, 누락을 최소화하고 안전하게 postgresql, elasticsearch에 저장


- 데이터 실시간 시각화를 위한 Elasticsearch & Kibana
  - 지도, 대시보드를 활용해 데이터를 시각화하고 실시간 업데이트 기능으로 데이터 변화 추이 모니터링

## 프로젝트 구성

### 환경 구축

- Docker compose 사용해 컨테이너 구성
- Zookeeper, Kafka broker, Control center, PostgreSQL, Elasticsearch, Kibana 컨테이너 실행 및 관리

### 데이터 수집

- Python 코드에서 배달 주문 데이터 생성
- Kafka 사용해 메시지 publish
- Confluent control center에서 kafka 모니터링

### 데이터 처리

- Flink job 사용해 배달 주문 내역이 저장된 토픽에서 메시지 소비, 실시간 처리
- 데이터 변환 및 집계 연산 수행

### 데이터 저장

- 처리된 데이터들을 sink operation을 통해 Elasticsearch, postgreSQL DB에 저장
- PostgreSQL delivery_information, pay_per_destination, pay_per_category, charge_per_day 테이블 생성
- 데이터 변환 및 집계 연산 완료 데이터들을 위 테이블에 삽입

### 데이터 분석

- Kibana 사용해 elasticsearch에 저장된 데이터를 지도, 대시보드에 시각화

## 개발 환경

- Docker compose
- Python: 3.9
- Java: OpenJDK 11
- Flink: 1.18.0
- OS: Mac

## 실행 방법

1. Kafka, PostgreSQL, Elasticsearch, Kibana 서비스 시작을 위해 `docker compose up`
2. Flink 클러스터 실행 `bin/start-cluster.sh`
3. config 설정
    - `DeliveryDataGenerator/resources/config.ini` kafka 브로커 설정, topic 이름, 카카오 API key 작성
    - `src/main/resources/config.properties` kafka 브로커 설정, topic 이름, DB 설정 작성
4. `mvn clean package` 명령어 실행
5. `DeliveryDataGenerator/data_generator.py` 실행
6. `flink-1.18.0/bin/flink run -c application.DataStreamJob target/DeliveryDataStreaming-1.0-SNAPSHOT.jar` 명령어로 flink job 실행

## 서비스 UI 접속

- Control Center: localhost:9091
- Flink: localhost:8081
- Kibana: localhost:5601

## 구현 상세 설명

아래 블로그에서 확인할 수 있습니다.

[실시간 배달 주문 데이터 처리 프로젝트](https://select-dev.tistory.com/category/Data/Project)


## 실행 화면

<img width="800" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/b56d04c7-1939-4ada-82e3-19bc61c5dba5">

<img width="800" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/8d78bf52-a67f-4388-9f4a-0e0e135939f0">

<img width="1221" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/9de04994-8d21-4a0e-91d5-c6780325aaf2">

<img width="400" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/8ed63e3f-92e0-4b0c-ae24-51d3386ff328">

<img width="900" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/980c26b0-863a-42c3-b3e3-1536cb5703c1">

<img width="900" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/b78555ce-7ef1-4a3d-91d8-9d7514e814ee">
