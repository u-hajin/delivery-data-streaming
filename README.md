# Delivery Data Streaming

## 프로젝트 소개

Kafka, flink를 사용해 실시간으로 발생하는 배달 주문 데이터를 처리하고 저장합니다. Sink operation을 사용해 데이터는 postgreSQL, elasticsearch에 저장되며, kibana에서 시각화합니다.

## 시스템 구조

<img width="800" height="auto" src="https://github.com/u-hajin/u-hajin/assets/68963707/46ddc1aa-a3f5-4d71-9f92-fc2b6067b2e0">

## 프로젝트 구성

### 환경 구축

- Docker compose 사용해 컨테이너 구성
- Zookeeper, Kafka broker, Control center, Postgres, Elasticsearch, Kibana 컨테이너 실행 및 관리

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

### 데이터 분석

- Kibana 사용해 elasticsearch에 저장된 데이터 시각화

## 개발 환경

- Docker compose
- Python: 3.9
- Java: OpenJDK 11
- Flink: 1.18.0
- OS: Mac

## 실행 방법

1. Kafka, Flink, PostgreSQL, Elasticsearch, Kibana 서비스 시작을 위해 `docker compose up`
2. DeliveryDataGenerator 디렉토리의 data_generator.py 실행 (Kakao API key 발급 받아 코드에 삽입 필요)
3. `run -c application.DataStreamJob target/DeliveryDataStreaming-1.0-SNAPSHOT.jar`를 통해 flink job 제출

## 서비스 UI 접속

- Control Center: localhost:9092
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
