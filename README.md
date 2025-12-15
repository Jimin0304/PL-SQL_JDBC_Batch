# 프로젝트 환경 및 성능 비교 개요

본 프로젝트는 Java(Spring Boot) + JDBC 기반 대용량 데이터 처리 방식에 따른 성능 차이를 분석하는 것을 목표로 한다.  
DB 위치(로컬 / 원격) 및 네트워크 환경에 따른 성능 편차를 비교한다.

---

## 1. 개발 및 실행 환경

| 구분 | 항목 | 상세 내용 | 비고 |
|------|------|-----------|------|
| 개발 환경 | Application | Spring Boot (Java 기반) | JDBC API 활용 |
| 클라이언트 구현 | DB 접근 방식 | JDBC API | CallableStatement / Batch 처리 비교 |
| 클라이언트 환경 | 가상화 / OS | VMware 환경 (Client PC) | Java 애플리케이션 실행 |
| DB 접속 도구 | Client Tool | SQL Developer | 원격 DB 접속 및 SQL 검증 |

---

## 2. 데이터 규모 및 성능 목표

| 구분 | 항목 | 상세 내용 | 비고 |
|------|------|-----------|------|
| 데이터 규모 | CUSTOMER 테이블 | 약 5,700,000 Rows | 성능 비교용 대용량 데이터 |
| 성능 목표 | 편차 기준 | 처리 방식에 따라 10배 ~ 100배 성능 차이 발생 가능 | 과제 핵심 목표 |

---

## 3. 테스트 환경 세부 비교

| 환경 구분 | DBMS 위치 | 네트워크 특성 | 역할 (주요 비교 항목) |
|-----------|-----------|----------------|-----------------------|
| 환경 A (로컬) | VMware 내 로컬 Oracle 19c | Low Latency (지연/부하 최소화) | DBMS 자체 처리 성능 비교 기준 |
| 환경 B (원격) | 서버실 원격 Oracle 19c | High Latency (실제 네트워크 지연 반영) | 네트워크 환경이 성능에 미치는 영향 분석 |

---

## 4. 과제 핵심 목표

- JDBC 데이터 처리 방식에 따른 성능 차이 정량적 비교
- 네트워크 환경에 따른 Batch / CallableStatement 성능 변화 분석
- 로컬 DB 대비 원격 DB 환경에서의 성능 저하 요인 도출

---

## 5. 과제 수행 결과

<img width="880" height="601" alt="image" src="https://github.com/user-attachments/assets/23c3cf6e-11ef-42ff-a445-e3b649d34d4d" />
<img width="880" height="529" alt="image" src="https://github.com/user-attachments/assets/a3b30146-f7d1-402d-9a75-fb2569ab9bf1" />
<img width="880" height="536" alt="image" src="https://github.com/user-attachments/assets/ca019e51-d731-4473-9461-ce55cf41327a" />
<img width="880" height="598" alt="image" src="https://github.com/user-attachments/assets/6c60181c-8dc5-4818-9833-d9300c4c23ee" />
