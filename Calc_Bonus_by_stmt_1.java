package com.kopo.jimin;

import java.sql.*;
import java.math.BigDecimal;

/**
 * 배치 처리 성능 비교 - Statement 방식 1단계 (발급률 문제 해결)
 * 특징: 전체 570만건 조회 후 Java에서 필터링, 매번 Statement 객체 생성, 개별 Commit
 * 성능 이슈: 불필요한 데이터 전송 + 빈번한 객체 생성/해제 + 과도한 Commit
 */
public class Calc_Bonus_by_stmt_1 {

    // 데이터베이스 연결 정보
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;

    static {
        // 인코딩 설정
        System.setProperty("file.encoding", "UTF-8");
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement selectStmt = null;
        ResultSet rs = null;

        long startTime = System.currentTimeMillis();
        int processedCount = 0;
        int filteredCount = 0;
        int insertCount = 0;
        int errorCount = 0;

        try {
            System.out.println("=== 배치 처리 시작 - Statement 방식 1단계 ===");
            System.out.println("처리 방식: 전체 데이터 조회 후 Java 필터링 (성능 저하 의도)");

            // 1. 데이터베이스 연결
            try {
                conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                conn.setAutoCommit(false);
                System.out.println("데이터베이스 연결 성공");
            } catch (SQLException e) {
                System.err.println("데이터베이스 연결 실패: " + e.getMessage());
                System.err.println("연결 URL: " + DB_URL);
                System.err.println("사용자: " + DB_USER);
                throw e; // 연결 실패시 프로그램 종료
            }

            // 2. 기존 데이터 삭제
            try {
                truncateTable(conn);
            } catch (SQLException e) {
                System.err.println("테이블 초기화 실패: " + e.getMessage());
                // 테이블 초기화 실패는 계속 진행 (테이블이 비어있을 수 있음)
            }

            // 3. 고객 데이터 조회 준비
            try {
                selectStmt = conn.createStatement();
                selectStmt.setFetchSize(10); // 작은 Fetch Size로 성능 저하 유발

                String selectSQL = """
                    SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2, ENROLL_DT
                    FROM CUSTOMER 
                    ORDER BY ID
                    """;

                System.out.println("SQL: 전체 CUSTOMER 데이터 조회 (필터링 없음)");
                System.out.println("Fetch Size: 10 (네트워크 라운드트립 증가)");

                rs = selectStmt.executeQuery(selectSQL);
                System.out.println("고객 데이터 조회 시작");

            } catch (SQLException e) {
                System.err.println("고객 데이터 조회 쿼리 실행 실패: " + e.getMessage());
                throw e;
            }

            // 4. 데이터 처리 루프
            try {
                while (rs.next()) {
                    processedCount++;

                    try {
                        // 데이터 읽기
                        String customerId = rs.getString("ID");
                        String email = rs.getString("EMAIL");

                        // Oracle NUMBER(9,2) → BigDecimal로 정확한 금융 데이터 처리
                        BigDecimal creditLimit = rs.getBigDecimal("CREDIT_LIMIT");

                        String gender = rs.getString("GENDER");
                        String address1 = rs.getString("ADDRESS1");
                        String address2 = rs.getString("ADDRESS2");
                        Date enrollDt = rs.getDate("ENROLL_DT");

                        // 진행률 출력 (전체 조회 진행상황)
                        if (processedCount % 50000 == 0) {
                            System.out.printf("전체 데이터 조회 진행률: %,d건 조회됨 (오류: %,d건)%n",
                                    processedCount, errorCount);
                        }

                        // 5. Java에서 2013년 이후 가입 조건 체크 (검증과 동일한 조건 사용)
                        if (enrollDt != null && enrollDt.compareTo(java.sql.Date.valueOf("2013-01-01")) >= 0) {
                            filteredCount++;

                            // NULL 체크
                            if (creditLimit == null || email == null || customerId == null) {
                                System.err.printf("필수 데이터 누락 - ID: %s, EMAIL: %s, CREDIT: %s%n",
                                        customerId, email, creditLimit);
                                continue;
                            }

                            String fullAddress = (address1 != null ? address1 : "") +
                                    (address2 != null ? " " + address2 : "");

                            // 6. 쿠폰 코드 계산
                            String couponCode = calculateCouponCode(creditLimit, gender, fullAddress);

                            if (couponCode != null) {
                                // 7. 쿠폰 발급 처리
                                if (insertCoupon(conn, customerId, email, couponCode, creditLimit)) {
                                    insertCount++;

                                    // 쿠폰 발급 진행률 출력
                                    if (insertCount % 1000 == 0) {
                                        System.out.printf("쿠폰 발급 진행률: %,d건 발급됨%n", insertCount);
                                    }
                                }
                            }
                        }

                    } catch (SQLException e) {
                        errorCount++;
                        System.err.printf("행 처리 중 오류 (행번호: %d): %s%n", processedCount, e.getMessage());

                        // 심각한 SQL 오류인 경우 중단
                        if (e.getErrorCode() == 1017 || // invalid username/password
                                e.getErrorCode() == 28000 || // account locked
                                e.getErrorCode() == 12541) { // TNS:no listener
                            System.err.println("심각한 데이터베이스 오류 발생. 처리를 중단합니다.");
                            throw e;
                        }

                        // 일반적인 오류는 계속 진행
                        continue;

                    } catch (Exception e) {
                        errorCount++;
                        System.err.printf("예상치 못한 오류 (행번호: %d): %s%n", processedCount, e.getMessage());
                        e.printStackTrace();

                        // 너무 많은 오류가 발생하면 중단
                        if (errorCount > 1000) {
                            System.err.println("오류가 너무 많이 발생했습니다. 처리를 중단합니다.");
                            break;
                        }
                        continue;
                    }
                }

            } catch (SQLException e) {
                System.err.println("데이터 처리 루프 중 심각한 오류: " + e.getMessage());
                throw e;
            }

            // 8. 최종 결과 출력
            printResults(startTime, processedCount, filteredCount, insertCount, errorCount);

            // 9. 결과 검증
            try {
                validateResults(conn);
            } catch (SQLException e) {
                System.err.println("결과 검증 중 오류: " + e.getMessage());
                // 검증 실패는 프로그램을 중단시키지 않음
            }

        } catch (SQLException e) {
            System.err.println("=== 데이터베이스 오류 발생 ===");
            System.err.println("오류 코드: " + e.getErrorCode());
            System.err.println("SQL 상태: " + e.getSQLState());
            System.err.println("오류 메시지: " + e.getMessage());
            e.printStackTrace();

            // 롤백 시도
            if (conn != null) {
                try {
                    conn.rollback();
                    System.out.println("트랜잭션이 롤백되었습니다.");
                } catch (SQLException rollbackEx) {
                    System.err.println("롤백 실패: " + rollbackEx.getMessage());
                }
            }

        } catch (Exception e) {
            System.err.println("=== 예상치 못한 오류 발생 ===");
            System.err.println("오류 타입: " + e.getClass().getSimpleName());
            System.err.println("오류 메시지: " + e.getMessage());
            e.printStackTrace();

        } finally {
            // 11. 리소스 정리
            closeResources(rs, selectStmt, conn);

            // 최종 처리 결과
            long endTime = System.currentTimeMillis();
            System.out.printf("%n=== 처리 완료 ===\n처리 시간: %,d ms\n최종 성공: %,d건\n총 오류: %,d건%n",
                    (endTime - startTime), insertCount, errorCount);
        }
    }

    /**
     * 쿠폰 발급 처리 (개별 트랜잭션)
     */
    private static boolean insertCoupon(Connection conn, String customerId, String email,
                                        String couponCode, BigDecimal creditLimit) {
        Statement insertStmt = null;

        try {
            // 매번 새로운 Statement 객체 생성 (성능 저하 요인)
            insertStmt = conn.createStatement();

            // SQL Injection 방지를 위해 PreparedStatement 사용하는 것이 더 좋지만,
            // 성능 저하 시연을 위해 String.format 사용
            String insertSQL = String.format("""
                INSERT INTO BONUS_COUPON 
                (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT)
                VALUES ('202506', '%s', '%s', '%s', %s, NULL)
                """,
                    customerId.replace("'", "''"), // SQL Injection 기본 방어
                    email.replace("'", "''"),
                    couponCode,
                    creditLimit.toString());

            insertStmt.executeUpdate(insertSQL);
            conn.commit(); // 매번 개별 Commit (성능 저하 요인)

            return true;

        } catch (SQLException e) {
            System.err.printf("쿠폰 발급 실패 - 고객ID: %s, 오류: %s%n", customerId, e.getMessage());

            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                System.err.printf("롤백 실패 - 고객ID: %s, 오류: %s%n", customerId, rollbackEx.getMessage());
            }

            return false;

        } catch (Exception e) {
            System.err.printf("예상치 못한 오류 - 고객ID: %s, 오류: %s%n", customerId, e.getMessage());
            return false;

        } finally {
            if (insertStmt != null) {
                try {
                    insertStmt.close();
                } catch (SQLException e) {
                    System.err.println("Statement 해제 실패: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 결과 출력
     */
    private static void printResults(long startTime, int processedCount, int filteredCount,
                                     int insertCount, int errorCount) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        System.out.println("\n=== 처리 결과 ===");
        System.out.printf("총 조회 건수: %,d건 (전체 고객)%n", processedCount);
        System.out.printf("2013년 이후 가입자: %,d건 (Java 필터링 결과)%n", filteredCount);
        System.out.printf("쿠폰 발급 건수: %,d건%n", insertCount);
        System.out.printf("처리 오류 건수: %,d건%n", errorCount);
        System.out.printf("총 처리 시간: %,d ms (%.2f초)%n",
                executionTime, executionTime / 1000.0);

        // 성능 저하 요인 분석
        System.out.println("\n=== 성능 저하 요인 분석 ===");
        System.out.printf("• 불필요한 데이터 전송: %,d건 (전체) vs %,d건 (필요)%n",
                processedCount, filteredCount);
        System.out.printf("• Statement 객체 생성: %,d회%n", insertCount);
        System.out.printf("• Commit 횟수: %,d회%n", insertCount);
        if (processedCount > 0) {
            System.out.printf("• 네트워크 낭비율: %.1f%% (%,d건 불필요 전송)%n",
                    ((double)(processedCount - filteredCount) / processedCount) * 100,
                    (processedCount - filteredCount));
        }
        if (errorCount > 0) {
            System.out.printf("• 오류율: %.2f%%\n", ((double)errorCount / processedCount) * 100);
        }
    }

    /**
     * 쿠폰 코드 계산 로직
     */
    private static String calculateCouponCode(BigDecimal creditLimit, String gender, String address) {
        try {
            if (creditLimit == null) {
                return null;
            }

            // BigDecimal 비교는 compareTo 사용
            if (creditLimit.compareTo(new BigDecimal("1000")) < 0) {
                return "AA";
            } else if (creditLimit.compareTo(new BigDecimal("1000")) >= 0 &&
                    creditLimit.compareTo(new BigDecimal("3000")) < 0) {
                return "BB";
            } else if (creditLimit.compareTo(new BigDecimal("3000")) >= 0 &&
                    creditLimit.compareTo(new BigDecimal("4000")) < 0) {
                // 특별 조건: 송파구 풍납1동 거주 여성 고객
                if ("F".equals(gender) && address != null &&
                        address.contains("송파구") && address.contains("풍납1동")) {
                    return "C2";
                }
                return "CC";
            } else if (creditLimit.compareTo(new BigDecimal("4000")) >= 0) {
                return "DD";
            }
            return null;

        } catch (Exception e) {
            System.err.printf("쿠폰 코드 계산 오류 - creditLimit: %s, gender: %s: %s%n",
                    creditLimit, gender, e.getMessage());
            return null;
        }
    }

    /**
     * 기존 테이블 데이터 삭제
     */
    private static void truncateTable(Connection conn) throws SQLException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
            System.out.println("기존 BONUS_COUPON 테이블 데이터 삭제 완료");
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    System.err.println("Statement 해제 실패: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 처리 결과 검증 (동일한 날짜 조건 사용)
     */
    private static void validateResults(Connection conn) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();

            String validationSQL = """
                SELECT COUPON_CD, COUNT(*) as CNT, ROUND(AVG(CREDIT_POINT), 2) as AVG_POINT
                FROM BONUS_COUPON 
                WHERE YYYYMM = '202506'
                GROUP BY COUPON_CD
                ORDER BY COUPON_CD
                """;

            rs = stmt.executeQuery(validationSQL);

            System.out.println("\n=== 쿠폰 발급 결과 검증 ===");
            System.out.println("쿠폰코드\t발급건수\t평균포인트");
            System.out.println("--------------------------------");

            while (rs.next()) {
                System.out.printf("%s\t\t%,d\t\t%.2f%n",
                        rs.getString("COUPON_CD"),
                        rs.getInt("CNT"),
                        rs.getDouble("AVG_POINT"));
            }

            if (rs != null) {
                rs.close();
                rs = null;
            }

            // 수정된 검증: Java와 동일한 날짜 조건 사용
            String ratioSQL = """
                SELECT 
                    (SELECT COUNT(*) FROM BONUS_COUPON WHERE YYYYMM = '202506') as 발급건수,
                    (SELECT COUNT(*) FROM CUSTOMER 
                     WHERE ENROLL_DT >= DATE '2013-01-01' 
                       AND CREDIT_LIMIT IS NOT NULL 
                       AND EMAIL IS NOT NULL 
                       AND ID IS NOT NULL) as 대상건수
                FROM DUAL
                """;

            rs = stmt.executeQuery(ratioSQL);
            if (rs.next()) {
                int issuedCount = rs.getInt("발급건수");
                int targetCount = rs.getInt("대상건수");
                if (targetCount > 0) {
                    double ratio = (double)issuedCount / targetCount * 100;
                    System.out.printf("\n발급률: %.1f%% (%,d건 / %,d건)%n",
                            ratio, issuedCount, targetCount);

                    // 발급률이 100%에 가까운지 확인
                    if (Math.abs(ratio - 100.0) < 1.0) {
                        System.out.println("✅ 발급률이 정상 범위입니다.");
                    } else {
                        System.out.printf("⚠️  발급률 이상: %.1f%% (100%와 %.1f%% 차이)%n",
                                ratio, Math.abs(ratio - 100.0));
                    }
                }
            }

            // 추가 디버깅: 실제 Java 조건과 SQL 조건 비교
            if (rs != null) {
                rs.close();
                rs = null;
            }

            System.out.println("\n=== 조건별 상세 분석 ===");

            String debugSQL = """
                SELECT 
                    '전체 고객' as 구분,
                    COUNT(*) as 건수
                FROM CUSTOMER
                UNION ALL
                SELECT 
                    '2013년 이후 가입 (>= 2013-01-01)' as 구분,
                    COUNT(*) as 건수
                FROM CUSTOMER 
                WHERE ENROLL_DT >= DATE '2013-01-01'
                UNION ALL
                SELECT 
                    '2013년 이후 + 필수데이터 존재' as 구분,
                    COUNT(*) as 건수
                FROM CUSTOMER 
                WHERE ENROLL_DT >= DATE '2013-01-01'
                  AND CREDIT_LIMIT IS NOT NULL 
                  AND EMAIL IS NOT NULL 
                  AND ID IS NOT NULL
                ORDER BY 구분
                """;

            rs = stmt.executeQuery(debugSQL);
            while (rs.next()) {
                System.out.printf("%-30s: %,d건%n",
                        rs.getString("구분"), rs.getInt("건수"));
            }

        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    System.err.println("ResultSet 해제 실패: " + e.getMessage());
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    System.err.println("Statement 해제 실패: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 리소스 정리
     */
    private static void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        // ResultSet 정리
        if (rs != null) {
            try {
                rs.close();
                System.out.println("ResultSet 정리 완료");
            } catch (SQLException e) {
                System.err.println("ResultSet 정리 중 오류: " + e.getMessage());
            }
        }

        // Statement 정리
        if (stmt != null) {
            try {
                stmt.close();
                System.out.println("Statement 정리 완료");
            } catch (SQLException e) {
                System.err.println("Statement 정리 중 오류: " + e.getMessage());
            }
        }

        // Connection 정리
        if (conn != null) {
            try {
                conn.close();
                System.out.println("Connection 정리 완료");
            } catch (SQLException e) {
                System.err.println("Connection 정리 중 오류: " + e.getMessage());
            }
        }
    }
}