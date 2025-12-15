package com.kopo.jimin;

import java.sql.*;

/**
 * 배치 처리 성능 비교 - CallableStatement 방식 3단계
 * 특징: CallableStatement + 1개의 최적화된 SQL로 데이터 처리
 * 처리방식: 단일 SQL 실행으로 집합 기반 처리
 */
public class Calc_Bonus_by_callstmt_3 {

    // 데이터베이스 연결 정보
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;
    
    public static void main(String[] args) {
        Connection conn = null;
        CallableStatement callStmt = null;
        long startTime = System.currentTimeMillis();

        try {
            System.out.println("=== CallableStatement 방식 3단계 시작 ===");

            // 데이터베이스 연결
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            conn.setAutoCommit(false);

            // 기존 데이터 삭제
            truncateTable(conn);

            // PL/SQL Anonymous Block (단일 SQL 방식)
            String plsqlBlock = """
                DECLARE
                    v_processed_count NUMBER := 0;
                    v_insert_count NUMBER := 0;
                    v_error_count NUMBER := 0;
                    v_sql_count NUMBER := 1;
                BEGIN
                    -- 단일 SQL로 모든 로직 처리 (CASE문 중복 제거)
                    INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT)
                    WITH coupon_calc AS (
                        SELECT
                            ID,
                            EMAIL,
                            CREDIT_LIMIT,
                            CASE
                                WHEN CREDIT_LIMIT < 1000 THEN 'AA'
                                WHEN CREDIT_LIMIT >= 1000 AND CREDIT_LIMIT < 3000 THEN 'BB'
                                WHEN CREDIT_LIMIT >= 3000 AND CREDIT_LIMIT < 4000 THEN
                                    CASE
                                        WHEN GENDER = 'F' 
                                             AND (ADDRESS1 LIKE '%송파구%' AND ADDRESS1 LIKE '%풍납1동%'
                                                  OR ADDRESS2 LIKE '%송파구%' AND ADDRESS2 LIKE '%풍납1동%') 
                                        THEN 'C2'
                                        ELSE 'CC'
                                    END
                                WHEN CREDIT_LIMIT >= 4000 THEN 'DD'
                                ELSE NULL
                            END AS COUPON_CD
                        FROM CUSTOMER
                        WHERE ENROLL_DT >= DATE '2018-01-01'
                          AND CREDIT_LIMIT IS NOT NULL
                          AND EMAIL IS NOT NULL
                          AND ID IS NOT NULL
                    )
                    SELECT
                        '202506',
                        ID,
                        EMAIL,
                        COUPON_CD,
                        CREDIT_LIMIT,
                        NULL
                    FROM coupon_calc
                    WHERE COUPON_CD IS NOT NULL;
                    
                    v_insert_count := SQL%ROWCOUNT;
                    
                    -- 대상 건수 조회
                    SELECT COUNT(*)
                    INTO v_processed_count
                    FROM CUSTOMER
                    WHERE ENROLL_DT >= DATE '2018-01-01'
                      AND CREDIT_LIMIT IS NOT NULL
                      AND EMAIL IS NOT NULL
                      AND ID IS NOT NULL;
                    
                    COMMIT;
                    
                    -- OUT 매개변수 설정
                    ? := v_processed_count;
                    ? := v_insert_count;
                    ? := v_error_count;
                    ? := v_sql_count;
                    
                EXCEPTION
                    WHEN OTHERS THEN
                        v_error_count := 1;
                        v_insert_count := 0;
                        v_sql_count := 0;
                        ROLLBACK;
                END;
                """;

            // CallableStatement 실행
            callStmt = conn.prepareCall(plsqlBlock);

            // OUT 매개변수 등록
            callStmt.registerOutParameter(1, Types.INTEGER); // 처리 건수
            callStmt.registerOutParameter(2, Types.INTEGER); // 발급 건수
            callStmt.registerOutParameter(3, Types.INTEGER); // 오류 건수
            callStmt.registerOutParameter(4, Types.INTEGER); // SQL 실행 횟수

            // 실행
            callStmt.execute();

            // 결과 가져오기
            int processedCount = callStmt.getInt(1);
            int insertCount = callStmt.getInt(2);
            int errorCount = callStmt.getInt(3);
            int sqlExecutionCount = callStmt.getInt(4);

            // 결과 출력
            printResults(startTime, processedCount, insertCount, errorCount, sqlExecutionCount);

            // 결과 검증
            validateResults(conn);

        } catch (SQLException e) {
            System.err.println("SQL 오류: " + e.getMessage());
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    System.err.println("롤백 실패: " + rollbackEx.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("오류: " + e.getMessage());
        } finally {
            closeResources(callStmt, conn);
            long endTime = System.currentTimeMillis();
            System.out.printf("총 처리 시간: %,d ms%n", (endTime - startTime));
        }
    }

    private static void printResults(long startTime, int processedCount, int insertCount,
                                     int errorCount, int sqlExecutionCount) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        System.out.printf("총 조회 건수: %,d건%n", processedCount);
        System.out.printf("쿠폰 발급 건수: %,d건%n", insertCount);
        System.out.printf("처리 오류 건수: %,d건%n", errorCount);
        System.out.printf("SQL 실행 횟수: %d회%n", sqlExecutionCount);
        System.out.printf("처리 시간: %,d ms (%.2f초)%n", executionTime, executionTime / 1000.0);

        if (processedCount > 0 && executionTime > 0) {
            System.out.printf("처리 속도: %,.0f건/초%n",
                    (double)processedCount / (executionTime / 1000.0));
        }
    }

    private static void truncateTable(Connection conn) throws SQLException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
            System.out.println("BONUS_COUPON 테이블 초기화 완료");
        } finally {
            if (stmt != null) stmt.close();
        }
    }

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

            System.out.println("\n=== 결과 검증 ===");
            System.out.println("쿠폰코드\t발급건수\t평균포인트");
            System.out.println("------------------------");

            while (rs.next()) {
                System.out.printf("%s\t\t%,d\t\t%.2f%n",
                        rs.getString("COUPON_CD"),
                        rs.getInt("CNT"),
                        rs.getDouble("AVG_POINT"));
            }

        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }
    }

    private static void closeResources(CallableStatement callStmt, Connection conn) {
        if (callStmt != null) {
            try {
                callStmt.close();
            } catch (SQLException e) {
                System.err.println("CallableStatement 정리 실패: " + e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Connection 정리 실패: " + e.getMessage());
            }
        }
    }
}