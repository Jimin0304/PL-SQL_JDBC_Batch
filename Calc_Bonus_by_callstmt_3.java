package com.kopo.jimin;

import java.sql.*;

public class Calc_Bonus_by_callstmt_3 {

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
        CallableStatement callStmt = null;

        long startTime = System.currentTimeMillis();

        try {
            System.out.println("=== 배치 처리 시작 - CallableStatement 방식 3단계 (단일 SQL 처리) ===");
            System.out.println("처리 방식: PL/SQL Stored Procedure (DB 서버에서 단일 SQL로 모든 로직 처리)");
            System.out.println("특징: 단일 SQL (INSERT INTO ... SELECT FROM ...) 사용");

            // 1. 데이터베이스 연결
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            conn.setAutoCommit(false); // Procedure handles its own COMMIT
            System.out.println("데이터베이스 연결 성공");

            // 2. 기존 데이터 삭제
            truncateTable(conn);

            // 3. CallableStatement 호출 (기존에 생성된 Stored Procedure 호출)
            // SQLPLUS에서 미리 아래 프로시저를 생성해야 합니다:
            // PROC_CALC_BONUS_COUPON
            String storedProcCall = "{CALL PROC_CALC_BONUS_COUPON(?, ?, ?)}";

            System.out.println("CallableStatement 생성 완료 (Stored Procedure 호출)");
            System.out.println("✅ 모든 로직을 DB 서버에서 단일 SQL로 처리 (Client ↔ DB 트래픽 최소화)");
            System.out.println("✅ PL/SQL Stored Procedure 내부에서 단일 SQL 실행");

            // 4. CallableStatement 생성 및 실행
            callStmt = conn.prepareCall(storedProcCall);

            // OUT 매개변수 등록
            callStmt.registerOutParameter(1, Types.INTEGER); // 처리 건수 (p_processed_count)
            callStmt.registerOutParameter(2, Types.INTEGER); // 발급 건수 (p_inserted_count)
            callStmt.registerOutParameter(3, Types.INTEGER); // 오류 건수 (p_error_count)

            System.out.println("CallableStatement 실행 시작 (PL/SQL Stored Procedure 호출)...");
            System.out.println("(모든 처리가 DB 서버에서 최적화된 방식으로 진행됩니다 - 단일 SQL)");

            // PL/SQL Stored Procedure 실행
            callStmt.execute();

            // 결과 가져오기
            int processedCount = callStmt.getInt(1);
            int insertCount = callStmt.getInt(2);
            int errorCount = callStmt.getInt(3);
            int commitCount = 1; // 단일 SQL 실행 후 단일 COMMIT (프로시저 내에서 처리)

            // 5. 최종 결과 출력
            printResults(startTime, processedCount, insertCount, errorCount, commitCount);

            // 6. 결과 검증
            validateResults(conn);

        } catch (SQLException e) {
            System.err.println("=== CallableStatement 오류 발생 ===");
            System.err.println("오류 코드: " + e.getErrorCode());
            System.err.println("SQL 상태: " + e.getSQLState());
            System.err.println("오류 메시지: " + e.getMessage());
            e.printStackTrace();

            // 롤백 시도 (프로시저 내에서 처리되므로 여기서는 필요 없을 수 있지만, 혹시 모를 외부 오류에 대비)
            if (conn != null) {
                try {
                    conn.rollback();
                    System.err.println("트랜잭션이 롤백되었습니다.");
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
            // 7. 리소스 정리
            closeResources(callStmt, conn);

            // 최종 처리 결과 (전체 실행 시간)
            long endTime = System.currentTimeMillis();
            System.out.printf("%n=== 처리 완료 ===%n총 처리 시간: %,d ms%n", (endTime - startTime));
        }
    }

    /**
     * 결과 출력 (메시지 업데이트)
     */
    private static void printResults(long startTime, int processedCount, int insertCount,
                                     int errorCount, int commitCount) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        System.out.println("\n=== CallableStatement (PL/SQL Stored Procedure - 단일 SQL) 처리 결과 ===");
        System.out.printf("총 조회 건수: %,d건 (2018년 이후 가입자)%n", processedCount);
        System.out.printf("쿠폰 발급 건수: %,d건%n", insertCount);
        System.out.printf("처리 오류 건수: %,d건 (쿠폰 생성 불가 건수)%n", errorCount);
        System.out.printf("총 Commit 횟수: %d회%n", commitCount); // Stored Procedure handles a single commit
        System.out.printf("총 처리 시간: %,d ms (%.2f초)%n", executionTime, executionTime / 1000.0);

        if (processedCount > 0) { // Avoid division by zero
            System.out.printf("• 오류율 (쿠폰 미발급률): %.2f%%\n", ((double)errorCount / processedCount) * 100);
        }
    }

    /**
     * 기존 테이블 데이터 삭제 (동일)
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
     * 리소스 정리 (동일)
     */
    private static void closeResources(CallableStatement callStmt, Connection conn) {
        if (callStmt != null) {
            try {
                callStmt.close();
                System.out.println("CallableStatement 정리 완료 (PL/SQL Stored Procedure 처리 완료)");
            } catch (SQLException e) {
                System.err.println("CallableStatement 정리 중 오류: " + e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
                System.out.println("Connection 정리 완료");
            } catch (SQLException e) {
                System.err.println("Connection 정리 중 오류: " + e.getMessage());
            }
        }
    }

    /**
     * 처리 결과 검증 (동일)
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

            // 발급률 검증
            String ratioSQL = """
                SELECT 
                    (SELECT COUNT(*) FROM BONUS_COUPON WHERE YYYYMM = '202506') as 발급건수,
                    (SELECT COUNT(*) FROM CUSTOMER 
                     WHERE ENROLL_DT >= DATE '2018-01-01' 
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

                    if (Math.abs(ratio - 100.0) < 1.0) { // Small tolerance for floating point comparisons
                        System.out.println("✅ 발급률이 정상 범위입니다.");
                    } else {
                        System.out.printf("⚠️  발급률 이상: %.1f%% (100%와 %.1f%% 차이)%n",
                                ratio, Math.abs(ratio - 100.0));
                    }
                }
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
}