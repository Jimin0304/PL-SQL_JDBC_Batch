package com.kopo.jimin;

import java.sql.*;
import java.math.BigDecimal;

/**
 * 배치 처리 성능 비교 - PreparedStatement 방식 2단계
 * 특징: addBatch, executeBatch를 적용하여 1,000개 단위로 INSERT
 * 개선: 빈번한 SQL 실행 개선 (Soft Parsing) (320만번 → 3200번)
 * 효과: Batch Processing으로 네트워크 라운드트립 대폭 감소
 */
public class Calc_Bonus_by_pstmt_2 {

    // 데이터베이스 연결 정보
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;

    // 최적화 파라미터
    private static final int COMMIT_SIZE = 10000;     // Commit 단위
    private static final int FETCH_SIZE = 1000;       // Fetch 단위
    private static final int BATCH_SIZE = 1000;       // Batch 단위 (NEW!)

    static {
        // 인코딩 설정
        System.setProperty("file.encoding", "UTF-8");
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement selectStmt = null;
        PreparedStatement insertPstmt = null;
        ResultSet rs = null;

        long startTime = System.currentTimeMillis();
        int processedCount = 0;
        int insertCount = 0;
        int batchCount = 0;
        int sqlExecutionCount = 0;
        int errorCount = 0;
        int commitCount = 0;

        try {
            System.out.println("=== 배치 처리 시작 - PreparedStatement 방식 2단계 ===");
            System.out.printf("핵심 개선: addBatch/executeBatch 적용 (Batch Size: %,d)%n", BATCH_SIZE);
            
            // 1. 데이터베이스 연결
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            conn.setAutoCommit(false);
            System.out.println("데이터베이스 연결 성공");

            // 2. 기존 데이터 삭제
            truncateTable(conn);

            // 3. PreparedStatement 미리 생성 (한 번만 Hard Parsing!)
            String insertSQL = """
                INSERT INTO BONUS_COUPON 
                (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT)
                VALUES ('202506', ?, ?, ?, ?, NULL)
                """;

            insertPstmt = conn.prepareStatement(insertSQL);
            System.out.println("INSERT용 PreparedStatement 생성 완료");
            System.out.println("✅ Hard Parsing: 1번 (SQL 파싱, 최적화, 실행계획 생성)");
            System.out.printf("✅ Batch Processing: %,d건 단위로 일괄 실행%n", BATCH_SIZE);

            // 4. 고객 데이터 조회
            selectStmt = conn.createStatement();
            selectStmt.setFetchSize(FETCH_SIZE);

            String selectSQL = """
                SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2, ENROLL_DT
                FROM CUSTOMER 
                WHERE ENROLL_DT >= DATE '2013-01-01'
                ORDER BY ID
                """;

            System.out.printf("SQL: 2013년 이후 가입자만 조회 (Fetch Size: %,d)%n", FETCH_SIZE);

            rs = selectStmt.executeQuery(selectSQL);
            System.out.println("고객 데이터 조회 시작 (Batch 처리로 고속 INSERT 예상)");

            // 5. 데이터 처리 및 배치 쿠폰 발급
            while (rs.next()) {
                processedCount++;

                try {
                    String customerId = rs.getString("ID");
                    String email = rs.getString("EMAIL");
                    BigDecimal creditLimit = rs.getBigDecimal("CREDIT_LIMIT");
                    String gender = rs.getString("GENDER");
                    String address1 = rs.getString("ADDRESS1");
                    String address2 = rs.getString("ADDRESS2");

                    // 진행률 출력
                    if (processedCount % 50000 == 0) {
                        System.out.printf("처리 진행률: %,d건 조회됨 (배치 대기: %,d건, 오류: %,d건)%n",
                                processedCount, batchCount, errorCount);
                    }

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
                        // 7. Batch에 추가 (실행하지 않고 누적)
                        if (addToBatch(insertPstmt, customerId, email, couponCode, creditLimit)) {
                            batchCount++;
                            insertCount++;

                            // Batch Size 단위로 일괄 실행
                            if (batchCount % BATCH_SIZE == 0) {
                                sqlExecutionCount++;
                                int[] results = insertPstmt.executeBatch();
                                System.out.printf("Batch 실행 #%,d: %,d건 일괄 처리 (SQL 실행: %,d회차)%n",
                                        sqlExecutionCount, results.length, sqlExecutionCount);

                                // 배치 실행 후 클리어
                                insertPstmt.clearBatch();
                                batchCount = 0; // 배치 카운터 리셋
                            }

                            // Commit 단위 체크
                            if (insertCount % COMMIT_SIZE == 0) {
                                conn.commit();
                                commitCount++;
                                System.out.printf("Commit 실행: %,d건 처리됨 (총 %d회 Commit)%n",
                                        insertCount, commitCount);
                            }

                            // 전체 진행률 출력
                            if (insertCount % 50000 == 0) {
                                System.out.printf("쿠폰 발급 진행률: %,d건 발급됨 (Batch로 고속 처리!)%n", insertCount);
                            }
                        }
                    }

                } catch (SQLException e) {
                    errorCount++;
                    System.err.printf("행 처리 중 오류 (행번호: %d): %s%n", processedCount, e.getMessage());

                    // 오류 발생시 현재 배치 클리어 및 롤백
                    try {
                        insertPstmt.clearBatch();
                        conn.rollback();
                        batchCount = 0; // 배치 카운터 리셋
                        System.err.printf("오류로 인한 배치 클리어 및 롤백 실행%n");
                    } catch (SQLException rollbackEx) {
                        System.err.println("롤백 실패: " + rollbackEx.getMessage());
                    }

                    if (errorCount > 1000) {
                        System.err.println("오류가 너무 많이 발생했습니다. 처리를 중단합니다.");
                        break;
                    }
                    continue;
                }
            }

            // 마지막 남은 Batch 실행
            if (batchCount > 0) {
                sqlExecutionCount++;
                int[] results = insertPstmt.executeBatch();
                System.out.printf("최종 Batch 실행: %,d건 처리 (총 SQL 실행: %,d회)%n",
                        results.length, sqlExecutionCount);
                insertPstmt.clearBatch();
            }

            // 마지막 남은 데이터 Commit
            if (insertCount % COMMIT_SIZE != 0) {
                conn.commit();
                commitCount++;
                System.out.printf("최종 Commit 실행: %,d건 처리 완료 (총 %d회 Commit)%n",
                        insertCount, commitCount);
            }

            // 8. 최종 결과 출력
            printResults(startTime, processedCount, insertCount, errorCount, commitCount, sqlExecutionCount);

            // 9. 결과 검증
            validateResults(conn);

        } catch (SQLException e) {
            System.err.println("=== 데이터베이스 오류 발생 ===");
            System.err.println("오류 코드: " + e.getErrorCode());
            System.err.println("SQL 상태: " + e.getSQLState());
            System.err.println("오류 메시지: " + e.getMessage());
            e.printStackTrace();

            // 배치 클리어 및 롤백
            if (insertPstmt != null) {
                try {
                    insertPstmt.clearBatch();
                } catch (SQLException ex) {
                    System.err.println("Batch 클리어 실패: " + ex.getMessage());
                }
            }

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
            // 10. 리소스 정리
            closeResources(rs, selectStmt, insertPstmt, conn);

            // 최종 처리 결과
            long endTime = System.currentTimeMillis();
            System.out.printf("%n=== 처리 완료 ===\n처리 시간: %,d ms\n최종 성공: %,d건\n총 오류: %,d건%n",
                    (endTime - startTime), insertCount, errorCount);
        }
    }

    /**
     * Batch에 INSERT 파라미터 추가 (실행하지 않고 누적)
     */
    private static boolean addToBatch(PreparedStatement insertPstmt, String customerId, String email,
                                      String couponCode, BigDecimal creditLimit) {
        try {
            // PreparedStatement 파라미터 설정
            insertPstmt.setString(1, customerId);      // CUSTOMER_ID
            insertPstmt.setString(2, email);           // EMAIL
            insertPstmt.setString(3, couponCode);      // COUPON_CD
            insertPstmt.setBigDecimal(4, creditLimit); // CREDIT_POINT

            // Batch에 추가 (실행하지 않고 누적!)
            insertPstmt.addBatch();

            return true;

        } catch (SQLException e) {
            System.err.printf("Batch 추가 실패 - 고객ID: %s, 오류: %s%n", customerId, e.getMessage());
            return false;
        }
    }

    /**
     * 결과 출력
     */
    private static void printResults(long startTime, int processedCount, int insertCount,
                                     int errorCount, int commitCount, int sqlExecutionCount) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        System.out.println("\n=== Batch Processing 처리 결과 ===");
        System.out.printf("총 조회 건수: %,d건 (2013년 이후 가입자만)%n", processedCount);
        System.out.printf("쿠폰 발급 건수: %,d건%n", insertCount);
        System.out.printf("처리 오류 건수: %,d건%n", errorCount);
        System.out.printf("총 처리 시간: %,d ms (%.2f초)%n", executionTime, executionTime / 1000.0);

        // Batch Processing 성능 최적화 분석
        System.out.println("\n=== Batch Processing 성능 최적화 분석 ===");
        System.out.printf("• Hard Parsing: 1회 (최초 PreparedStatement 생성시)%n");
        System.out.printf("• SQL 실행 횟수: %,d회 (기존 %,d회에서 %.1f%% 감소!)%n",
                sqlExecutionCount, insertCount, ((double)(insertCount - sqlExecutionCount) / insertCount) * 100);
        System.out.printf("• Batch Size: %,d건 (일괄 처리 단위)%n", BATCH_SIZE);
        System.out.printf("• 네트워크 라운드트립: %,d회 → %,d회 (대폭 감소)%n", insertCount, sqlExecutionCount);
        System.out.printf("• 배치 효율성: %.1f배 향상 (개별 실행 대비)%n", (double)insertCount / sqlExecutionCount);

        System.out.println("\n=== 기존 최적화 유지 ===");
        System.out.printf("• 데이터 전송량: %,d건 (SQL 조건절)%n", processedCount);
        System.out.printf("• Commit 횟수: %d회 (배치 트랜잭션)%n", commitCount);
        System.out.printf("• Fetch Size: %,d건 (네트워크 최적화)%n", FETCH_SIZE);

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

            if (creditLimit.compareTo(new BigDecimal("1000")) < 0) {
                return "AA";
            } else if (creditLimit.compareTo(new BigDecimal("1000")) >= 0 &&
                    creditLimit.compareTo(new BigDecimal("3000")) < 0) {
                return "BB";
            } else if (creditLimit.compareTo(new BigDecimal("3000")) >= 0 &&
                    creditLimit.compareTo(new BigDecimal("4000")) < 0) {
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
     * 처리 결과 검증
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

                    if (Math.abs(ratio - 100.0) < 1.0) {
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

    /**
     * 리소스 정리
     */
    private static void closeResources(ResultSet rs, Statement selectStmt, PreparedStatement insertPstmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
                System.out.println("ResultSet 정리 완료");
            } catch (SQLException e) {
                System.err.println("ResultSet 정리 중 오류: " + e.getMessage());
            }
        }

        if (selectStmt != null) {
            try {
                selectStmt.close();
                System.out.println("SELECT Statement 정리 완료");
            } catch (SQLException e) {
                System.err.println("SELECT Statement 정리 중 오류: " + e.getMessage());
            }
        }

        if (insertPstmt != null) {
            try {
                insertPstmt.close();
                System.out.println("INSERT PreparedStatement 정리 완료 (Batch Processing 최적화 완료)");
            } catch (SQLException e) {
                System.err.println("INSERT PreparedStatement 정리 중 오류: " + e.getMessage());
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
}