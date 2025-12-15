package com.kopo.jimin;

import java.sql.*;

/**
 * 배치 처리 성능 비교 - CallableStatement 방식 1단계
 * 특징: Anonymous Block(PL/SQL)으로 쿠폰 계산 로직 구현 후 Java의 CallableStatement 객체 사용
 * 처리방식: Cursor 사용, 1Row 단위 Fetch, 1Row 단위 쿠폰계산, 1Row 단위 Insert, 10,000 단위 Commit
 * 효과: 모든 로직을 DB 서버에서 처리 (네트워크 트래픽 최소화)
 */
public class Calc_Bonus_by_callstmt_1 {

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
            System.out.println("=== 배치 처리 시작 - CallableStatement 방식 1단계 ===");
            System.out.println("처리 방식: PL/SQL Anonymous Block (DB 서버에서 모든 로직 처리)");
            System.out.println("특징: Cursor + 1Row 단위 처리 + 10,000건 단위 Commit");

            // 1. 데이터베이스 연결
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            conn.setAutoCommit(false);
            System.out.println("데이터베이스 연결 성공");

            // 2. 기존 데이터 삭제
            truncateTable(conn);

            // 3. PL/SQL Anonymous Block 생성 (쿠폰 계산 로직 포함)
            String plsqlBlock = """
                DECLARE
                    -- 변수 선언
                    v_processed_count NUMBER := 0;
                    v_insert_count NUMBER := 0;
                    v_commit_count NUMBER := 0;
                    v_error_count NUMBER := 0;
                    v_coupon_code VARCHAR2(10);
                    v_full_address VARCHAR2(200);
                    
                    -- Cursor 선언: 2013년 이후 가입 고객
                    CURSOR customer_cursor IS
                        SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2, ENROLL_DT
                        FROM CUSTOMER 
                        WHERE ENROLL_DT >= DATE '2013-01-01'
                          AND CREDIT_LIMIT IS NOT NULL 
                          AND EMAIL IS NOT NULL 
                          AND ID IS NOT NULL
                        ORDER BY ID;
                    
                    -- 쿠폰 코드 계산 함수
                    FUNCTION calculate_coupon_code(
                        p_credit_limit NUMBER,
                        p_gender VARCHAR2,
                        p_address VARCHAR2
                    ) RETURN VARCHAR2 IS
                    BEGIN
                        IF p_credit_limit < 1000 THEN
                            RETURN 'AA';
                        ELSIF p_credit_limit >= 1000 AND p_credit_limit < 3000 THEN
                            RETURN 'BB';
                        ELSIF p_credit_limit >= 3000 AND p_credit_limit < 4000 THEN
                            -- 특별 조건: 송파구 풍납1동 거주 여성 고객
                            IF p_gender = 'F' AND p_address IS NOT NULL AND
                               INSTR(p_address, '송파구') > 0 AND INSTR(p_address, '풍납1동') > 0 THEN
                                RETURN 'C2';
                            ELSE
                                RETURN 'CC';
                            END IF;
                        ELSIF p_credit_limit >= 4000 THEN
                            RETURN 'DD';
                        ELSE
                            RETURN NULL;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            RETURN NULL;
                    END calculate_coupon_code;
                    
                BEGIN
                    -- 시작 메시지
                    DBMS_OUTPUT.PUT_LINE('=== PL/SQL 쿠폰 발급 처리 시작 ===');
                    DBMS_OUTPUT.PUT_LINE('처리 방식: Cursor + 1Row 단위 Fetch/계산/Insert');
                    
                    -- Cursor를 통한 1Row 단위 처리
                    FOR customer_rec IN customer_cursor LOOP
                        BEGIN
                            v_processed_count := v_processed_count + 1;
                            
                            -- 주소 연결
                            v_full_address := NVL(customer_rec.ADDRESS1, '') || ' ' || NVL(customer_rec.ADDRESS2, '');
                            
                            -- 1Row 단위 쿠폰 코드 계산
                            v_coupon_code := calculate_coupon_code(
                                customer_rec.CREDIT_LIMIT,
                                customer_rec.GENDER,
                                v_full_address
                            );
                            
                            -- 쿠폰 코드가 유효한 경우 1Row 단위 INSERT
                            IF v_coupon_code IS NOT NULL THEN
                                INSERT INTO BONUS_COUPON 
                                (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT)
                                VALUES ('202506', customer_rec.ID, customer_rec.EMAIL, 
                                        v_coupon_code, customer_rec.CREDIT_LIMIT, NULL);
                                
                                v_insert_count := v_insert_count + 1;
                                
                                -- 10,000건 단위 Commit
                                IF MOD(v_insert_count, 10000) = 0 THEN
                                    COMMIT;
                                    v_commit_count := v_commit_count + 1;
                                    DBMS_OUTPUT.PUT_LINE('Commit 실행: ' || v_insert_count || '건 처리됨 (총 ' || v_commit_count || '회 Commit)');
                                END IF;
                                
                                -- 진행률 출력 (50,000건 단위)
                                IF MOD(v_insert_count, 50000) = 0 THEN
                                    DBMS_OUTPUT.PUT_LINE('쿠폰 발급 진행률: ' || v_insert_count || '건 발급됨 (PL/SQL 고속 처리!)');
                                END IF;
                            END IF;
                            
                            -- 전체 진행률 출력 (100,000건 단위)
                            IF MOD(v_processed_count, 100000) = 0 THEN
                                DBMS_OUTPUT.PUT_LINE('전체 처리 진행률: ' || v_processed_count || '건 조회됨');
                            END IF;
                            
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_error_count := v_error_count + 1;
                                DBMS_OUTPUT.PUT_LINE('행 처리 중 오류 (고객ID: ' || customer_rec.ID || '): ' || SQLERRM);
                                
                                -- 너무 많은 오류시 중단
                                IF v_error_count > 1000 THEN
                                    DBMS_OUTPUT.PUT_LINE('오류가 너무 많이 발생했습니다. 처리를 중단합니다.');
                                    EXIT;
                                END IF;
                        END;
                    END LOOP;
                    
                    -- 마지막 남은 데이터 Commit
                    IF MOD(v_insert_count, 10000) != 0 THEN
                        COMMIT;
                        v_commit_count := v_commit_count + 1;
                        DBMS_OUTPUT.PUT_LINE('최종 Commit 실행: ' || v_insert_count || '건 처리 완료 (총 ' || v_commit_count || '회 Commit)');
                    END IF;
                    
                    -- 최종 결과 출력
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE('=== PL/SQL 처리 결과 ===');
                    DBMS_OUTPUT.PUT_LINE('총 조회 건수: ' || v_processed_count || '건');
                    DBMS_OUTPUT.PUT_LINE('쿠폰 발급 건수: ' || v_insert_count || '건');
                    DBMS_OUTPUT.PUT_LINE('처리 오류 건수: ' || v_error_count || '건');
                    DBMS_OUTPUT.PUT_LINE('총 Commit 횟수: ' || v_commit_count || '회');
                    
                    -- 출력 매개변수 설정
                    ? := v_processed_count;  -- OUT 매개변수 1: 처리 건수
                    ? := v_insert_count;     -- OUT 매개변수 2: 발급 건수  
                    ? := v_error_count;      -- OUT 매개변수 3: 오류 건수
                    ? := v_commit_count;     -- OUT 매개변수 4: 커밋 횟수
                    
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        DBMS_OUTPUT.PUT_LINE('심각한 오류 발생: ' || SQLERRM);
                        RAISE;
                END;
                """;

            System.out.println("PL/SQL Anonymous Block 생성 완료");
            System.out.println("✅ 모든 로직을 DB 서버에서 처리 (Client ↔ DB 트래픽 최소화)");

            // 4. CallableStatement 생성 및 실행
            callStmt = conn.prepareCall(plsqlBlock);

            // OUT 매개변수 등록
            callStmt.registerOutParameter(1, Types.INTEGER); // 처리 건수
            callStmt.registerOutParameter(2, Types.INTEGER); // 발급 건수
            callStmt.registerOutParameter(3, Types.INTEGER); // 오류 건수
            callStmt.registerOutParameter(4, Types.INTEGER); // 커밋 횟수

            System.out.println("CallableStatement 실행 시작...");
            System.out.println("(모든 처리가 DB 서버에서 진행됩니다)");

            // PL/SQL 블록 실행
            callStmt.execute();

            // 결과 가져오기
            int processedCount = callStmt.getInt(1);
            int insertCount = callStmt.getInt(2);
            int errorCount = callStmt.getInt(3);
            int commitCount = callStmt.getInt(4);

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
            // 7. 리소스 정리
            closeResources(callStmt, conn);

            // 최종 처리 결과
            long endTime = System.currentTimeMillis();
            System.out.printf("%n=== 처리 완료 ===\n총 처리 시간: %,d ms%n", (endTime - startTime));
        }
    }

    /**
     * 결과 출력
     */
    private static void printResults(long startTime, int processedCount, int insertCount,
                                     int errorCount, int commitCount) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        System.out.println("\n=== CallableStatement (PL/SQL) 처리 결과 ===");
        System.out.printf("총 조회 건수: %,d건 (2013년 이후 가입자)%n", processedCount);
        System.out.printf("쿠폰 발급 건수: %,d건%n", insertCount);
        System.out.printf("처리 오류 건수: %,d건%n", errorCount);
        System.out.printf("총 Commit 횟수: %d회%n", commitCount);
        System.out.printf("총 처리 시간: %,d ms (%.2f초)%n", executionTime, executionTime / 1000.0);

        if (errorCount > 0) {
            System.out.printf("• 오류율: %.2f%%\n", ((double)errorCount / processedCount) * 100);
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
    private static void closeResources(CallableStatement callStmt, Connection conn) {
        if (callStmt != null) {
            try {
                callStmt.close();
                System.out.println("CallableStatement 정리 완료 (PL/SQL 처리 완료)");
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
}