package com.kopo.jimin;

import java.sql.*;

public class Calc_Bonus_by_callstmt_2 {

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
            System.out.println("=== 배치 처리 시작 - CallableStatement 방식 2단계 ===");
            System.out.println("처리 방식: PL/SQL Anonymous Block (DB 서버에서 모든 로직 처리)");
            System.out.println("특징: Bulk Collect + Forall (1,000개 단위 Fetch/계산/Insert/Commit)");

            // 1. 데이터베이스 연결
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            conn.setAutoCommit(false); // PL/SQL 내에서 COMMIT 처리
            System.out.println("데이터베이스 연결 성공");

            // 2. 기존 데이터 삭제
            truncateTable(conn);

            // 3. PL/SQL Anonymous Block 생성 (Bulk Collect + Forall 적용)
            // 1000개 단위로 Fetch, 계산, Insert, Commit을 수행합니다.
            String plsqlBlock = """
                DECLARE
                    -- 변수 선언
                    v_processed_count NUMBER := 0;
                    v_insert_count NUMBER := 0;
                    v_commit_count NUMBER := 0;
                    v_error_count NUMBER := 0;
                    
                    -- PL/SQL Collection Types (Nested Table)
                    TYPE customer_id_tt IS TABLE OF CUSTOMER.ID%TYPE;
                    TYPE customer_email_tt IS TABLE OF CUSTOMER.EMAIL%TYPE;
                    TYPE customer_credit_limit_tt IS TABLE OF CUSTOMER.CREDIT_LIMIT%TYPE;
                    TYPE customer_gender_tt IS TABLE OF CUSTOMER.GENDER%TYPE;
                    TYPE customer_address1_tt IS TABLE OF CUSTOMER.ADDRESS1%TYPE;
                    TYPE customer_address2_tt IS TABLE OF CUSTOMER.ADDRESS2%TYPE;
                    TYPE bonus_coupon_cd_tt IS TABLE OF BONUS_COUPON.COUPON_CD%TYPE;
                    TYPE bonus_credit_point_tt IS TABLE OF BONUS_COUPON.CREDIT_POINT%TYPE;
                    
                    l_customer_ids        customer_id_tt;
                    l_customer_emails     customer_email_tt;
                    l_credit_limits       customer_credit_limit_tt;
                    l_genders             customer_gender_tt;
                    l_addresses1          customer_address1_tt;
                    l_addresses2          customer_address2_tt;
                    
                    l_coupon_codes        bonus_coupon_cd_tt;
                    l_final_credit_points bonus_credit_point_tt;
                    
                    -- Cursor 선언: 2013년 이후 가입 고객 (Bulk Collect용)
                    CURSOR customer_cursor IS
                        SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2
                        FROM CUSTOMER 
                        WHERE ENROLL_DT >= DATE '2013-01-01'
                          AND CREDIT_LIMIT IS NOT NULL 
                          AND EMAIL IS NOT NULL 
                          AND ID IS NOT NULL
                        ORDER BY ID;
                    
                    -- 쿠폰 코드 계산 함수 (동일)
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
                    DBMS_OUTPUT.PUT_LINE('=== PL/SQL 쿠폰 발급 처리 시작 (BULK COLLECT + FORALL) ===');
                    DBMS_OUTPUT.PUT_LINE('처리 방식: ' || (1000) || ' Row 단위 Fetch/계산/Insert/Commit');
                    
                    OPEN customer_cursor;
                    LOOP
                        -- Bulk Collect: 1000개 단위로 데이터를 컬렉션에 가져오기
                        FETCH customer_cursor BULK COLLECT INTO 
                            l_customer_ids, l_customer_emails, l_credit_limits, 
                            l_genders, l_addresses1, l_addresses2
                        LIMIT 1000; -- 한 번에 가져올 행 수 지정
                        
                        EXIT WHEN l_customer_ids.COUNT = 0; -- 더 이상 가져올 데이터가 없으면 루프 종료
                        
                        -- 컬렉션 초기화 및 크기 조정
                        l_coupon_codes := bonus_coupon_cd_tt(); -- 중요: 매번 새로운 배치 처리를 위해 재초기화
                        l_final_credit_points := bonus_credit_point_tt(); -- 중요: 매번 새로운 배치 처리를 위해 재초기화
                        
                        l_coupon_codes.EXTEND(l_customer_ids.COUNT);
                        l_final_credit_points.EXTEND(l_customer_ids.COUNT);
                        
                        -- Bulk 처리된 각 행에 대해 쿠폰 코드 계산
                        FOR i IN 1..l_customer_ids.COUNT LOOP
                            DECLARE
                                v_current_full_address VARCHAR2(200);
                            BEGIN
                                v_processed_count := v_processed_count + 1;
                                
                                v_current_full_address := NVL(l_addresses1(i), '') || ' ' || NVL(l_addresses2(i), '');
                                
                                l_coupon_codes(i) := calculate_coupon_code(
                                    l_credit_limits(i),
                                    l_genders(i),
                                    v_current_full_address
                                );
                                l_final_credit_points(i) := l_credit_limits(i);
                                
                                IF l_coupon_codes(i) IS NOT NULL THEN
                                    v_insert_count := v_insert_count + 1;
                                END IF;
                                
                            EXCEPTION
                                WHEN OTHERS THEN
                                    v_error_count := v_error_count + 1;
                                    DBMS_OUTPUT.PUT_LINE('데이터 처리 중 오류 (고객ID: ' || l_customer_ids(i) || '): ' || SQLERRM);
                                    l_coupon_codes(i) := NULL; -- 오류 발생 시 쿠폰 발급 안함
                                    l_final_credit_points(i) := NULL;
                            END;
                        END LOOP;
                        
                        -- 유효한 쿠폰만 담을 새로운 컬렉션을 만들어 FORALL에 전달
                        DECLARE
                            -- **수정: 매 루프마다 명시적으로 초기화**
                            l_valid_customer_ids        customer_id_tt := customer_id_tt();
                            l_valid_customer_emails     customer_email_tt := customer_email_tt();
                            l_valid_coupon_codes        bonus_coupon_cd_tt := bonus_coupon_cd_tt();
                            l_valid_credit_points       bonus_credit_point_tt := bonus_credit_point_tt();
                        BEGIN
                            FOR i IN 1..l_customer_ids.COUNT LOOP
                                IF l_coupon_codes(i) IS NOT NULL THEN
                                    l_valid_customer_ids.EXTEND; l_valid_customer_ids(l_valid_customer_ids.COUNT) := l_customer_ids(i);
                                    l_valid_customer_emails.EXTEND; l_valid_customer_emails(l_valid_customer_emails.COUNT) := l_customer_emails(i);
                                    l_valid_coupon_codes.EXTEND; l_valid_coupon_codes(l_valid_coupon_codes.COUNT) := l_coupon_codes(i);
                                    l_valid_credit_points.EXTEND; l_valid_credit_points(l_valid_credit_points.COUNT) := l_final_credit_points(i);
                                END IF;
                            END LOOP;
                            
                            IF l_valid_customer_ids.COUNT > 0 THEN -- **중요: 비어있지 않을 때만 FORALL 실행**
                                FORALL i IN 1..l_valid_customer_ids.COUNT
                                    INSERT INTO BONUS_COUPON 
                                    (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT)
                                    VALUES ('202506', l_valid_customer_ids(i), l_valid_customer_emails(i), 
                                            l_valid_coupon_codes(i), l_valid_credit_points(i), NULL);
                            END IF;
                        END; 
                                    
                        DBMS_OUTPUT.PUT_LINE('BULK INSERT 완료: ' || l_customer_ids.COUNT || '건 (총 ' || v_insert_count || '건 처리)');
                        
                        COMMIT; -- 1000개 단위 (FETCH LIMIT) 로 COMMIT
                        v_commit_count := v_commit_count + 1;
                        DBMS_OUTPUT.PUT_LINE('Commit 실행: ' || v_insert_count || '건 처리됨 (총 ' || v_commit_count || '회 Commit)');
                        
                        -- 진행률 출력 (50,000건 단위)
                        IF MOD(v_insert_count, 50000) = 0 THEN
                            DBMS_OUTPUT.PUT_LINE('쿠폰 발급 진행률: ' || v_insert_count || '건 발급됨 (PL/SQL BULK 고속 처리!)');
                        END IF;
                        
                        -- 전체 진행률 출력 (100,000건 단위)
                        IF MOD(v_processed_count, 100000) = 0 THEN
                            DBMS_OUTPUT.PUT_LINE('전체 처리 진행률: ' || v_processed_count || '건 조회됨');
                        END IF;
                        
                    END LOOP;
                    CLOSE customer_cursor;
                    
                    -- 최종 결과 출력
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE('=== PL/SQL 처리 결과 (BULK COLLECT + FORALL) ===');
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
                        IF customer_cursor%ISOPEN THEN
                            CLOSE customer_cursor;
                        END IF;
                        RAISE;
                END;
                """;

            // 4. CallableStatement 생성 및 실행
            callStmt = conn.prepareCall(plsqlBlock);

            // OUT 매개변수 등록
            callStmt.registerOutParameter(1, Types.INTEGER); // 처리 건수
            callStmt.registerOutParameter(2, Types.INTEGER); // 발급 건수
            callStmt.registerOutParameter(3, Types.INTEGER); // 오류 건수
            callStmt.registerOutParameter(4, Types.INTEGER); // 커밋 횟수

            System.out.println("CallableStatement 실행 시작 (PL/SQL BULK 처리)...");

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
                    System.err.println("트랜잭션이 롤백되었습니다."); // err 스트림으로 변경
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

        System.out.println("\n=== CallableStatement (PL/SQL BULK) 처리 결과 ===");
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
     * 리소스 정리 (동일)
     */
    private static void closeResources(CallableStatement callStmt, Connection conn) {
        if (callStmt != null) {
            try {
                callStmt.close();
                System.out.println("CallableStatement 정리 완료 (PL/SQL BULK 처리 완료)");
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