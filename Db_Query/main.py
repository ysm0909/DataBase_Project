import mysql.connector

# 데이터베이스 연결
def connect_to_db():
    cur = mysql.connector.connect(user='ysm', password='0504',
                                  host='localhost',
                                  database='hospitaldb')
    return cur

# 데이터베이스 연결 종료
def close_db(cur):
    if cur:
        cur.close()

# SQL 쿼리 실행 후 결과 반환
def execute_query(cur, query):
    cursor = cur.cursor()
    cursor.execute(query)
    field_names = [i[0] for i in cursor.description]
    result = cursor.fetchall()
    return field_names, result

# 메인 함수
def main():
    cur = connect_to_db()
    while True:
        # 메뉴 출력
        print("[정보 검색]")
        print("1. 모든 환자의 진료 내역 확인")
        print("2. 병원 내 소속 의사 명단 확인")
        print("3. 가장 많이 진료한 진료과 확인")
        print("4. 분기별 병원 내 전체 진료 수익 확인")
        print("5. 소속별 의사의 평균 진료 횟수 확인")
        print("6. 가장 많은 진료를 수행한 의사 확인")
        print("[특정 테이블 입력]")
        print("7. 특정 환자의 진료 내역 확인")
        print("8. 특정 질병으로 내원한 환자의 진료 내역 확인")
        print("9. 특정 진료과별 소속 의사 명단 확인")
        print("10. 특정 의사가 진료한 환자 정보 확인")
        print("0. 종료")
        choice = input("번호를 입력해주세요: ")

        # 사용자 입력에 따라 쿼리 생성 및 실행
        if choice == '1':
            query = """
                    SELECT 
                        MR.record_id AS "진료ID",
                        MR.record_year AS "년도",
                        MR.record_month AS "월",
                        MR.record_day AS "일",
                        P.patient_name AS "환자",
                        P.patient_sex AS "성별",
                        P.patient_birth AS "생년월일",
                        DS.disease_name AS "질병명",
                        D.doctor_name AS "담당 의사",
                        MR.cost AS "진료비"
                    FROM 
                        MedicalRecords MR
                    JOIN 
                        Patients P ON MR.patient_id = P.patient_id
                    JOIN 
                        Doctors D ON MR.doctor_id = D.doctor_id
                    JOIN 
                        Diseases DS ON MR.disease_id = DS.disease_id
                    ORDER BY 
                        년도 DESC, 월 DESC, 일 DESC;
                    """

        elif choice == '2':
            query = """
                    SELECT 
                        MF.majorfield AS "소속",
                        MF.minorfield AS "진료과",
                        D.doctor_name AS "의사이름"
                    FROM 
                        Doctors D
                    JOIN 
                        MedicalFields MF ON D.field_id = MF.field_id;
                    """

        elif choice == '3':
            query = """
                    SELECT 
                        MF.minorfield AS "진료과",
                        MF.majorfield AS "소속",
                        COUNT(MR.record_id) AS "진료횟수"
                    FROM 
                        MedicalRecords MR
                    JOIN 
                        Doctors D ON MR.doctor_id = D.doctor_id
                    JOIN 
                        MedicalFields MF ON D.field_id = MF.field_id
                    GROUP BY 
                        MF.minorfield, MF.majorfield
                    ORDER BY 
                        진료횟수 DESC;
                    """

        elif choice == '4':
            query = """
                    SELECT 
                        MR.record_year AS "년도",
                        CASE 
                            WHEN MR.record_month BETWEEN 1 AND 6 THEN '1분기'
                            WHEN MR.record_month BETWEEN 7 AND 12 THEN '2분기'
                        END AS "분기",
                        SUM(MR.cost) AS "총 진료수익"
                    FROM 
                        MedicalRecords MR
                    GROUP BY 
                        MR.record_year,  분기
                    ORDER BY 
                        MR.record_year DESC, 분기 ASC;
                    """

        elif choice == '5':
            query = """
                    SELECT 
                        MF.majorfield AS "진료과",
                        AVG(CASE WHEN MR.record_id IS NOT NULL THEN 1 ELSE 0 END) AS "평균 진료 횟수",
                        COUNT(D.doctor_id) AS "의사 수"
                    FROM 
                        MedicalFields MF
                    LEFT JOIN 
                        Doctors D ON MF.field_id = D.field_id
                    LEFT JOIN 
                        MedicalRecords MR ON D.doctor_id = MR.doctor_id
                    GROUP BY 
                        MF.majorfield;
                    """

        elif choice == '6':
            query = """
                    WITH RankedDoctors AS (
                        SELECT 
                            DENSE_RANK() OVER (ORDER BY COUNT(MR.record_id) DESC) AS 순위,
                            COUNT(MR.record_id) AS 진료횟수,
                            D.doctor_name AS 의사,
                            MF.majorfield AS 소속,
                            MF.minorfield AS 진료과
                        FROM 
                            MedicalRecords MR
                        JOIN 
                            Doctors D ON MR.doctor_id = D.doctor_id
                        JOIN 
                            MedicalFields MF ON D.field_id = MF.field_id
                        GROUP BY 
                            D.doctor_id
                    )
                    
                    SELECT 
                        순위, 진료횟수, 의사, 소속, 진료과
                    FROM 
                        RankedDoctors
                    WHERE 
                        순위 = 1;
                    """

        elif choice == '7':
            pati_name = input("환자의 이름을 입력해주세요.: ")
            query = f"""
                    SELECT 
                        MR.record_id AS "진료ID",
                        MR.record_year AS "년도",
                        MR.record_month AS "월",
                        MR.record_day AS "일",
                        P.patient_name AS "환자",
                        P.patient_sex AS "성별",
                        P.patient_birth AS "생년월일",
                        DS.disease_name AS "질병명",
                        D.doctor_name AS "담당 의사",
                        MR.cost AS "진료비"
                    FROM 
                        MedicalRecords MR
                    JOIN 
                        Patients P ON MR.patient_id = P.patient_id
                    JOIN 
                        Doctors D ON MR.doctor_id = D.doctor_id
                    JOIN 
                        Diseases DS ON MR.disease_id = DS.disease_id
                    WHERE 
                        P.patient_name = '{pati_name}'
                    ORDER BY 
                        MR.record_year DESC, MR.record_month DESC, MR.record_day DESC;
                    """
        elif choice == '8':
            dis_name = input("질병명을 입력해주세요.: ")
            query = f"""
                    SELECT 
                        DS.disease_name AS "질병명",
                        P.patient_name AS "환자",
                        P.patient_sex AS "성별",
                        P.patient_birth AS "생년월일",
                        MR.record_year AS "방문년도",
                        MR.record_month AS "방문월",
                        MR.record_day AS "방문일"
                    FROM 
                        MedicalRecords MR
                    JOIN 
                        Patients P ON MR.patient_id = P.patient_id
                    JOIN 
                        Diseases DS ON MR.disease_id = DS.disease_id
                    WHERE 
                        DS.disease_name LIKE '%{dis_name}%'
                    ORDER BY 
                        MR.record_year DESC, MR.record_month DESC, MR.record_day DESC, DS.disease_name, P.patient_name;
                    """

        elif choice == '9':
            fields = input("진료분야를 입력해주세요.[내과, 외과, 소아청소년과, 기타]: ")
            query = f"""
                    SELECT 
                        MF.minorfield AS "진료과",
                        D.doctor_name AS "의사 이름",
                        D.doctor_phone AS "전화번호",
                        D.doctor_email AS "이메일",
                        D.specialized_field AS "전문 분야"
                    FROM 
                        Doctors D
                    JOIN 
                        MedicalFields MF ON D.field_id = MF.field_id
                    WHERE 
                        MF.majorfield = '{fields}';
                    """

        elif choice == '10':
            doc_name = input("의사이름을 입력해주세요.: ")
            query = f"""
                    SELECT 
                        P.patient_name AS "환자 이름",
                        P.patient_sex AS "성별",
                        P.patient_birth AS "생년월일",
                        DS.disease_name AS "진단된 질병",
                        MR.record_year AS "진료 년도",
                        MR.record_month AS "진료 월",
                        MR.record_day AS "진료 일",
                        MR.cost AS "진료비"
                    FROM 
                        MedicalRecords MR
                    JOIN 
                        Patients P ON MR.patient_id = P.patient_id
                    JOIN 
                        Doctors D ON MR.doctor_id = D.doctor_id
                    JOIN 
                        Diseases DS ON MR.disease_id = DS.disease_id
                    WHERE 
                        D.doctor_name = '{doc_name}';
                    """

        elif choice == '0':
            break

        else:
            print("잘못된 입력입니다. 다시 시도해주세요.")
            continue

            # 쿼리 실행 후 결과 출력
        field_names, result = execute_query(cur, query)
        print(field_names)
        for row in result:
            print(row)

    close_db(cur)

if __name__ == "__main__":
    main()