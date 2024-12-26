# 🌱 Hadoop 분산 파일 시스템 프로그래밍

> **Hadoop Client와 Spring Boot Frameworks를 연동한 분산 파일 시스템 실습 프로젝트**  
> Hadoop 분산 파일 시스템(HDFS)을 활용하여 로그 파일의 업로드, 다운로드 및 데이터 처리 기능을 구현합니다.  
> 공유되는 코드는 한국폴리텍대학 서울강서캠퍼스 빅데이터과 수업에서 사용된 코드입니다.

---

### 📚 **작성자**
- **한국폴리텍대학 서울강서캠퍼스 빅데이터과**  
- **이협건 교수**  
- ✉️ [hglee67@kopo.ac.kr](mailto:hglee67@kopo.ac.kr)  
- 🔗 [빅데이터학과 입학 상담 오픈채팅방](https://open.kakao.com/o/gEd0JIad)

---

## 🚀 주요 실습 내용

1. **Hadoop Client + Spring Boot Frameworks 연동**  
2. **로그파일 업로드**  
3. **로그파일 다운로드**  
4. **로그파일의 일부분 업로드**  
5. **IP 추출 후 특정 데이터만 업로드**

---

## 📩 문의 및 입학 상담

- 📧 **이메일**: [hglee67@kopo.ac.kr](mailto:hglee67@kopo.ac.kr)  
- 💬 **입학 상담 오픈채팅방**: [바로가기](https://open.kakao.com/o/gEd0JIad)

---

## 💡 **우리 학과 소개**
- 한국폴리텍대학 서울강서캠퍼스 빅데이터과는 **클라우드 컴퓨팅, 인공지능, 빅데이터 기술**을 활용하여 소프트웨어 개발자를 양성하는 학과입니다.  
- 학과에 대한 더 자세한 정보는 [학과 홈페이지](https://www.kopo.ac.kr/kangseo/content.do?menu=1547)를 참고하세요.

---

## 📦 **설치 및 실행 방법**

### 1. 레포지토리 클론
- 아래 명령어를 실행하여 레포지토리를 클론합니다.

```bash
git clone https://github.com/Hyeopgeon-Lee/hadoopPRJ.git
cd hadoopPRJ
```

### 2. Hadoop 설정
- Hadoop HDFS가 로컬 또는 클러스터 환경에서 실행 중이어야 합니다.
- application.yml 또는 application.properties 파일에서 Hadoop HDFS 연결 정보를 설정합니다.

### 3. 의존성 설치 및 빌드
- Maven을 사용하여 의존성을 설치하고 애플리케이션을 빌드합니다.

```bash
mvn clean install
```

### 4. 애플리케이션 실행
- 아래 명령어를 실행하여 애플리케이션을 시작합니다.

```bash
mvn spring-boot:run
```
