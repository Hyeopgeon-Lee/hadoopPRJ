package kopo.poly.component.impl;

import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IGzFileService;
import kopo.poly.service.ILogUploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

/**
 * 실습 내용
 * 1. Gz로 압축된 파일 내용 중 10.56.xxx.xxx 로그만 올리기
 * 2. HDFS에 access_log.gz 파일 중 10.56.xxx.xxx 로그만 올리기
 * 3. HDFS에 저장된 파일 내용 보기
 */
@Log4j2
@RequiredArgsConstructor
@Component
public class IpLogUploadExam implements IHdfsExam {

    // IP 대역 검색 서비스
    private final ILogUploadService logUploadService;

    // Gz파일 서비스(파일업로드 및 파일읽기 구현)
    private final IGzFileService gzFileService;

    @Override
    public void doExam() throws Exception {

        HadoopDTO pDTO = null;

        log.info("[실습1.] Gz로 압축된 파일 내용 중 10.56.xxx.xxx 로그만 올리기 ");

        pDTO = new HadoopDTO();

        // 내컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");
        pDTO.setRegExp("10\\.223\\.[0-9]{1,3}\\.[0-9]{1,3}"); // 10.56.xxx.xxx 찾기

        String ipLog = logUploadService.readLocalGzFileIP(pDTO);

        pDTO = null;

        log.info("[실습1.결과] Gz로 압축된 파일 내용 중 10.56.xxx.xxx 로그만 올리기");
        log.info(ipLog);

        log.info("[실습2.] HDFS에 access_log.gz 파일 중 10.56.xxx.xxx 로그만 올리기 ");

        pDTO = new HadoopDTO();

        // 하둡에 생성할 파일 정보
        pDTO.setHadoopUploadPath("/01/02"); // 하둡분산파일시스템 업로드 폴더
        pDTO.setHadoopUploadFileName("10.56.xxx.xxx.log"); // 하둡분산파일시스템 업로드 파일
        pDTO.setFileContents(ipLog); // 하둡분산파일시스템 업로드 파일에 작성될 내용

        // 이전 실습 서비스 활용
        gzFileService.upload10Line(pDTO); // 파일 생성하기

        pDTO = null;

        log.info("[실습3.] HDFS에 저장된 파일 내용 보기! ");

        pDTO = new HadoopDTO();

        // 하둡에 생성할 파일 정보
        pDTO.setHadoopUploadPath("/01/02"); // 하둡분산파일시스템 업로드 폴더
        pDTO.setHadoopUploadFileName("10.56.xxx.xxx.log"); // 하둡분산파일시스템 업로드 파일

        // 이전 실습 서비스 활용
        String hadoopLog = gzFileService.readHadoopFile(pDTO);

        log.info("[실습3.결과] HDFS에 저장된 파일 내용 ");
        log.info(hadoopLog);

        pDTO = null;

    }
}
