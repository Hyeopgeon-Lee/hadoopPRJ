package kopo.poly.component.impl;

import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IGzFileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

/**
 * 실습 내용
 * 1. Gz로 압축된 파일 최초 10줄만 읽기 실습
 * 2. HDFS에 access_log.gz 파일 중 최초 10줄 올리기
 * 3. HDFS에 저장된 파일 내용 보기
 */
@Log4j2
@RequiredArgsConstructor
@Component
public class GzFileExam implements IHdfsExam {

    // Gz파일 서비스
    private final IGzFileService gzFileService;

    @Override
    public void doExam() throws Exception {

        HadoopDTO pDTO = null;

        log.info("[실습1.] Gz로 압축된 파일 최초 10줄만 읽기 실습! ");

        pDTO = new HadoopDTO();

        // 내컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");
        pDTO.setLineCnt(10); // 읽을 라인 수

        String line10 = gzFileService.readLocalGzFile(pDTO);

        pDTO = null;

        log.info("[실습1.결과] 최초 10줄 읽은 내용 : " + line10);


        log.info("[실습2.] HDFS에 access_log.gz 파일 중 최초 10줄 올리기! ");

        pDTO = new HadoopDTO();

        // 하둡에 생성할 파일 정보
        pDTO.setHadoopUploadPath("/01/02"); // 하둡분산파일시스템 업로드 폴더
        pDTO.setHadoopUploadFileName("line10.log"); // 하둡분산파일시스템 업로드 파일
        pDTO.setFileContents(line10); // 하둡분산파일시스템 업로드 파일에 작성될 내용

        gzFileService.upload10Line(pDTO); // 파일 생성하기

        pDTO = null;

        log.info("[실습3.] HDFS에 저장된 파일 내용 보기! ");

        pDTO = new HadoopDTO();

        // 하둡에 생성할 파일 정보
        pDTO.setHadoopUploadPath("/01/02"); // 하둡분산파일시스템 업로드 폴더
        pDTO.setHadoopUploadFileName("line10.log"); // 하둡분산파일시스템 업로드 파일

        String hadoopLog = gzFileService.readHadoopFile(pDTO);

        log.info("[실습3.결과] HDFS에 저장된 파일 내용 ");
        log.info(hadoopLog);

        pDTO = null;

    }
}
