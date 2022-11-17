package kopo.poly.component.impl;

import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IAccessLogUploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;


/**
 * HDFS에 Access_log.gz 파일 업로드 실습 실행을 위한 자바
 */
@Log4j2
@RequiredArgsConstructor
@Component
public class AccessLogUploadExam implements IHdfsExam {

    // access_log.gz 업로드 실습용 서비스(비즈니스 로직)
    private final IAccessLogUploadService accessLogUploadService;

    @Override
    public void doExam() throws Exception {

        HadoopDTO pDTO = new HadoopDTO();

        // 하둡에 저장될 파일 정보
        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("access_log.gz");

        // 내컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");

        accessLogUploadService.uploadFile(pDTO);

    }
}