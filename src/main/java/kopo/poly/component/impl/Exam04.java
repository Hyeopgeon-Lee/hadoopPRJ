package kopo.poly.component.impl;

import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IHdfsFileDownloadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 실습 내용
 * 1. HDFS에 저장된 access_log.gz 파일 다운로드
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class Exam04 implements IHdfsExam {

    // HDFS에 저장된 파일 다운로드 서비스
    private final IHdfsFileDownloadService hdfsFileDownloadService;

    @Override
    public void doExam() throws Exception {

        log.info("[실습1.] HDFS에 저장된 access_log.gz 파일 다운로드 ");

        HadoopDTO pDTO = new HadoopDTO();

        // 하둡에 저장될 파일 정보
        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("access_log.gz");

        // 내컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("hdfs_access_log.gz");

        hdfsFileDownloadService.downloadHdfsFile(pDTO);

        pDTO = null;

        log.info("[실습1.결과] 파일다운로드 완료");

    }
}
