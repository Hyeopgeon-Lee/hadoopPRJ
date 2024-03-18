package kopo.poly.component.impl;

import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IHdfsFileReadService;
import kopo.poly.service.IHdfsFileUploadService;
import kopo.poly.service.ILocalGzFileReadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class Exam04 implements IHdfsExam {

    // Gz파일 서비스
    private final ILocalGzFileReadService localGzFileReadService;

    // HDFS 파일업로드 서비스
    private final IHdfsFileUploadService hdfsFileUploadService;

    // HDFS 업로드된 파일 내용보기
    private final IHdfsFileReadService hdfsFileReadService;

    @Override
    public void doExam() throws Exception {

        HadoopDTO pDTO = null;

        log.info("[1단계] Gz로 압축된 파일 내용 중 최초 10줄 읽기");

        pDTO = new HadoopDTO();

        // 내컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");
        pDTO.setRegExp("10\\.56\\.[0-9]{1,3}\\.[0-9]{1,3}"); // 10.56.xxx.xxx 찾기

        List<String> ipLog = localGzFileReadService.readLocalGzFileIP(pDTO);

        pDTO = null;

        log.info("[2단계.] 1단계에서 가져온 최초 10줄을 하둡에 업로드하기 ");

        pDTO = new HadoopDTO();

        // 하둡에 생성할 파일 정보
        pDTO.setHadoopUploadPath("/01/02"); // 하둡분산파일시스템 업로드 폴더
        pDTO.setHadoopUploadFileName("ip.log"); // 하둡분산파일시스템 업로드 파일
        pDTO.setContentList(ipLog); // 하둡분산파일시스템 업로드 파일에 작성될 내용

        // HDFS에 파일업로드
        hdfsFileUploadService.uploadHdfsFileContents(pDTO);

        pDTO = null;

        log.info("[3단계] 하둡에 저장된 파일 내용 보기! ");

        pDTO = new HadoopDTO();

        // 하둡에 생성할 파일 정보
        pDTO.setHadoopUploadPath("/01/02"); // 하둡분산파일시스템 업로드된 폴더
        pDTO.setHadoopUploadFileName("ip.log"); // 하둡분산파일시스템 업로드된 파일

        // HDFS 파일 내용보기
        String hadoopLog = hdfsFileReadService.readHdfsFile(pDTO);

        log.info("[3단계 결과] 하둡에 저장된 파일 내용 ");

        // IP 결과 로그 결과가 많아 5개만 출력되게 프로그램 작성함
        Arrays.stream(hadoopLog.split("\n")).limit(5).forEach(
                readLog -> log.info(readLog)
        );

        pDTO = null;

    }
}
