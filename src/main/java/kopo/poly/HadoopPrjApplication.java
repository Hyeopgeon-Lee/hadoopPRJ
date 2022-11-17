package kopo.poly;

import kopo.poly.service.IAccessLogUploadService;
import kopo.poly.service.IGzFileService;
import kopo.poly.service.ILogUploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

import java.util.regex.Pattern;

@Log4j2
@RequiredArgsConstructor
@Configuration
@SpringBootApplication
public class HadoopPrjApplication implements CommandLineRunner {

    private final IAccessLogUploadService accessLogUploadService;

    private final IGzFileService gzFileService;

    private final ILogUploadService logUploadService;

    @Override
    public void run(String... args) throws Exception {

//        log.info("안녕하세요~~ 하둡 프로그래밍 실습!");
//
//        HadoopDTO pDTO = new HadoopDTO();
//
//        // 하둡에 저장될 파일 정보
//        pDTO.setHadoopUploadPath("/01/02");
//        pDTO.setHadoopUploadFileName("access_log.gz");
//
//        pDTO.setLocalUploadPath("c:/hadoop_data");
//        pDTO.setLocalUploadFileName("access_log.gz");
//
//        accessLogUploadService.uploadFile(pDTO);
//
//        log.info("간단한 파일 업로드 실습 끝!");
//
//        log.info("Gz로 압축된 파일 최초 10줄만 읽기 실습!");
//
//        pDTO.setLineCnt(10);
//
//        String line10 = gzFileService.readLocalGzFile(pDTO);
//
//        log.info("최초 10줄 읽은 내용 : " + line10);
//
//        log.info("Gz로 압축된 파일 최초 10줄만 읽기 내용을 하둡분산파일시스템에 업로드하기 실습!");
//
//        pDTO.setHadoopUploadPath("/01/02");
//        pDTO.setHadoopUploadFileName("line10.log");
//
//        pDTO.setFileContents(line10);
//        gzFileService.upload10Line(pDTO);
//
//        log.info("하둡분산파일시스템에 업로드된 파일 내용 읽기!");
//
//        String hadoopLog = gzFileService.readHadoopFile(pDTO);
//
//        log.info("하둡분산파일시스템에 저장된 내용 읽기 ");
//        log.info(hadoopLog);
//
//
//        pDTO.setExp("10.56.[0-9]{1,3}.");
//
//        String ipLog = logUploadService.readLocalGzFileIP(pDTO);
//
//        log.info("10.56.xxx.xxx : " + ipLog);


        String text = "10.223.157.186";

        Pattern IPv4_PATTERN = Pattern.compile("10.223.[0-9]{1,3}.[0-9]{1,3}");

        if (IPv4_PATTERN.matcher(text).matches()) {
//        if (Pattern.matches("", text)) {
            log.info("asdfasd");
        } else {
            log.info("asdfasd12341243");

        }


    }

    public static void main(String[] args) {

        // Spring Boot 함수의 시작은 static으로 정의되어 메모리에 올라감
        SpringApplication.run(HadoopPrjApplication.class, args);

    }

}
