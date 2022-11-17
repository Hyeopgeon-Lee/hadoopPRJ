package kopo.poly;

import kopo.poly.component.IHdfsExam;
import kopo.poly.component.impl.AccessLogUploadExam;
import kopo.poly.component.impl.GzFileExam;
import kopo.poly.component.impl.HdfsFileDownloadExam;
import kopo.poly.component.impl.IpLogUploadExam;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

@Log4j2
@RequiredArgsConstructor
@Configuration
@SpringBootApplication
public class HadoopPrjApplication implements CommandLineRunner {

    // 재네릭(Generic) 타입 활용하여 동일한 interface를 구현했을 때 객체 구분함
    private final IHdfsExam<AccessLogUploadExam> accessLogUploadExamIHdfsExam;

    private final IHdfsExam<GzFileExam> gzFileExamIHdfsExam;

    private final IHdfsExam<IpLogUploadExam> ipLogUploadExamIHdfsExam;

    private final IHdfsExam<HdfsFileDownloadExam> hdfsFileDownloadExamIHdfsExam;

    @Override
    public void run(String... args) throws Exception {

        log.info("안녕하세요~~ 하둡 프로그래밍 실습!");

        log.info("첫번째 실습");
        accessLogUploadExamIHdfsExam.doExam();

        log.info("두번째 실습");
        gzFileExamIHdfsExam.doExam();

        log.info("세번째 실습");
        ipLogUploadExamIHdfsExam.doExam();
        
        log.info("네번째 실습");
        hdfsFileDownloadExamIHdfsExam.doExam();
        
        log.info("안녕하세요~~ 하둡 프로그래밍 실습 끝!");

    }

    public static void main(String[] args) {
        SpringApplication.run(HadoopPrjApplication.class, args);

    }

}
