package kopo.poly;

import kopo.poly.component.IHdfsExam;
import kopo.poly.component.impl.Exam01;
import kopo.poly.component.impl.Exam02;
import kopo.poly.component.impl.Exam04;
import kopo.poly.component.impl.Exam03;
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
    private final IHdfsExam<Exam01> exam01IHdfsExam;

    private final IHdfsExam<Exam02> exam02IHdfsExam;

    private final IHdfsExam<Exam03> exam03IHdfsExam;

    private final IHdfsExam<Exam04> exam04IHdfsExam;

    @Override
    public void run(String... args) throws Exception {

        log.info("안녕하세요~~ 하둡 프로그래밍 실습!");

        log.info("첫번째 실습");
        exam01IHdfsExam.doExam();

        log.info("두번째 실습");
        exam02IHdfsExam.doExam();

        log.info("세번째 실습");
        exam03IHdfsExam.doExam();

        log.info("네번째 실습");
        exam04IHdfsExam.doExam();

        log.info("안녕하세요~~ 하둡 프로그래밍 실습 끝!");

    }

    public static void main(String[] args) {
        SpringApplication.run(HadoopPrjApplication.class, args);

    }

}
