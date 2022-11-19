package kopo.poly;

import kopo.poly.component.impl.Exam01;
import kopo.poly.component.impl.Exam02;
import kopo.poly.component.impl.Exam03;
import kopo.poly.component.impl.Exam04;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

@Log4j
@RequiredArgsConstructor
@Configuration
@SpringBootApplication
public class HadoopPrjApplication implements CommandLineRunner {

    private final Exam01 exam01;

    private final Exam02 exam02;

    private final Exam03 exam03;

    private final Exam04 exam04;

    @Override
    public void run(String... args) throws Exception {

        log.info("안녕하세요~~ 하둡 프로그래밍 실습!");

        log.info("첫번째 실습");
        exam01.doExam();

        log.info("두번째 실습");
        exam02.doExam();

        log.info("세번째 실습");
        exam03.doExam();
//
//        log.info("네번째 실습");
//        exam04.doExam();

        log.info("안녕하세요~~ 하둡 프로그래밍 실습 끝!");

    }

    public static void main(String[] args) {
        SpringApplication.run(HadoopPrjApplication.class, args);

    }

}
