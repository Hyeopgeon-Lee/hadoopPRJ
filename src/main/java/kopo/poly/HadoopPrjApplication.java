package kopo.poly;

import kopo.poly.component.IHdfsExam;
import kopo.poly.component.impl.Exam05;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
public class HadoopPrjApplication implements CommandLineRunner {

//    private final IHdfsExam<Exam01> exam01;

//    private final IHdfsExam<Exam02> exam02;

//    private final IHdfsExam<Exam03> exam03;

//    private final IHdfsExam<Exam04> exam04;

    private final IHdfsExam<Exam05> exam05;

    @Override
    public void run(String... args) throws Exception {

        log.info("안녕하세요~~ 하둡 프로그래밍 실습!");

//        log.info("첫번째 실습");
//        exam01.doExam();

//        log.info("두번째 실습");
//        exam02.doExam();

//        log.info("세번째 실습");
//        exam03.doExam();

//        log.info("네번째 실습");
//        exam04.doExam();

        log.info("다섯번째 실습");
        exam05.doExam();

        log.info("안녕하세요~~ 하둡 프로그래밍 실습 끝!");

    }

    public static void main(String[] args) {
        SpringApplication.run(HadoopPrjApplication.class, args);

    }

}
