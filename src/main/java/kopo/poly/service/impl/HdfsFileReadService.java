package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IHdfsFileReadService;
import kopo.poly.util.CmmUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class HdfsFileReadService implements IHdfsFileReadService {

    private final Configuration hadoopConfig;

    @Override
    public String readHdfsFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".readHdfsFile Start!");

        // 하둡 분산 파일 시스템 객체
        FileSystem hdfs = FileSystem.get(hadoopConfig);

        // 하둡분산파일시스템에 저장될 파일경로 및 폴더명
        // 예 : hadoop fs -put access_log.gz /01/02/access_log.gz
        String hadoopFile = CmmUtil.nvl(pDTO.getHadoopUploadPath()) + "/"
                + CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        // 하둡분산파일시스템에 저장가능한 객체로 변환
        Path path = new Path(hadoopFile);

        // 기존 하둡에 존재하는지 로그 찍어보기
        log.info("HDFS Exists : " + hdfs.exists(path));

        String readLog = "";

        if (hdfs.exists(path)) { // 하둡분산파일시스템에 파일 존재하면...
            // 하둡분산파일시스템의 파일 읽기
            // FSDataOutputStream 객체는 ByteBufferReadable를 상속하여 구현함
            FSDataInputStream inputStream = hdfs.open(path);

            // 하둡분산파일시스템의 파일 크기만큼 배열 크기 만들기
            byte[] rawData = new byte[inputStream.available()];

            inputStream.readFully(rawData); // 하둡분산파일시스템의 전체 내용 읽기

            readLog = new String(rawData); // 바이트 배열 구조를 문자열로 변환하기

            inputStream.close();
        }

        log.info(this.getClass().getName() + ".readHdfsFile End!");

        return readLog;
    }

}
