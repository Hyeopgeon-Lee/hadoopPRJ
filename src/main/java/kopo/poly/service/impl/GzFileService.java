package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IAccessLogUploadService;
import kopo.poly.service.IGzFileService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.zip.GZIPInputStream;

@Log4j2
@Service
public class GzFileService implements IGzFileService {



    @Override
    public String readLocalGzFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".readLocalGzFile Start!");

        String result = ""; // GzFile 내용 결과

        long readCnt = pDTO.getLineCnt();

        log.info("readCnt : " + readCnt);

        // 예 : c:/hadoop_data/access_log.gz
        String localFile = CmmUtil.nvl(pDTO.getLocalUploadPath()) +
                "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName());

        log.info("localFile : " + localFile);

        // Gz 파일 가져오기
        File localGzFile = new File(localFile);

        // 파일 처리를 위해 파일을 스트림 형태로 변환
        FileInputStream fis = new FileInputStream(localGzFile);

        // Gz 파일로 압축된 파일스트림을 압축해제한 스트림으로 변경
        GZIPInputStream gzis = new GZIPInputStream(fis);

        // Gz스트림 내용을 읽기
        InputStreamReader streamReader = new InputStreamReader(gzis);

        // 스트림으로 내용을 읽는 경우는 무조건 BufferedReader을 통해 읽자!
        try (BufferedReader lineReader = new BufferedReader(streamReader)) {

            // String 객체보다 처리 속도를 빠르게 하기 위해 StringBuilder 사용
            StringBuilder gzContents = new StringBuilder();

            long idx = 0; //읽은 라인 횟수

            String line; // 읽은 라인의 값이 저장되는 변수

            while ((line = lineReader.readLine()) != null) {
                gzContents.append(line + "\n"); //읽은 라인

                idx++; // 읽은 라인 횟수 세기

                // 읽을 라인수까지 읽으면 더 이상 파일 내용을 읽지 않음
                if (readCnt == idx) {
                    break;
                }
            }

            // 읽은 전체 내용을 String 타입으로 변경하기
            result = gzContents.toString();

        } catch (IOException e) {
            throw new RuntimeException("Gzip 파일 읽기 실패했습니다.", e);

        }

        gzis.close();
        gzis = null;

        fis.close();
        fis = null;

        streamReader.close();
        streamReader = null;

        log.info(this.getClass().getName() + ".readLocalGzFile End!");

        return result;
    }

    @Override
    public void upload10Line(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".upload10Line Start!");

        // 하둡 분산 파일 시스템 객체
        FileSystem hdfs = null;

        // 하둡 파일에 저장할 내용
        String uploadContents = CmmUtil.nvl(pDTO.getFileContents());
        log.info("uploadContents : " + uploadContents);

        // CentOS에 설치된 하둡 분산 파일 시스템 연결 및 설정하기
        hdfs = FileSystem.get(this.getHadoopConfiguration());

        log.info("HDFS Home Dir : " + hdfs.getHomeDirectory());
        log.info("HDFS Working Dir : " + hdfs.getWorkingDirectory());

        // 하둡에 파일을 업로드할 폴더
        // 예 : /01/02
        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath());

        // 하둡에 업로드할 파일명
        // 예 : access_log.gz
        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        // 하둡에 폴더 생성하기
        // hadoop fs -mkdir -p /01/02
        hdfs.mkdirs(new Path(hadoopUploadFilePath));

        // 하둡분산파일시스템에 저장될 파일경로 및 폴더명
        // 예 : hadoop fs -put access_log.gz /01/02/access_log.gz
        String hadoopFile = CmmUtil.nvl(pDTO.getHadoopUploadPath())
                + "/" + CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        // 하둡분산파일시스템에 저장가능한 객체로 변환
        Path path = new Path(hadoopFile);

        // 기존 하둡에 존재하는지 로그 찍어보기
        log.info("HDFS Exists : " + hdfs.exists(path));

        if (hdfs.exists(path)) { // 하둡분산파일시스템에 파일 존재하면...

            // 기존 업로드되어 있는 파일 삭제하기
            // hadoop fs -rm -r /01/02/access_log.gz
            hdfs.delete(path, true);

        }

        // 하둡분산파일시스템에 생성된 파일에 내용 작성하기
        // FSDataOutputStream 객체는 Buffer를 상속하여 구현함
        FSDataOutputStream outputStream = hdfs.create(path);
        outputStream.writeUTF(uploadContents);
        outputStream.close();

        log.info(this.getClass().getName() + ".upload10Line End!");

    }

    @Override
    public String readHadoopFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".upload10Line Start!");

        // 하둡 분산 파일 시스템 객체
        FileSystem hdfs = null;

        hdfs = FileSystem.get(this.getHadoopConfiguration());

        // 하둡분산파일시스템에 저장될 파일경로 및 폴더명
        // 예 : hadoop fs -put access_log.gz /01/02/access_log.gz
        String hadoopFile = CmmUtil.nvl(pDTO.getHadoopUploadPath())
                + "/" + CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        // 하둡분산파일시스템에 저장가능한 객체로 변환
        Path path = new Path(hadoopFile);

        // 기존 하둡에 존재하는지 로그 찍어보기
        log.info("HDFS Exists : " + hdfs.exists(path));

        String readLog = "";

        if (hdfs.exists(path)) { // 하둡분산파일시스템에 파일 존재하면...

            // 하둡분산파일시스템의 파일 읽기
            // FSDataOutputStream 객체는 ByteBufferReadable를 상속하여 구현함
            FSDataInputStream inputStream = hdfs.open(path);
            readLog = inputStream.readUTF();
            inputStream.close();

        }

        log.info(this.getClass().getName() + ".upload10Line End!");

        return readLog;
    }
}
