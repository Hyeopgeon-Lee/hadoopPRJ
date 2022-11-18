package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IHdfsFileReadService {

    /**
     * 하둡 분산 파일 시스템에 저장된 파일 내용 읽기
     * <p>
     * 예 : hadoop fs -cat /access_log
     */
    String readHdfsFile(HadoopDTO pDTO) throws Exception;
}
