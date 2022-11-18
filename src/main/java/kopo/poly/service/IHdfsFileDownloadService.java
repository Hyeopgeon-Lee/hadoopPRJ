package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IHdfsFileDownloadService {

    /**
     * 하둡 분산 파일 시스템에 저장된 파일 다운로드
     *
     *  예 : hadoop fs -get /access_log.gz c:/hadoop_data/hadoop_access_log.gz
     */
    void downloadHdfsFile(HadoopDTO pDTO) throws Exception;


}
