package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface ILocalGzFileReadService {

    /**
     * Gz파일의 최초 기준 라인수만큼 읽기
     * 예 : zcat access_log.gz | head -n 10
     */
    String readLocalGzFileCnt(HadoopDTO pDTO) throws Exception;

    /**
     * Gz파일의 IP 대역읽기
     * 예 : zcat access_log.gz | grep -E '10\.56\.[0-9]{1,3}\.[0-9]{1,3}'
     */
    String readLocalGzFileIP(HadoopDTO pDTO) throws Exception;


}
