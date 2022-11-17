package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IGzFileService {

    String readLocalGzFile(HadoopDTO pDTO) throws Exception;

    void upload10Line(HadoopDTO pDTO) throws Exception;

    String readHadoopFile(HadoopDTO pDTO) throws Exception;

}
