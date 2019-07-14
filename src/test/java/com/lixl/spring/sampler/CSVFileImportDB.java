package com.lixl.spring.sampler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.util.IOUtils;

/**
 * https://github.com/SophonPlus/ChineseNlpCorpus/blob/master/datasets/yf_dianping/intro.ipynb
 *
 * @author : P2M.wuzh
 * @date : 2019-05-31 14:33
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:application_core.xml")
public class CSVFileImportDB {

    final static Logger logger = LoggerFactory.getLogger(CSVFileImportDB.class);
    /**
     * 文件第一列会被忽略
     *
     * @author wuzh
     * @date 2019-06-04 18:02
     */
    private String fileName = "ratings.csv";
    private String filePath = "G:\\dataset";
    /***
     * 批量提交，每次提交数量
     * @author wuzh
     * @date 2019-06-04 18:00
     */
    private int j = 1000;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    public void test() {
        upload(fileName, filePath);
    }

    /**
     * 准备参数
     *
     * @param params
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author wuzh
     * @date 2019-06-04 18:03
     */
    public Map<String, Object> packagingParameters(String[] params) {
        Map<String, Object> map = new HashMap<>();
        if(params!=null) {
        	for(int i=0;i<params.length;i++) {
        		String value = params[i];
        		if(value==null || value.isEmpty()) {
        			value = "0";
        		}
        		switch (i) {
				case 0:
					 map.put("USERID", value);
					break;
				case 1:
					 map.put("RESTID", value);
					break;
				case 2:
					 map.put("RATING", value);
					break;
				case 3:
					 map.put("RATING_ENV", value);
					break;
				case 4:
					 map.put("RATING_FLAVOR", value);
					break;
				case 5:
					 map.put("RATING_SERVICE", value);
					break;
				case 6:
					 map.put("TIMESTAMP", value);
					break;
				case 7:
					 map.put("RATINGS_COMMENT", value);
					break;
				default:
					break;
				}
        	}
        }
       
        return map;
    }

    /**
     * 批量插入
     *
     * @param params 插入数据
     * @return void
     * @author wuzh
     * @date 2019-06-04 18:04
     */
    public void batchInsert(String sql, Map<String, Object>[] params) {
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        namedParameterJdbcTemplate.batchUpdate(sql, params);
    }

    /**
     * 上传主体
     *
     * @param fileName
     * @param filePath
     * @return void
     * @author wuzh
     * @date 2019-05-31 15:14
     */
    public void upload(String fileName, String filePath) {
        File file = new File(filePath + File.separator + fileName);
        BufferedReader reader = null;
        int i = -1;
        Long pre = new Date().getTime();
        long lineCount = 0;
        long rowCount = 0;
        Map<String, Object>[] params = new HashMap[j];
        //USERID, RESTID, RATING, RATING_ENV, RATING_FLAVOR, RATING_SERVICE, TIMESTAMP, RATINGS_COMMENT"
        String sql = "INSERT INTO RATINGS(USERID, RESTID, RATING, RATING_ENV, RATING_FLAVOR, RATING_SERVICE, TIMESTAMP, RATINGS_COMMENT) values(?,?,?,?,?,?,?,?) ";
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = "";
            String line = "";
            while ((line = reader.readLine()) != null) {
            	lineCount ++;
                if (line.isEmpty() || lineCount==1) {
                    continue;
                }
                tempString += line.trim();
                if(tempString.endsWith("\"")) {
                	 String[] fileds = tempString.split(",");
                	 tempString = "";
                	 i++;
                	 rowCount ++;
                	 params[i] = packagingParameters(fileds);
                     if(i==j-1) {
                    	 batchInsert(sql, params);
                    	 i = -1;
                    	 params = new HashMap[j];
                     }
                }
            }
        } catch (Exception e) {
            logger.error("发生异常：", e);
        } finally {
        	System.out.println("lineCount="+lineCount+" \trowCount="+rowCount);
            IOUtils.close(reader);
            Long after = new Date().getTime();
            logger.info("执行时间：" + (after - pre) + "毫秒");
        }

    }

    /***
     * 格式化comm
     * 将单引号转为两个单引号，去掉开头结尾的"
     * @author wuzh
     * @date 2019-06-04 18:21
     * @param comm
     * @return java.lang.String
     */
    private String formatComm(String comm) {
        if (null != comm && (!"".equalsIgnoreCase(comm))) {
            comm = comm.substring(1, comm.length() - 1)
                    .replaceAll("\'", "\'\'");
        }

        return comm;
    }

}
