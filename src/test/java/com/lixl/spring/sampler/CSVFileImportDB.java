package com.lixl.spring.sampler;

import com.alibaba.fastjson.util.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
    private String tableName = "ratings";
    private String[] fileds = {"USERID", "RESTID", "RATING", "RATING_ENV", "RATING_FLAVOR", "RATING_SERVICE", "TIMESTAMP", "RATINGS_COMMENT"};
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
        map.put("USERID", params[0]);
        map.put("RESTID", params[1]);
        map.put("RATING", params[2]);
        map.put("RATING_ENV", params[3]);
        map.put("RATING_FLAVOR", params[4]);
        map.put("RATING_SERVICE", params[5]);
        map.put("TIMESTAMP", params[6]);
        map.put("RATINGS_COMMENT", params[7]);
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
    public void batchInset(String sql, Map<String, ?>[] params) {
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        namedParameterJdbcTemplate.batchUpdate(sql, params);
    }

    /**
     * sql语句
     *
     * @param
     * @return java.lang.String
     * @author wuzh
     * @date 2019-06-04 18:04
     */
    private String insertNamedParameter(String tableName, String[] fileds) {
        StringBuilder sb = new StringBuilder();
        sb.append(" insert into \n");
        sb.append(tableName + " \n");
        sb.append(" ( \n");
        for (String filed : fileds) {
            sb.append(filed + ",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" ) \n");
        sb.append(" VALUES\n");
        sb.append(" (");
        for (String filed : fileds) {
            sb.append(":" + filed + ",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")\n");
        return sb.toString();
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
        int line = 0;
        int i = -1;
        Long pre = new Date().getTime();

        Map[] params = new HashMap[j];
        String sql = insertNamedParameter(tableName, this.fileds);
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = "";
            while ((tempString = reader.readLine()) != null) {
                line++;
                if (line == 1) {
                    continue;
                }
                String[] fileds = tempString.split(",", this.fileds.length);
                while (!lineEnd(fileds[7])) {
                    fileds[7] += "\n" + reader.readLine();
                }
                fileds[7] = formatComm(fileds[7]);
                logger.info(Arrays.toString(fileds));
                i++;
                if (i < j - 1) {
                    Map<String, Object> param = packagingParameters(fileds);
                    params[i] = param;
                } else {
                    Map<String, Object> param = packagingParameters(fileds);
                    params[i] = param;
                    batchInset(sql, params);
                    //初始化i,params
                    i = -1;
                    params = new HashMap[j];
                }
            }

        } catch (Exception e) {
            logger.error("发生异常：", e);
        } finally {
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

    /***
     * 判断数据是否读取完，根据comm字段
     * 判断条件：以"结尾并以"开始，或者为空
     * @author wuzh
     * @date 2019-06-04 18:19
     * @param line
     * @return boolean
     */
    public boolean lineEnd(String line) {
        return (line.endsWith("\"") && (!line.endsWith("\"\"")) && line.startsWith("\"")) || (line.isEmpty());
    }

}
