package com.lixl.spring.sampler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
	private String[] fileds = { "USERID", "RESTID", "RATING", "RATING_ENV", "RATING_FLAVOR", "RATING_SERVICE",
			"TIMESTAMP", "RATINGS_COMMENT" };
	/***
	 * 批量提交，每次提交数量
	 * 
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
	 * 批量插入
	 *
	 * @param params 插入数据
	 * @return void
	 * @author wuzh
	 * @date 2019-06-04 18:04
	 */
	public void batchInsert(String sql, List<Map<String, Object>> params) {
		if(params==null || params.isEmpty()) return;
		Map<String, Object>[] batchValues = params.toArray(new HashMap[params.size()]);
		NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
		namedParameterJdbcTemplate.batchUpdate(sql, batchValues);
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
		List<Map<String, Object>> params = new ArrayList<>(j);

		String sql = "INSERT INTO RATINGS(" + String.join(",", fileds) + ") " + "values(:" + String.join(",:", fileds)
				+ ") ";
		try {
			FileReader fileReader = new FileReader(file);
			reader = new BufferedReader(fileReader);
			String tempString = "";
			String line = "";

			while ((line = reader.readLine()) != null) {
				lineCount++;
				if (lineCount == 1) {
					continue;
				}
				tempString += line;
				if(line.endsWith("\"\"")) {
					continue;
				}
				if (!line.endsWith("\"")) {
					continue;
				}
				
				i++;
				rowCount++;
				//System.err.println(lineCount);
				//System.out.println(tempString);
				Map<String, Object> map = new HashMap<>();
				for (int j = 0; j < 8; j++) {
					int l = tempString.length();
					int k = tempString.indexOf(",");
					String value = "";
					if (j < 7) {
						value = tempString.substring(0, k);
						tempString = tempString.substring(k + 1, l);
						/*
						 * if(value!=null && value.length()>3 && j<6) { try { Integer.parseInt(value); }
						 * catch (Exception e) { e.printStackTrace(); System.out.println(value); } }
						 */
					} else {
						value = tempString;
					}
					map.put(fileds[j], value);
				}
				tempString = "";
				params.add(map);

				if (i == j - 1) {
					batchInsert(sql, params);
					i = -1;
					params.clear();
				}
			}
		} catch (Exception e) {
			logger.error("发生异常：", e);
		} finally {
			System.out.println("lineCount=" + lineCount + " \trowCount=" + rowCount);
			IOUtils.close(reader);
			Long after = new Date().getTime();
			logger.info("执行时间：" + (after - pre) + "毫秒");
		}

	}

}
