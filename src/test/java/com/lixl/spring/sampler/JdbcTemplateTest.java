package com.lixl.spring.sampler;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:application_core.xml")
public class JdbcTemplateTest {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Test
	public void doTest() {
		Integer sal = jdbcTemplate.queryForInt("SELECT SAL FROM EMP WHERE EMPNO=7369");
		System.out.println(sal);
		try {
			jdbcTemplate.execute("UPDATE EMP SET SAL=1600 WHERE EMPNO=7369");
			jdbcTemplate.execute("commit");
		} catch (DataAccessException e) {
			e.printStackTrace();
			jdbcTemplate.execute("rollback");
		}
		sal = jdbcTemplate.queryForInt("SELECT SAL FROM EMP WHERE EMPNO=7369");
		System.out.println(sal);
	}
	
	private void execute() {
		jdbcTemplate.execute(new ConnectionCallback<Object>() {

			@Override
			public Object doInConnection(Connection con) throws SQLException, DataAccessException {
				con.setAutoCommit(false);
				
				con.commit();
				
				return null;
			}
			
		});
	}
	

}
