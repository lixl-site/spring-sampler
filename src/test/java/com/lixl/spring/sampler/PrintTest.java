package com.lixl.spring.sampler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class PrintTest {
	private static String[] fileds = { "USERID", "RESTID", "RATING", "RATING_ENV", "RATING_FLAVOR", "RATING_SERVICE",
			"TIMESTAMP", "RATINGS_COMMENT" };
	
	public static void main(String[] args) {
		System.out.println(String.join(",:", fileds));
	}

}
