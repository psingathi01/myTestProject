package com.myspark.java;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RunTask {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ExecutorService executor = Executors.newFixedThreadPool(10);
		
		for(int i=0;i<=100;i++){
			executor.execute(new Task());
		}
		
		executor.shutdown();

	}

}
