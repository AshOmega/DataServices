package com.datautils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceUtil {
	private static final Logger log = LoggerFactory.getLogger(PerformanceUtil.class);
	private long startTime = 0;
	private long endTime = 0;
	
	public void resetClock()
	{
		startTime = 0;
		endTime = 0;
	}
	
	public void startClock()
	{
		resetClock();
		startTime = System.currentTimeMillis();
	}
	
	public void endClock(String feature)
	{
		endTime = System.currentTimeMillis();
		long timeDifference = endTime - startTime;
		if(timeDifference > 1000)
			log.info("ProcessingTime : " + feature + " -> " +  timeDifference/1000 + " seconds\n");
		else
			log.info("ProcessingTime : " +  feature + " -> " +  timeDifference + " milliseconds\n");
		resetClock();
	}
	
}
