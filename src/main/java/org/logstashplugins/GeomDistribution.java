package org.logstashplugins;

import java.util.Random;

// Based on x^-1/exponential/geometric distribution of probability - that given i-index will be chosen.
public class GeomDistribution
{
	
	private Random rand = new Random();
	private boolean all = false;
	private double cumulative;
	private int targetCount;
	private int totalCount;	

	public GeomDistribution(int aTotalCount, long aTargetCount)
	{
		targetCount = (int)Math.max(1L, aTargetCount);
		totalCount = (int)Math.max(1, aTotalCount);
		cumulative = 1.01; // first will get picked always
		all = targetCount >= totalCount;
	}

	public boolean pickNext()
	{	
		if (all) 
		{ 
			return true;
		}
		else if (targetCount <= 0)
		{
			return false;
		}
		
		cumulative += targetCount*1.0/totalCount;				

		if (rand.nextDouble() < cumulative)
		{
			cumulative = Math.max(0, cumulative - 1.0);
			--targetCount;
			return true;
		}
		
		return false;
	}
	
	/*
	public static void main(String[] args)
	{
		int count = 10;
		for (int run = 1; run  <= 15; ++run)
		{
			System.out.print("RUN #"+run+" : ");
			count += 0;
			int total = 200;
			int picked = 0;
			GeomDistribution d = new GeomDistribution(total, count);
			for (int i = 0; i < total; ++i)
			{
				if (d.pickNext())
				{
					System.out.print("X");
					++picked;
				}
				else
				{
					System.out.print("_");
				}
			}			
			System.out.println(" "+picked+"/"+count);
		}
	}
	*/
}
