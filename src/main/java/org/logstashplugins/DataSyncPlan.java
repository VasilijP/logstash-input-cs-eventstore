package org.logstashplugins;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class DataSyncPlan implements Iterable<TimeSegment> {
	
	private List<TimeSegment> segments;
	private int maximumSegmentRecords;
	private int historyLengthDays;
	private int invalidCount;
	private int dirtyCount;
	private int okCount;
	
	public DataSyncPlan(int aHistoryLengthDays, int aMaximumSegmentSize)
	{
		this.segments = new ArrayList<TimeSegment>();
		this.maximumSegmentRecords = aMaximumSegmentSize;
		this.historyLengthDays = aHistoryLengthDays;
		
		Instant aTo = Instant.now();
		Instant aFrom = aTo.minus(aHistoryLengthDays, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS);
		
		segments.add(new TimeSegment(aFrom.toEpochMilli(), aTo.toEpochMilli()));
	}
	
	/**
	 * Repartitions the plan. Ok segments are merged if still under limit, all segments are subdivided if exceeding the limit.
	 * @throws Exception 
	 */
	public void Repartition() throws Exception
	{
			List<TimeSegment> newSegments = new ArrayList<TimeSegment>();			
			
			while (segments.size() > 0)
			{				
				TimeSegment lastSegment = segments.remove(0); 
				newSegments.add(lastSegment);
				
				while (segments.size() > 0 && lastSegment.couldMerge(segments.get(0), maximumSegmentRecords))
				{
					lastSegment.Merge(segments.remove(0));
				}
							
				if (lastSegment.getElCount() >= maximumSegmentRecords) // it is important to split for equal amount - this means the segment has synclimit or more records (is hitting the upper query limit)
				{
					lastSegment = lastSegment.Split();
					newSegments.add(lastSegment);
				}
			}
			
			segments = newSegments;
			for (int i = segments.size()-1; i > 0; --i) // update next and previous pointers in segments in order to allow spreading of dirty state
			{
				segments.get(i).setPrevious(segments.get(i-1));
			}
			
			dirtyCount = 0;
			invalidCount = 0;
			okCount = 0;
			for (TimeSegment ts : segments)
			{
				TimeSegmentStatus tss = ts.getStatus();
				if (tss == TimeSegmentStatus.Dirty)
				{
					++dirtyCount;
				} 
				else if (tss == TimeSegmentStatus.Invalid)
				{
					++invalidCount;
				}
				else if (tss == TimeSegmentStatus.Ok)
				{
					++okCount;
				}
			}
	}
	
	/**
	 * Clips the old parts of the plan and adds most recent segment as necessary.
	 * Returns segment which encapsulates whole plan.
	 * @return 
	 */
	public TimeSegment ShiftToNow()
	{
		Instant nowInstant = Instant.now();
		long now = nowInstant.toEpochMilli();
		long from = nowInstant.minus(historyLengthDays, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
		
		TimeSegment returnValue = new TimeSegment(from, now);
	
		// clip plan to latest period
		while (segments.size() > 0 && segments.get(0).getToTsInstant().toEpochMilli() < from)
		{
			segments.remove(0);
		}
		
		if (segments.size() > 0)
		{
			segments.get(0).ClipBy(from);
		}
		
		// extend plan 'up to' now		
		from = segments.get(segments.size()-1).getToTsInstant().toEpochMilli();
		if (now > from)
		{
			segments.add(new TimeSegment(from,  now));
		}
		
		return returnValue;
	}

	/**
	 * Iterates over plan segments, allows caller to act upon each segment one by one.
	 */
	@Override
	public Iterator<TimeSegment> iterator() {
		return new Iterator<TimeSegment>() {

			private int index = 0;
			
			@Override
			public TimeSegment next() {
				return segments.get(index++);
			}
			
			@Override
			public boolean hasNext() {
				return index < segments.size();
			}
		};
	}

	// Mark additional segments as dirty with higher priority towards latest period.
	public void markDirty(long segmentsToMarkDirty)
	{
		GeomDistribution distribution = new GeomDistribution(this.okCount, segmentsToMarkDirty);
		
		for (int i = segments.size()-1; i >= 0; --i)
		{
			if (segmentsToMarkDirty <= 0)
			{
				break;
			}
			
			if (segments.get(i).getStatus() == TimeSegmentStatus.Ok &&
				distribution.pickNext())
			{
				segments.get(i).resetStatus();
				--segmentsToMarkDirty;
			}
		}		
	}

	// Total is always accurate, other counts are only recalculated at plan Repartition.
	@Override
	public String toString() 
	{
		return "[total: "+segments.size()+", Dirty: "+dirtyCount+", Invalid: "+invalidCount+", Ok: "+okCount+"]";
	}

}
