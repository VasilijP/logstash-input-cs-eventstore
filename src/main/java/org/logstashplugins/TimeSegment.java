package org.logstashplugins;

import java.time.Instant;

public final class TimeSegment
{	
	private long from;
	
	private long to;
	
	private TimeSegmentStatus status;
	public TimeSegmentStatus getStatus() { return status; }

	private int elCount;
	public int getElCount() { return elCount; }

	private int csCount;
	public int getCsCount() { return csCount; }
	
	private TimeSegment next = this;
	public TimeSegment getNext() { return next;	}

	private TimeSegment previous = this;
	public TimeSegment getPrevious() { return previous; }
	public void setPrevious(TimeSegment previous)
	{ 
		this.previous = previous;
		previous.next = this;
	}

	public TimeSegment(long aFrom, long aTo)	
	{
		from = aFrom;
		to = aTo;
		status = TimeSegmentStatus.Dirty;
		elCount = 0;
		csCount = 0;
	}
	
	public void setCheckResult(int aElCount, int aCsCount)
	{
		this.csCount = aCsCount;
		this.elCount = aElCount;
		this.status = (aCsCount <= aElCount)?TimeSegmentStatus.Ok:TimeSegmentStatus.Invalid;
	}
	
	public void resetStatus()
	{
		status = TimeSegmentStatus.Dirty;
		elCount = 0;
		csCount = 0;
	}
	
	public void Merge(TimeSegment other) throws Exception
	{
		if (this.status == TimeSegmentStatus.Ok && other.status == TimeSegmentStatus.Ok)
		{
			if (this.to == other.from) // this extends into future
			{
				this.to = other.to;				
			}
			else if (this.from == other.to) // this extends into past
			{
				this.from = other.from;				
			}
			else
			{
				throw new Exception("Invalid Merge, cannot merge non-adjacent segments.");
			}	
			
			this.csCount += other.csCount;
			this.elCount += other.elCount;
		}
		else
		{
			throw new Exception("Invalid Merge, cannot merge non-Ok statuses.");
		}
	}
		
	/**
	 * Split segment in place (in the middle) and return latter part.
	 * 
	 * @return
	 * @throws Exception
	 */
	public TimeSegment Split() throws Exception
	{
		long splitPoint = (from+to)/2L;
		TimeSegment latterPart = new TimeSegment(splitPoint, this.to);
		this.to = splitPoint;
		this.status = TimeSegmentStatus.Dirty; // invalid splits to -> Dirty as well
		this.csCount = 0;
		this.elCount = 0;
		
		return latterPart;
	}
	
	/**
	 * Clip oldest part of interval by supplied time point, works only if not whole TimeSegment would be clipped (it has to be removed completely in that case).
	 * @param aNewFrom
	 */
	public void ClipBy(long aNewFrom)	
	{
		if (this.to >= aNewFrom && this.from < aNewFrom)
		{
			this.from = aNewFrom;
			this.csCount = 0;
			this.elCount = 0;
			this.status = TimeSegmentStatus.Dirty;
		}				
	}
	
	public Instant getFromTsInstant()
	{
		return Instant.ofEpochMilli(from);
	}
	
	public Instant getToTsInstant()
	{
		return Instant.ofEpochMilli(to);
	}
	
	public String getFromTs()
	{
		return getFromTsInstant().toString();
	}
	
	public String getToTs()
	{
		return getToTsInstant().toString();
	}
	
	public boolean couldMerge(TimeSegment timeSegment, int maximumSegmentRecords)
	{
		return ((this.to == timeSegment.from) &&
				(this.status == TimeSegmentStatus.Ok) &&
				(timeSegment.status == TimeSegmentStatus.Ok)  &&
				(this.elCount + timeSegment.elCount < maximumSegmentRecords));
	}
	
	@Override
	public String toString()
	{
		return "[status: "+status+", "+getFromTsInstant()+" -to- "+getToTsInstant()+", count Elastic: " + getElCount() + ", count Cassandra: " + getCsCount()+"]";
	}

}
