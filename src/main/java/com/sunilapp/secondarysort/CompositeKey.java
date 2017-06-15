package com.sunilapp.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This key is a composite key. The "actual" key is the UID. The secondary sort
 * will be performed against the datetime.
 */
public class CompositeKey implements WritableComparable {

	private String uid;
	private String datetime;

	public CompositeKey() {
	}

	public CompositeKey(String udid, String datetime) {

		this.uid = udid;
		this.datetime = datetime;
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append(uid).append(',').append(datetime).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		uid = WritableUtils.readString(in);
		datetime = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, uid);
		WritableUtils.writeString(out, datetime);
	}

	public int compareTo(CompositeKey o) {
		int result = uid.compareTo(o.uid);
		if (0 == result) {
			result = datetime.compareTo(o.datetime);
		}
		return result;
	}

	/**
	 * Gets the udid.
	 *
	 * @return UDID.
	 */
	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	/**
	 * Gets the datetime.
	 *
	 * @return Datetime
	 */
	public String getDatetime() {
		return datetime;
	}	

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}	

}
