package com.sunilapp.customwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class CustWritableComparable implements WritableComparable<CustWritableComparable> {


	private IntWritable empid ;
	private Text empname ;
	private Text date ;
	private Text city;
	
	
		
	public CustWritableComparable(){
		
		this.empid = new IntWritable();
		this.empname = new Text();
		this.date = new Text();
		this.city = new Text();
		
	}
	
	public CustWritableComparable(IntWritable empid, Text empname, Text date, Text city) {
		this.empid = empid;
		this.empname = empname;
		this.date = date;
		this.city = city;
	}
	
	public CustWritableComparable(int empid, String empname, String date, String city) {
		set(new IntWritable(empid) , new Text(empname) ,new Text(date),new Text(city) );
		
	}
	
	public void set (IntWritable empid, Text empname, Text date, Text city) {
		this.empid = empid;
		this.empname = empname;
		this.date = date;
		this.city = city;
		
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		empid.write(out);
		empname.write(out);
		date.write(out);
		city.write(out);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		empid.readFields(in);
		empname.readFields(in);
		date.readFields(in);
		city.readFields(in);
			  
		
		
	}
	
	
	
	@Override

	public int compareTo(CustWritableComparable o) {
		System.out.println("compare method");
		System.out.println(empid);
		System.out.println(o.empid);
		if (empid.compareTo(o.empid) == 0) {
			System.out.println(date);
			System.out.println(o.date);
			System.out.println("-------------------------");
			return (date.compareTo(o.date));
		} else
			System.out.println("****************************");
			return (empid.compareTo(o.empid));		
		
	}

	@Override
	public boolean equals(Object o) {
		System.out.println("equals method");
		if (o instanceof CustWritableComparable) {
			CustWritableComparable other = (CustWritableComparable) o;
			return date.equals(other.date) && city.equals(other.city);
		}
		return false;
	}
		 
	@Override
	 public String toString() {
          String e =  empid.toString();
          String en = empname.toString();
          String dt = date.toString();
          String ct = city.toString();
		return  e +","+ en +","+ dt +","+ ct;

		  }
	@Override
	public int hashCode() {
		return empid.hashCode();
	}

}
