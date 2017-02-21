package org.pcchen.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * File: PageViews.java 
 * Author:pcchen
 * Email：cpc_geek@163.com
 * Date: 2017年2月21日下午4:10:27
 * Description: 
 * 
 */

public class PageViews implements Writable{
	private String session;
	private String remote_addr;
	private String timeStr;
	private String request;
	private Integer step;
	private String stayLong;
	private String referal;
	private String userAgent;
	private String bytes_send;
	private String status;
	
	public void set(String session, String remote_addr, String timeStr,
			String request, Integer step, String stayLong, String referal,
			String userAgent, String bytes_send, String status) {
		this.session = session;
		this.remote_addr = remote_addr;
		this.timeStr = timeStr;
		this.request = request;
		this.step = step;
		this.stayLong = stayLong;
		this.referal = referal;
		this.userAgent = userAgent;
		this.bytes_send = bytes_send;
		this.status = status;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getTimeStr() {
		return timeStr;
	}

	public void setTimeStr(String timeStr) {
		this.timeStr = timeStr;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public Integer getStep() {
		return step;
	}

	public void setStep(Integer step) {
		this.step = step;
	}

	public String getStayLong() {
		return stayLong;
	}

	public void setStayLong(String stayLong) {
		this.stayLong = stayLong;
	}

	public String getReferal() {
		return referal;
	}

	public void setReferal(String referal) {
		this.referal = referal;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public String getBytes_send() {
		return bytes_send;
	}

	public void setBytes_send(String bytes_send) {
		this.bytes_send = bytes_send;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\001").append(this.getSession());
		sb.append("\001").append(this.getRemote_addr());
		sb.append("\001").append(this.getTimeStr());
		sb.append("\001").append(this.getRequest());
		sb.append("\001").append(this.getStep());
		sb.append("\001").append(this.getStayLong());
		sb.append("\001").append(this.getReferal());
		sb.append("\001").append(this.getUserAgent());
		sb.append("\001").append(this.getBytes_send());
		sb.append("\001").append(this.getStatus());
		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(session);
		out.writeUTF(remote_addr);
		out.writeUTF(timeStr);
		out.writeUTF(request);
		out.writeInt(step);
		out.writeUTF(stayLong);
		out.writeUTF(referal);
		out.writeUTF(userAgent);
		out.writeUTF(bytes_send);
		out.writeUTF(status);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.session = in.readUTF();
		this.remote_addr = in.readUTF();
		this.timeStr = in.readUTF();
		this.request = in.readUTF();
		this.step = in.readInt();
		this.stayLong = in.readUTF();
		this.referal = in.readUTF();
		this.userAgent = in.readUTF();
		this.bytes_send = in.readUTF();
		this.status = in.readUTF();
	}
	
}


