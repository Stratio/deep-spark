package com.stratio.deep.entity;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;

/**
 * Created by luca on 26/01/14.
 */
@DeepEntity
public class OutputPageEntity implements IDeepType {

    /**
     * 
     */
    private static final long serialVersionUID = 8558815316194850744L;

    @DeepField(fieldName = "key", isPartOfPartitionKey = true)
    private String id;

    @DeepField(fieldName = "domain_name")
    private String domainName;

    @DeepField
    private String url;

    @DeepField
    private String charset;

    @DeepField
    private String content;

    @DeepField(fieldName = "download_time", validationClass = LongType.class)
    private Long downloadTime;

    @DeepField(fieldName = "first_download_time", validationClass = LongType.class)
    private Long firstDownloadTime;

    @DeepField(fieldName = "response_code", validationClass = Int32Type.class)
    private Integer responseCode;

    @DeepField(fieldName = "response_time", validationClass = LongType.class)
    private Long responseTime;

    public OutputPageEntity() {

    }

    public OutputPageEntity(PageEntity pe) {
	this.id = pe.getId();
	this.charset = pe.getCharset();
	this.content = pe.getContent();
	this.downloadTime = pe.getDownloadTime();
	this.firstDownloadTime = pe.getFirstDownloadTime();
	this.url = pe.getUrl();
	this.domainName = pe.getDomainName();
	this.responseCode = pe.getResponseCode();
	this.responseTime = pe.getResponseTime();
    }

    public String getCharset() {
	return charset;
    }

    public String getContent() {
	return content;
    }

    public String getDomainName() {
	return domainName;
    }

    public Long getDownloadTime() {
	return downloadTime;
    }

    public Long getFirstDownloadTime() {
	return firstDownloadTime;
    }

    public String getId() {
	return id;
    }

    public Integer getResponseCode() {
	return responseCode;
    }

    public Long getResponseTime() {
	return responseTime;
    }

    public String getUrl() {
	return url;
    }

    public void setCharset(String charset) {
	this.charset = charset;
    }

    public void setContent(String content) {
	this.content = content;
    }

    public void setDomainName(String domainName) {
	this.domainName = domainName;
    }

    public void setDownloadTime(Long downloadTime) {
	this.downloadTime = downloadTime;
    }

    public void setFirstDownloadTime(Long firstDownloadTime) {
	this.firstDownloadTime = firstDownloadTime;
    }

    public void setId(String id) {
	this.id = id;
    }

    public void setResponseCode(Integer responseCode) {
	this.responseCode = responseCode;
    }

    public void setResponseTime(Long responseTime) {
	this.responseTime = responseTime;
    }

    public void setUrl(String url) {
	this.url = url;
    }
}
