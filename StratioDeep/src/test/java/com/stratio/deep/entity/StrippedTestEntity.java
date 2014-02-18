package com.stratio.deep.entity;

import org.apache.cassandra.db.marshal.Int32Type;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;

@DeepEntity
public class StrippedTestEntity implements IDeepType {

    private static final long serialVersionUID = -7394476231513436262L;

    @DeepField
    private String id;

    @DeepField
    private String url;

    @DeepField(validationClass = Int32Type.class, fieldName = "response_time")
    private Integer responseTime;

    public StrippedTestEntity(TestEntity te) {
	this.id = te.getId();
	this.url = te.getUrl();
	this.responseTime = te.getResponseTime();
    }

    String getId() {
	return id;
    }

    Integer getResponseTime() {
	return responseTime;
    }

    String getUrl() {
	return url;
    }

    void setId(String id) {
	this.id = id;
    }

    void setResponseTime(Integer responseTime) {
	this.responseTime = responseTime;
    }

    void setUrl(String url) {
	this.url = url;
    }

}