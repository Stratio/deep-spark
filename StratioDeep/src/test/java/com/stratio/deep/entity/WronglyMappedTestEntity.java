package com.stratio.deep.entity;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;

@DeepEntity
public class WronglyMappedTestEntity implements  IDeepType{

    @DeepField(isPartOfPartitionKey = true)
    private String id;

    @DeepField(fieldName = "domain_name")
    private String domain;

    @DeepField
    private String url;

    @DeepField(fieldName = "not_existent_field")
    private String wronglyMappedField;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getWronglyMappedField() {
        return wronglyMappedField;
    }

    public void setWronglyMappedField(String wronglyMappedField) {
        this.wronglyMappedField = wronglyMappedField;
    }

    @Override
    public String toString() {
        return "WronglyMappedTestEntity{" +
            "id='" + id + '\'' +
            ", domain='" + domain + '\'' +
            ", url='" + url + '\'' +
            ", wronglyMappedField='" + wronglyMappedField + '\'' +
            '}';
    }
}
