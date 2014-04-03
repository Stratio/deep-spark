/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.testentity;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.IDeepType;
import org.apache.cassandra.db.marshal.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@DeepEntity
public class TestEntity implements IDeepType {
    private static final long serialVersionUID = -6242942929275890323L;

    @DeepField(isPartOfPartitionKey = true)
    private String id;

    @DeepField(fieldName = "domain_name")
    private String domain;

    @DeepField
    private String url;

    @DeepField(validationClass = Int32Type.class, fieldName = "response_time")
    private Integer responseTime;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Set<String> getEmails() {
        return emails;
    }

    public void setEmails(Set<String> emails) {
        this.emails = emails;
    }

    public List<String> getPhones() {
        return phones;
    }

    public void setPhones(List<String> phones) {
        this.phones = phones;
    }

    public Map<UUID, Integer> getUuid2id() {
        return uuid2id;
    }

    public void setUuid2id(Map<UUID, Integer> uuid2id) {
        this.uuid2id = uuid2id;
    }

    @DeepField(fieldName = "response_code", validationClass = Int32Type.class)
    private Integer responseCode;

    @DeepField(validationClass = LongType.class, fieldName = "download_time")
    private Long downloadTime;

    private String notMappedField;

    @DeepField(validationClass = SetType.class)
    private Set<String> emails;

    @DeepField(validationClass = ListType.class)
    private List<String> phones;

    @DeepField(validationClass = MapType.class)
    private Map<UUID, Integer> uuid2id;

    public TestEntity() {
        super();
    }

    public TestEntity(String id, String domain, String url, Integer responseTime, Integer responseCode,
        String notMappedField) {
        this.id = id;
        this.domain = domain;
        this.url = url;
        this.responseTime = responseTime;
        this.responseCode = responseCode;
        this.downloadTime = null;
        this.notMappedField = notMappedField;
    }

    public String getDomain() {
        return domain;
    }

    public Long getDownloadTime() {
        return downloadTime;
    }

    public String getId() {
        return id;
    }

    public String getNotMappedField() {
        return notMappedField;
    }

    public Integer getResponseCode() {
        return responseCode;
    }

    public Integer getResponseTime() {
        return responseTime;
    }

    public String getUrl() {
        return url;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setDownloadTime(Long downloadTime) {
        this.downloadTime = downloadTime;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setNotMappedField(String notMappedField) {
        this.notMappedField = notMappedField;
    }

    public void setResponseCode(Integer responseCode) {
        this.responseCode = responseCode;
    }

    public void setResponseTime(Integer responseTime) {
        this.responseTime = responseTime;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "TestEntity [" + (id != null ? "id=" + id + ", " : "")
            + (domain != null ? "domain=" + domain + ", " : "") + (url != null ? "url=" + url + ", " : "")
            + (responseTime != null ? "responseTime=" + responseTime + ", " : "")
            + (responseCode != null ? "responseCode=" + responseCode + ", " : "")
            + (downloadTime != null ? "downloadTime=" + downloadTime + ", " : "")
            + (notMappedField != null ? "notMappedField=" + notMappedField : "") + "]\n";
    }
}
