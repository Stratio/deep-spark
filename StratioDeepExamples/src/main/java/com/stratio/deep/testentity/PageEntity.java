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
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;

import java.math.BigInteger;

/**
 * Author: Luca Rosellini
 */

@DeepEntity
public class PageEntity implements IDeepType {

    private static final long serialVersionUID = -9213306241759793383L;

    @DeepField(fieldName = "key", isPartOfPartitionKey = true)
    private String id;

    @DeepField(fieldName = "domainName")
    private String domainName;

    @DeepField
    private String url;

    @DeepField
    private String charset;

    @DeepField
    private String content;

    @DeepField(fieldName = "downloadTime", validationClass = LongType.class)
    private Long downloadTime;

    @DeepField(fieldName = "firstDownloadTime", validationClass = LongType.class)
    private Long firstDownloadTime;

    @DeepField(fieldName = "responseCode", validationClass = IntegerType.class )
    private BigInteger responseCode;

    @DeepField(fieldName = "responseTime", validationClass = LongType.class)
    private Long responseTime;

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

    public BigInteger getResponseCode() {
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

    public void setResponseCode(BigInteger responseCode) {
        this.responseCode = responseCode;
    }

    public void setResponseTime(Long responseTime) {
        this.responseTime = responseTime;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
