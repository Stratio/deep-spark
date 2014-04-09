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
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

/**
 * Author: Emmanuelle Raffenne
 */

@DeepEntity
public class TweetEntity implements IDeepType {

	private static final long serialVersionUID = 7743109162467182820L;
	
	@DeepField(fieldName="tweet_id", isPartOfPartitionKey = true, validationClass = UUIDType.class)
	private java.util.UUID tweetID;
	
	@DeepField(fieldName="tweet_date", validationClass = TimestampType.class)
	private java.util.Date tweetDate;

	@DeepField(validationClass = UTF8Type.class)
	private String author;

	@DeepField(validationClass = SetType.class )
	private java.util.Set<String> hashtags;

	@DeepField(fieldName = "favorite_count", validationClass = Int32Type.class)
	private Integer favoriteCount;

	@DeepField(validationClass = UTF8Type.class)
	private String content;

	@DeepField(fieldName = "truncated", validationClass = BooleanType.class)
	private Boolean isTruncated;

	public java.util.UUID getTweetID() {
		return tweetID;
	}

	public void setTweetID(java.util.UUID tweetID) {
		this.tweetID = tweetID;
	}

	public java.util.Date getTweetDate() {
		return tweetDate;
	}

	public void setTweetDate(java.util.Date tweetDate) {
		this.tweetDate = tweetDate;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public java.util.Set<String> getHashtags() {
		return hashtags;
	}

	public void setHashtags(java.util.Set<String> hashtags) {
		this.hashtags = hashtags;
	}

	public Integer getFavoriteCount() {
		return favoriteCount;
	}

	public void setFavoriteCount(Integer favoriteCount) {
		this.favoriteCount = favoriteCount;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Boolean getIsTruncated() {
		return isTruncated;
	}

	public void setIsTruncated(Boolean isTruncated) {
		this.isTruncated = isTruncated;
	}
}
