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

package com.stratio.deep.core.function;

import static com.stratio.deep.commons.utils.Utils.getExtractorInstance;
import static com.stratio.deep.core.util.ExtractorClientUtil.getExtractorClient;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.VoidFunction;

import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepExtractorinitializationException;
import com.stratio.deep.commons.rdd.IExtractor;

//public class SaveMaxFunction<T, S extends BaseConfig<T>> implements VoidFunction<Cells>, Serializable {
//
//    private String columnName;
//    private List<String> primaryKeys;
//    private S config;
//    private T first;
//
//    public SaveMaxFunction(S config, T first, String qualifiedColumnName, String... primaryKeys) {
//        this.first = first;
//        this.config = config;
//        this.columnName = columnName;
//        this.primaryKeys = Arrays.asList(primaryKeys);
//    }
//
//    /*
//     * (non-Javadoc)
//     * 
//     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
//     */
//    @Override
//    public void call(Cells cells) throws Exception {
//        String[] namespace = cells.getnameSpace().split("\\.");
//        String catalogName = namespace[0];
//        String tableName = namespace[1];
//
//        // TODO create a single server instead of one for each Cells
//        IExtractor<T, S> extractor;
//        try {
//            extractor = getExtractorInstance(config);
//        } catch (DeepExtractorinitializationException e) {
//            extractor = getExtractorClient();
//        }
//
//        System.out.println("******" + catalogName);
//        extractor.saveMaxRDD(first, config, columnName, primaryKeys);
//
//        // config.setPartitionId(config.getPartitionId() + 1);
//        // extractor.close();
//
//    }
//
//}

public class SaveMaxFunction<T, S extends BaseConfig<T>> extends
		AbstractFunction1<Iterator<T>, BoxedUnit> implements Serializable {

	private S config;

	private T first;
	private String columnName;
	private List<String> primaryKeys;

	public SaveMaxFunction(S config, T first, String qualifiedColumnName,
			String... primaryKeys) {
		this.first = first;
		this.config = config;
		this.columnName = qualifiedColumnName;
		this.primaryKeys = Arrays.asList(primaryKeys);
	}

	@Override
	public BoxedUnit apply(Iterator<T> v1) {
		IExtractor<T, S> extractor;
		try {
			extractor = getExtractorInstance(config);
		} catch (DeepExtractorinitializationException e) {
			extractor = getExtractorClient();
		}

		extractor.initSaveMax(config, first);
		while (v1.hasNext()) {
			extractor.saveMaxRDD(v1.next(), columnName, primaryKeys);
		}
		config.setPartitionId(config.getPartitionId() + 1);
		extractor.close();
		return null;
	}

}
