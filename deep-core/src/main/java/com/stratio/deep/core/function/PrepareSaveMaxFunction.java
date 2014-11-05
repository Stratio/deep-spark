package com.stratio.deep.core.function;

import static com.stratio.deep.commons.utils.Utils.getExtractorInstance;
import static com.stratio.deep.core.util.ExtractorClientUtil.getExtractorClient;

import java.io.Serializable;

import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.exception.DeepExtractorinitializationException;
import com.stratio.deep.commons.rdd.IExtractor;

public class PrepareSaveMaxFunction<T, S extends BaseConfig<T>> extends
		AbstractFunction1<Iterator<T>, BoxedUnit> implements Serializable {

	private S config;

	private T first;

	public PrepareSaveMaxFunction(S config, T first) {
		this.first = first;
		this.config = config;
	}

	@Override
	public BoxedUnit apply(Iterator<T> v1) {
		IExtractor<T, S> extractor;
		try {
			extractor = getExtractorInstance(config);
		} catch (DeepExtractorinitializationException e) {
			extractor = getExtractorClient();
		}

		extractor.initSave(config, first);
		while (v1.hasNext()) {
			extractor.saveRDD(v1.next());
		}
		config.setPartitionId(config.getPartitionId() + 1);
		extractor.close();
		return null;
	}

}