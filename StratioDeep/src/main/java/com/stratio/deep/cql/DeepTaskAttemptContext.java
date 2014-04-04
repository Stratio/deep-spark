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

package com.stratio.deep.cql;

import com.stratio.deep.config.impl.GenericDeepJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Custom {@link org.apache.hadoop.mapreduce.TaskAttemptContext} needed in order to
 * propagate a deep's own configuration object ({@link com.stratio.deep.config.impl.GenericDeepJobConfig}).
 */
public class DeepTaskAttemptContext<T> extends TaskAttemptContext {
    GenericDeepJobConfig<T> conf;

    /**
     * Public constructor.
     *
     * @param conf
     * @param taskId
     */
    public DeepTaskAttemptContext(GenericDeepJobConfig<T> conf, TaskAttemptID taskId) {
        super(conf.getConfiguration(), taskId);

        this.conf = conf;
    }
}
