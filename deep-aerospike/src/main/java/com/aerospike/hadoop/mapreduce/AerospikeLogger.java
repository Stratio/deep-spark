/* 
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.aerospike.hadoop.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AerospikeLogger implements com.aerospike.client.Log.Callback {

    private static final Log log = LogFactory.getLog(AerospikeLogger.class);

	@Override
	public void log(com.aerospike.client.Log.Level level, String message) {
        switch (level) {
        case ERROR:
            log.error(message);
            break;
        case WARN:
            log.warn(message);
            break;
        case INFO:
            log.info(message);
            break;
        case DEBUG:
            log.debug(message);
            break;
        }
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
