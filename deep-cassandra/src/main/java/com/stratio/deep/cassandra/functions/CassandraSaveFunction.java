package com.stratio.deep.cassandra.functions;

import com.datastax.driver.core.Session;
import com.stratio.deep.commons.functions.SaveFunction;

/**
 * Created by david on 5/11/14.
 */
public abstract class CassandraSaveFunction implements SaveFunction{

        protected Session session;

        public void setSession(Session session){
            this.session = session;
        }
}
