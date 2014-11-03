package com.stratio.deep.aerospike.testentity;

import com.stratio.deep.commons.annotations.DeepField;

/**
 * Created by mariomgal on 01/11/14.
 */
public class AerospikeTestEntity {

    @DeepField(fieldName = "_id")
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
