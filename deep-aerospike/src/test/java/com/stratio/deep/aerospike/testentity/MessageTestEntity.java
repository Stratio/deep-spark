package com.stratio.deep.aerospike.testentity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by mariomgal on 01/01/2014.
 */
@DeepEntity
public class MessageTestEntity implements IDeepType {

    @DeepField(fieldName = "_id")
    private Integer id;

    @DeepField
    private String message;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
