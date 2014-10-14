package com.stratio.deep.mongodb.testentity;

import org.bson.types.ObjectId;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by rcrespo on 18/06/14.
 */
@DeepEntity
public class MessageTestEntity implements IDeepType {

    @DeepField(fieldName = "_id")
    private ObjectId id;

    @DeepField
    private String message;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
