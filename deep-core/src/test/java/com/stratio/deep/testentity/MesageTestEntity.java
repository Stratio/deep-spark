package com.stratio.deep.testentity;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.IDeepType;
import org.bson.types.ObjectId;

/**
 * Created by rcrespo on 18/06/14.
 */
@DeepEntity
public class MesageTestEntity implements IDeepType {

    @DeepField( fieldName = "_id")
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
