package com.stratio.deep.testentity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by mariomgal on 19/11/14.
 */
@DeepEntity
public class MessageEntity implements IDeepType {

    @DeepField(fieldName = "id")
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
