package com.stratio.deep.jdbc.testentity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;

/**
 * Created by mariomgal on 12/12/14.
 */
@DeepEntity
public class MessageTestEntity {

    @DeepField
    public String id;

    @DeepField
    public String message;

    @DeepField
    public Long number;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }
}
