package com.stratio.deep.testentity;

import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

import java.util.List;

/**
 * Created by rcrespo on 5/08/14.
 */
public class TweetES implements IDeepType{

    @DeepField
    private String user;
    @DeepField
    private String message;
    @DeepField
    private String postDate;
    @DeepField
    private List<DocumentTest> arraySubdocumento;
    @DeepField
    private DocumentTest subDocumento;
    @DeepField
    private Long numerico;


    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getPostDate() {
        return postDate;
    }

    public void setPostDate(String postDate) {
        this.postDate = postDate;
    }

    public List<DocumentTest> getArraySubdocumento() {
        return arraySubdocumento;
    }

    public void setArraySubdocumento(List<DocumentTest> arraySubdocumento) {
        this.arraySubdocumento = arraySubdocumento;
    }

    public DocumentTest getSubDocumento() {
        return subDocumento;
    }

    public void setSubDocumento(DocumentTest subDocumento) {
        this.subDocumento = subDocumento;
    }

    public Long getNumerico() {
        return numerico;
    }

    public void setNumerico(Long numerico) {
        this.numerico = numerico;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TweetES{");
        sb.append("user='").append(user).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", postDate='").append(postDate).append('\'');
        sb.append(", arraySubdocumento=").append(arraySubdocumento);
        sb.append(", subDocumento=").append(subDocumento);
        sb.append(", numerico=").append(numerico);
        sb.append('}');
        return sb.toString();
    }
}


