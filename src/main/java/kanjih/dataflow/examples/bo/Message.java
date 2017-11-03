package kanjih.dataflow.examples.bo;

import java.io.Serializable;

public class Message implements Serializable {

    private static final long serialVersionUID = -5730389958206087436L;

    private String createDate;
    private String from;
    private String msgId;
    private String msgType;
    
    private String storeId;
    private String productId;
    private String eventDateTime;
    private Integer aQtdy;
    private Integer bQtdy;
    private Integer cQtdy;
    private Integer dQtdy;
    
    public String getCreateDate() {
        return createDate;
    }
    public void setCreateDate(String createDate) {
        this.createDate = createDate;
    }
    public String getFrom() {
        return from;
    }
    public void setFrom(String from) {
        this.from = from;
    }
    public String getMsgId() {
        return msgId;
    }
    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
    public String getMsgType() {
        return msgType;
    }
    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }
    public String getStoreId() {
        return storeId;
    }
    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }
    public String getProductId() {
        return productId;
    }
    public void setProductId(String productId) {
        this.productId = productId;
    }
  
    public Integer getaQtdy() {
        return aQtdy;
    }
    public void setaQtdy(Integer aQtdy) {
        this.aQtdy = aQtdy;
    }
    public Integer getbQtdy() {
        return bQtdy;
    }
    public void setbQtdy(Integer bQtdy) {
        this.bQtdy = bQtdy;
    }
    public Integer getcQtdy() {
        return cQtdy;
    }
    public void setcQtdy(Integer cQtdy) {
        this.cQtdy = cQtdy;
    }
    public Integer getdQtdy() {
        return dQtdy;
    }
    public void setdQtdy(Integer dQtdy) {
        this.dQtdy = dQtdy;
    }
    
    
  
    public String getEventDateTime() {
        return eventDateTime;
    }
    public void setEventDateTime(String eventDateTime) {
        this.eventDateTime = eventDateTime;
    }
    @Override
    public String toString() {
        return "Message [createDate=" + createDate + ", from=" + from + ", msgId=" + msgId + ", msgType=" + msgType
                + ", storeId=" + storeId + ", productId=" + productId + ", eventDateTime=" + eventDateTime + ", aQtdy="
                + aQtdy + ", bQtdy=" + bQtdy + ", cQtdy=" + cQtdy + ", dQtdy=" + dQtdy + "]";
    }
    
    
}
