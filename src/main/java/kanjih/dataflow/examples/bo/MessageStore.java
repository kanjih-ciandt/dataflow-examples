package kanjih.dataflow.examples.bo;

import java.io.Serializable;

public class MessageStore implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 7372893644529579269L;
    
    private String createDateTime;
    private String messageType;
    private String fromApp;
    private String correlationId;
    private String storeId;
    private String itemId;
    private Integer stockRoom;
    private Integer salesFloor;
    private Integer storeInProcess;
    private String eventDate;
    public String getCreateDateTime() {
        return createDateTime;
    }
    public void setCreateDateTime(String createDateTime) {
        this.createDateTime = createDateTime;
    }
    public String getMessageType() {
        return messageType;
    }
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }
    public String getFromApp() {
        return fromApp;
    }
    public void setFromApp(String fromApp) {
        this.fromApp = fromApp;
    }
    public String getCorrelationId() {
        return correlationId;
    }
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    public String getStoreId() {
        return storeId;
    }
    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }
    public String getItemId() {
        return itemId;
    }
    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
    public Integer getStockRoom() {
        return stockRoom;
    }
    public void setStockRoom(Integer stockRoom) {
        this.stockRoom = stockRoom;
    }
    public Integer getSalesFloor() {
        return salesFloor;
    }
    public void setSalesFloor(Integer salesFloor) {
        this.salesFloor = salesFloor;
    }
    public Integer getStoreInProcess() {
        return storeInProcess;
    }
    public void setStoreInProcess(Integer storeInProcess) {
        this.storeInProcess = storeInProcess;
    }
    public String getEventDate() {
        return eventDate;
    }
    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }
    public static long getSerialversionuid() {
        return serialVersionUID;
    }
    
    @Override
    public String toString() {
        return "MessageStore [createDateTime=" + createDateTime + ", messageType=" + messageType + ", fromApp="
                + fromApp + ", correlationId=" + correlationId + ", storeId=" + storeId + ", itemId=" + itemId
                + ", stockRoom=" + stockRoom + ", salesFloor=" + salesFloor + ", storeInProcess=" + storeInProcess
                + ", eventDate=" + eventDate + "]";
    }
}
