public class LogData {

    private String           host;
    private String           timestamp;
    private String           request;
    private String           reply;
    private String           columnByteReply;


    public String getHost() {
        return host;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getRequest() {
        return request;
    }

    public String getReply() {
        return reply;
    }

    public String getColumnByteReply() {
        return columnByteReply;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public void setReply(String reply) {
        this.reply = reply;
    }

    public void setColumnByteReply(String columnByteReply) {
        this.columnByteReply = columnByteReply;
    }


    //public LogData( String host,
    //                String timestamp,
    //                String request,
    //                String reply,
    //                String columnByteReply)
    //{
//
    //    this.host = host;
    //    this.timestamp = timestamp;
    //    this.request = request;
    //    this.reply = reply;
    //    this.columnByteReply = columnByteReply;
    //}

    @Override
    public String toString() {
        return "Host: " + host +
        " Timestamp: " + timestamp +
        " Request: " + request +
        " Reply: " + reply +
        " ColumnByteReply: " + columnByteReply +
        " \n";
    }
}
