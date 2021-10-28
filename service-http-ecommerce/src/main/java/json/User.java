package json;

public class User {
    private String uuid;

    public User(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getPath() {
        return "service-reading-report/target/" + uuid + "-report.txt";
    }
}
