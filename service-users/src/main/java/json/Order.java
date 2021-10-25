package json;

import java.math.BigDecimal;

public class Order {
    private String userId, orderId, email;
    private BigDecimal amount;

    public Order(String userId, String orderId, String email, BigDecimal amount) {
        this.userId = userId;
        this.orderId = orderId;
        this.email = email;
        this.amount = amount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
