import jakarta.servlet.*;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import json.Email;
import json.Order;
import kafka.dto.CorrelationID;
import kafka.producer.KafkaDispatcher;
import org.eclipse.jetty.servlet.Source;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet implements Servlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
        try {
            var email = req.getParameter("email");
            var value = req.getParameter("value");
            this.orderDispatcher.send("ECOMMERCE_NEW_ORDER"
                    , email
                    , new Order(UUID.randomUUID().toString()
                            , email
                            , new BigDecimal(value)),
                    new CorrelationID(NewOrderServlet.class.getSimpleName()));
            System.out.println("New Order was successfully");
            resp.getWriter().println("New Order was successfully posted");
        } catch (Exception e) {
            System.err.println("A error occur while the system eas processing your purchase");
            throw new ServletException("A error occur while the system eas processing your purchase", e);

        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            orderDispatcher.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
