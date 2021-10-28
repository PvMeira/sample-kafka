import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import kafka.dto.CorrelationID;
import kafka.producer.KafkaDispatcher;

import java.io.IOException;

public class GenerateAllReportServlet extends HttpServlet implements Servlet {

    private final KafkaDispatcher<String> dispatcher = new KafkaDispatcher<>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws  ServletException {
        try {

            dispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                    "ECOMMERCE_USER_GENERATE_READING_REPORT",
                   "ECOMMERCE_USER_GENERATE_READING_REPORT",
                         new CorrelationID(GenerateAllReportServlet.class.getSimpleName()));
            System.out.println("Generate All Reports order was successfully posted");
            resp.getWriter().println("Generate All Reports order was successfully posted");
        } catch (Exception e) {
            System.err.println("A error occur while the system was processing your request");
            throw new ServletException("A error occur while the system was processing your request", e);

        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            dispatcher.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
