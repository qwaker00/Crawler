import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

public class WServer extends Thread
{
    private ServerSocket server;
    private WServerHandler handler;
    private int port;
    private volatile CountDownLatch finished = new CountDownLatch(1);

    public WServer(WServerHandler handler, int port)
    {
        this.handler = handler;
        this.port = port;
    }

    public void setStop() {
        this.finished.countDown();
        try
        {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(this.port));
            s.close();
        } catch (IOException ignored) {}
    }

    public void waitFinish() {
        if (isAlive()) try {
            join();
        } catch (InterruptedException ignored) {
        }
    }

    public void run() {
        try { this.server = new ServerSocket(this.port);
            while (this.finished.getCount() > 0L) {
                Socket s = this.server.accept();
                new WServer.Worker(s, this.handler).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                this.server.close();
            }
            catch (Exception ignored)
            {
            }
        }
    }

    class Worker extends Thread
    {
        private Socket s;
        private WServerHandler handler;

        public Worker(Socket s, WServerHandler handler)
        {
            this.s = s;
            this.handler = handler;
        }

        public void run()
        {
            try {
                PrintStream printer = new PrintStream(this.s.getOutputStream(), true);
                String headLine = new BufferedReader(new InputStreamReader(this.s.getInputStream())).readLine();
                if ((headLine != null) && (headLine.startsWith("GET"))) {
                    this.s.setSoTimeout(2000000000);
                    String params = headLine.substring(5, headLine.indexOf(' ', 5));
                    String response = this.handler.getResponse(params);
                    if (response != null) {
                        byte[] content = response.getBytes(Charset.forName("UTF-8"));
                        printer.print("HTTP/1.0 200 OK\nConnection: close\nContent-length: " + content.length + "\nContent-type: text/html\n\n");
                        this.s.getOutputStream().write(content);
                    } else {
                        printer.print("HTTP/1.0 400 Bad Request\n\n");
                        this.s.setSoTimeout(500);
                    }
                } else {
                    printer.print("HTTP/1.0 400 Bad Request\n\n");
                    this.s.setSoTimeout(500);
                }
                this.s.getOutputStream().flush();
                this.s.close();
            }
            catch (IOException ignored) { }
        }
    }
}
