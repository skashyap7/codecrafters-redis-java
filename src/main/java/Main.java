import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
  public static final int REDIS_CONNECTION_PORT = 6379;
  public static final String PONG_REPLY= "+PONG\r\n";
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    try {
      serverSocket = new ServerSocket(REDIS_CONNECTION_PORT);
      serverSocket.setReuseAddress(true);
      // Start Executor service
      ExecutorService executorService = new ThreadPoolExecutor(1, 10, 10L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
      // wait for connection from client
      clientSocket = serverSocket.accept();
      // Send a response for PING
      Socket finalClientSocket = clientSocket;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          handleRequest(finalClientSocket);
        }
      });
    } catch (IOException ex) {
      System.out.println("IOException: " + ex.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException ex) {
        System.out.println(" IOException: "+ ex.getMessage());
      }
    }
  }
  private static void handleRequest(Socket clientSocket) {
    boolean autoflush = true;
    try (PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), autoflush)) {
      // read a few bytes from clientSocket
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      String clientCommand;
      while ((clientCommand = inputReader.readLine()) != null) {
        if (clientCommand.equalsIgnoreCase("ping")) {
          // Respond to client using OutputStream as in previous stage
          output.println("+PONG\r");
        }
      }
    } catch (IOException e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
}
