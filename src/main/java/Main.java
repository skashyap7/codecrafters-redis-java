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
      while (true) {
        clientSocket = serverSocket.accept();
        final Socket currentSocketConnection = clientSocket;
        // Send a response for PING
        new Thread(() -> {
            handleRequest(currentSocketConnection);
        }).start();
      }
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
      // read a line from clientSocket
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      String clientCommand;
      while ((clientCommand = inputReader.readLine()) != null) {
        System.out.println(" Echo command = " + clientCommand );
        if (clientCommand.equalsIgnoreCase("ping")) {
          // Respond to client using OutputStream as in previous stage
          output.println("+PONG\r");
        }
        else if (clientCommand.contains("ECHO") || clientCommand.contains("echo")){
          handleEcho(clientCommand, output);
        }
      }
    } catch (IOException e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }

  private static void handleEcho(String clientCommand, PrintWriter output) throws IOException {
    System.out.println(" Echo command = " + clientCommand );
    // Since the ECHO format is already known to be an array
    // and Bulk Strings just use them as is
    String protocolTerminator = "\r\n";
    String[] parts = clientCommand.split(protocolTerminator);
    if (parts.length < 3 || parts[1].equalsIgnoreCase("4")
    || !parts[2].equalsIgnoreCase("ECHO")) {
      throw new IOException("Invalid argument");
    }
    else {
      System.out.println(" Value = " + parts[4]);
      output.print(parts[4]);
    }
  }

  // Sample for ECHO  : *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
  // Bulk Strings: $<length>\r\n<data>\r\n
  private static void handleCommand(String clientCommand) {
    String protocolTerminator = "\r\n"; // As per RESP \r\n is the protocol terminator
    char[] chars = clientCommand.toCharArray();
    int index = 0;

    while (index < chars.length) {

    }
  }
}
