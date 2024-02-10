import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

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
      // wait for connection from client
      clientSocket = serverSocket.accept();
      // Send a response for PING
      boolean autoflush = true;
      PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), autoflush);

      // read a few bytes from clientSocket
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      String clientCommand;
      while ((clientCommand = inputReader.readLine()) != null) {
        // Respond to client using OutputStream as in previous stage
        output.print("+PONG\r\n");
        output.flush();
      }
      //System.out.println("Data recieved "+ is.readAllBytes().toString());
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

    //  Uncomment this block to pass the first stage
    //    ServerSocket serverSocket = null;
    //    Socket clientSocket = null;
    //    int port = 6379;
    //    try {
    //      serverSocket = new ServerSocket(port);
    //      serverSocket.setReuseAddress(true);
    //      // Wait for connection from client.
    //      clientSocket = serverSocket.accept();
    //    } catch (IOException e) {
    //      System.out.println("IOException: " + e.getMessage());
    //    } finally {
    //      try {
    //        if (clientSocket != null) {
    //          clientSocket.close();
    //        }
    //      } catch (IOException e) {
    //        System.out.println("IOException: " + e.getMessage());
    //      }
    //    }
  }
}
