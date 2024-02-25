import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static final int DEFAULT_REDIS_CONNECTION_PORT = 6379;
  public static int masterPort = 0;
  public static String masterHost = null;

  public static boolean isMaster = true;
  public static void main(String[] args){
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = DEFAULT_REDIS_CONNECTION_PORT;
    if (args.length >= 2 && (args[0].equalsIgnoreCase("--port") || args[0].equalsIgnoreCase("-p")) ) {
      port = Integer.parseInt(args[1]);
    }
    if (args.length >= 4 && args[2].equalsIgnoreCase("--replicaof")) {
      masterHost = args[3];
      masterPort = Integer.parseInt(args[4]);
      isMaster = false;
    }
    try {
      Server server = new Server(port, masterHost, masterPort, isMaster);
      server.startServer();
    } catch (IOException ex) {
      System.out.println("IOException: " + ex.getMessage());
    }
  }
}

