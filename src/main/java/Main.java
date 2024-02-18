import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
  public static final int DEFAULT_REDIS_CONNECTION_PORT = 6379;
  public static final String PONG_REPLY= "+PONG\r\n";
  public static final Map<String,KeyValue> redisStore = new HashMap<>();
  public static void main(String[] args){
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = DEFAULT_REDIS_CONNECTION_PORT;
    if (args.length >= 2 && args[0].equalsIgnoreCase("--port")) {
      port = Integer.parseInt(args[1]);
    }
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
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
      Command currentCommand = new Command();
      while ((clientCommand = inputReader.readLine()) != null) {
        System.out.println(" Echo command = " + clientCommand );
        currentCommand.process(clientCommand);
        if (currentCommand.isComandComplete()) {
          currentCommand.runCommand(output);
          currentCommand = new Command();
        }
      }
    } catch (IOException e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }

  public static class Command {
    boolean dataTypeProcessed = false;
    String dataType;
    int lengthData = 0;
    int dataProcessed = 0;

    String command;
    List<String> arguments = new ArrayList<>();
    int currentLength = 0;
    String current;
    public void process(String str) {
      //System.out.println(" Processing data = " + str);
      if (!dataTypeProcessed) {
        char firstByte = str.charAt(0);
        switch (firstByte) {
          case '+':
            this.dataType = "Simple strings";
            break;
          case '-':
            this.dataType = "Simple Errors";
            break;
          case ':':
            this.dataType = "Integers";
            break;
          case '$':
            this.dataType = "Bulk strings";
            break;
          case '*':
            this.dataType = "Arrays";
            break;
          case '_':
            this.dataType = "Nulls";
            break;
          case '#':
            this.dataType = "Booleans";
            break;
          case ',':
            this.dataType = "Doubles";
            break;
          case '(':
            this.dataType = "Big numbers";
            break;
          case '!':
            this.dataType = "Bulk errors";
            break;
          case '=':
            this.dataType = "Verbatim strings";
            break;
          case '%':
            this.dataType = "Maps";
            break;
          case '~':
            this.dataType = "Sets";
            break;
          case '>':
            this.dataType = "Pushes";
            break;
        }
        this.dataTypeProcessed = true;
        this.lengthData = Integer.parseInt(String.valueOf(str.charAt(1)));
        return;
      }
      if (dataProcessed < lengthData) {
        if (str.startsWith("$")) {
          currentLength = str.charAt(1);
        }
        else {
          current = str;
          if (command == null) {
            command = current;
          }
          else {
            arguments.add(current);
          }
          dataProcessed++;
        }
      }
    }

    public boolean isComandComplete() {
      return (dataProcessed == lengthData);
    }

    public void runCommand(PrintWriter output) {
      System.out.println(" Command = " + command);
      switch (command.toLowerCase()) {
        case "echo":
          String outputStr = String.join(" ",arguments);
          System.out.println(outputStr);
          output.printf("$%d\r\n%s\r\n", outputStr.length(), outputStr);
          break;
        case "ping":
          output.println("+PONG\r");
          break;
        case "set":
          long expiry = 0L;
          if (this.arguments.size() > 2 && arguments.get(2).equalsIgnoreCase("px")) {
            expiry = System.currentTimeMillis() + Long.parseLong(this.arguments.get(3));
          }
          redisStore.put(this.arguments.get(0), new KeyValue(this.arguments.get(1), expiry));
          output.printf("$%d\r\n%s\r\n", "OK".length(), "OK");

          break;
        case "get":
          String key = this.arguments.get(0);
          KeyValue value = redisStore.get(key);
          boolean isExpired = (value.expiry <= System.currentTimeMillis() && (value.expiry != 0L));
          if (!redisStore.containsKey(key) || isExpired) {
            output.printf("$-1\r\n");
          }
          else {
            System.out.println(value);
            output.printf("$%d\r\n%s\r\n", value.val.length(), value.val);
          }
          break;
        default:
          System.out.println(" Unknown command "+ command);
      }
    }

  }

  public static class KeyValue {
    String val;
    long expiry;

    public KeyValue(){}

    public KeyValue(String _val, long _expiry) {
      this.val = _val;
      this.expiry = _expiry;
    }
  }
}

