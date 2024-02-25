import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
This class represents a Redis server
 */
public class Server {
    public boolean isMaster;
    public String host;
    public String masterHost;

    public int port;
    public int masterPort;

    private ServerSocket serverSocket;
    private  Socket clientSocket = null;
    public static final Map<String,KeyValue> redisStore = new HashMap<>();

    public Server(int port, String masterHost, int masterPort, boolean isMaster)  {
        this.port = port;
        this.masterHost = masterHost;
        this.isMaster = isMaster;
        this.masterPort = masterPort;
    }

    public void startServer() throws IOException {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            if (!isMaster) {
                // Start Handshake with Master server
                startHandshake();
            }
            // wait for connection from client
            while (true) {
                clientSocket = serverSocket.accept();
                final Socket currentSocketConnection = clientSocket;
                // Send a response for PING
                new Thread(() -> {
                    handleRequest(currentSocketConnection);
                }).start();
            }
        }
        catch (IOException ex) {
            throw ex;
        }
        finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException ex) {
                System.out.println(" IOException: "+ ex.getMessage());
            }
        }
    }

    private void handleRequest(Socket clientSocket) {

        boolean autoflush = true;
        try (PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), autoflush)) {
            // read a line from clientSocket
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String clientCommand;
            Command currentCommand = new Command();
            while ((clientCommand = inputReader.readLine()) != null) {
                //System.out.println(" Echo command = " + clientCommand );
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

    public void startHandshake() {
        // Send a PING to master
        String replCommand1 = String.format("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", this.port);
        String replCommand2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        try (Socket masterSocket = new Socket(masterHost, masterPort)) {
            try (PrintWriter output = new PrintWriter(masterSocket.getOutputStream(), true)) {
                // write PING To the output stream
                output.print("*1\r\n$4\r\nping\r\n");
                //  Read output
                //BufferedReader inputReader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                //String responseToPing = inputReader.readLine();
                //if (responseToPing != null && responseToPing.equalsIgnoreCase("pong")) {
                    output.print(replCommand1);
                    //String responseToReplCommand1 = inputReader.readLine();
                    output.print(replCommand2);
                    //String responseToReplCommand2 = inputReader.readLine();
                    // Send the PSYCN Command
                    output.print(psyncCommand);
                //}
//              //else {
//              //      System.out.println("Failed to get response from master for  PING during handshake, response = "+ responseToPing);
//              //}
            } catch (IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private class Command {
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

        private boolean isComandComplete() {
            return (dataProcessed == lengthData);
        }

        public void runCommand(PrintWriter output) {
            System.out.println(" Command = " + command);
            switch (command.toLowerCase()) {
                case "echo":
                    executeEcho(output);
                    break;
                case "ping":
                    executePing(output);
                    break;
                case "set":
                    executeSet(output);
                    break;
                case "get":
                    executeGet(output);
                    break;
                case "info":
                    executeInfo(output);
                    break;
                case "replconf":
                    sendOk(output);
                case "psync":
                    sendOk(output);
                    break;
                default:
                    System.out.println(" Unknown command "+ command);
            }
        }

        private void sendOk(PrintWriter output) {
            output.printf("$%d\r\n%s\r\n", "OK".length(), "OK");
        }

        private void executePsync(PrintWriter output) {
            String response = "+FULLRESYNC <REPL_ID> 0";
            output.printf("$%d\r\n%s\r\n", response.length(), response);
        }
        private void executeEcho(PrintWriter output) {
            String outputStr = String.join(" ",arguments);
            System.out.println(outputStr);
            output.printf("$%d\r\n%s\r\n", outputStr.length(), outputStr);
        }

        private void executePing(PrintWriter output){
            output.println("+PONG\r");
        }

        private void executeSet(PrintWriter output) {
            long expiry = 0L;
            if (this.arguments.size() > 2 && arguments.get(2).equalsIgnoreCase("px")) {
                expiry = System.currentTimeMillis() + Long.parseLong(this.arguments.get(3));
            }
            redisStore.put(this.arguments.get(0), new KeyValue(this.arguments.get(1), expiry));
            sendOk(output);
        }

        private void executeGet(PrintWriter output) {
            String key = this.arguments.getFirst();
            KeyValue value = redisStore.get(key);
            boolean isExpired = (value.expiry <= System.currentTimeMillis() && (value.expiry != 0L));
            if (!redisStore.containsKey(key) || isExpired) {
                output.printf("$-1\r\n");
            }
            else {
                System.out.println(value);
                output.printf("$%d\r\n%s\r\n", value.val.length(), value.val);
            }
        }

        private void executeInfo(PrintWriter output) {
            System.out.println("INFO command");
            InfoReply infoReply = new InfoReply(isMaster ? "master" : "slave");
            infoReply.outputRespResponse(output);
        }
    }

    private class KeyValue {
        String val;
        long expiry;

        public KeyValue(){}

        public KeyValue(String _val, long _expiry) {
            this.val = _val;
            this.expiry = _expiry;
        }
    }

    private class InfoReply {
        public String role;
        public int connected_slaves = 0;
        public String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        public int master_repl_offset = 0;
        public int second_repl_offset = -1;
        public int repl_backlog_active = 0;
        public int repl_backlog_size = 1048576;
        public int repl_backlog_first_byte_offset = 0;
        public String repl_backlog_histlen = "";

        public InfoReply(String _role) {
            this.role = _role;
        }
        public void outputRespResponse(PrintWriter output) {
            StringBuilder stringBuilder = new StringBuilder();
            String replicationString = "# Replication";
            stringBuilder.append(String.format("%s\n", replicationString));
            try {
                Field[] fields = this.getClass().getDeclaredFields();
                for (Field f : fields) {
                    String line = String.format("%s:%s", f.getName(), f.get(this).toString());
                    System.out.println(line);
                    stringBuilder.append(String.format("%s\n", line));
                }
            }
            catch (IllegalAccessException ex) {
                System.out.println("Caught exception while accessing member fields" + ex.getMessage());
            }
            output.printf("$%d\r\n%s\r\n", stringBuilder.toString().length(), stringBuilder.toString());
        }
    }
}
