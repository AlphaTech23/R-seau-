package client;

import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Client {
    private String ipMaster;
    private int portMaster;

    public void connect(String ip, int port) {
        this.ipMaster = ip;
        this.portMaster = port;
        System.out.println("Connected to master at " + ip + ":" + port);
    }

    public void ls() {
        if (ipMaster == null || portMaster == 0) {
            System.out.println("Not connected to a master. Use 'connect <ip>:<port>' first.");
            return;
        }
    
        try (Socket socket = new Socket(ipMaster, portMaster);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
    
            out.writeUTF("ls");
    
            String response;
            while ((response = in.readUTF()) != null) {
                if (response.equals("END_OF_RESPONSE")) {
                    break;
                }
                System.out.println(response);
            }
    
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void get(String fileName, String destination) {
        if (ipMaster == null || portMaster == 0) {
            System.out.println("Not connected to a master. Use 'connect <ip>:<port>' first.");
            return;
        }
    
        try (Socket socket = new Socket(ipMaster, portMaster);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
    
            out.writeUTF("get " + fileName + " " + destination);
            System.out.println("Sent 'get' request for file: " + fileName);
    
            String response = in.readUTF();
            if (response.equals("SUCCESS")) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(destination))) {
                    String line;
                    while (!(line = in.readUTF()).equals("EOF")) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
                System.out.println("File saved to: " + destination);
            } else {
                System.out.println(response); // Error message
            }
    
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void upload(String filePath) {
        if (ipMaster == null || portMaster == 0) {
            System.out.println("Not connected to a master. Use 'connect <ip>:<port>' first.");
            return;
        }

        try (Socket socket = new Socket(ipMaster, portMaster);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             BufferedReader fileReader = new BufferedReader(new FileReader(filePath))) {

            out.writeUTF("upload " + filePath);
            System.out.println("Sent upload request for file: " + filePath);

            String line;
            while ((line = fileReader.readLine()) != null) {
                out.writeUTF(line);
            }
            out.writeUTF("EOF"); 
            System.out.println("File content sent to master.");

        } catch (IOException e) {
            System.out.println(ipMaster+" "+portMaster);
            e.printStackTrace();
        }
    }

    public void rm(String fileName) {
        if (ipMaster == null || portMaster == 0) {
            System.out.println("Not connected to a master. Use 'connect <ip>:<port>' first.");
            return;
        }
    
        try (Socket socket = new Socket(ipMaster, portMaster);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
    
            out.writeUTF("rm " + fileName);
            System.out.println("Sent 'rm' request for file: " + fileName);
    
            String response = in.readUTF();
            System.out.println(response);
    
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client();
        Scanner scanner = new Scanner(System.in);

        System.out.println("Client CLI. Use commands: connect <ip>:<port>, upload <filepath>");

        while (true) {
            System.out.print("> ");
            String command = scanner.nextLine();
            String[] parts = command.split(" ");

            if (parts[0].equals("connect") && parts.length == 2) {
                String[] address = parts[1].split(":");
                client.connect(address[0], Integer.parseInt(address[1]));
            } else if (parts[0].equals("upload") && parts.length == 2) {
                client.upload(parts[1]);
            } else if (parts[0].equals("ls")) {
                client.ls();
            } else if (parts[0].equals("exit")) {
                scanner.close();
                break;
            } else if (parts[0].equals("get") && parts.length == 3) {
                client.get(parts[1], parts[2]);
            } else if (parts[0].equals("rm") && parts.length == 2) {
                client.rm(parts[1]);
            } else {
                System.out.println("Invalid command.");
            } 
        }
    }
}
