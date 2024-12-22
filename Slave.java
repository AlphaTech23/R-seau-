package server;

import java.io.*;
import java.net.*;
import java.util.*;

public class Slave {
    private String ip;
    private int port;
    private String localRoot;
    private int masterPort;
    String configFile = "../conf/slave.conf";

    public Slave(int  index) {
        loadConfig(index);
    }

    public void loadConfig(int index) {
        try {
            Properties prop = new Properties();
            FileInputStream input = new FileInputStream(configFile);

            prop.load(input);

            ip = prop.getProperty("slave" + index + ".ip");
            port = Integer.parseInt(prop.getProperty("slave" + index + ".port"));
            localRoot = prop.getProperty("slave" + index + ".local_root");
            masterPort = Integer.parseInt(prop.getProperty("master_port"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendRegisterMessage() {
        try {
            DatagramSocket socket = new DatagramSocket();
            socket.setBroadcast(true);

            String message = "REGISTER:" + ip + ":" + port + ":" + localRoot + ":" + masterPort;
            byte[] buffer = message.getBytes();

            InetAddress masterAddress = InetAddress.getByName("255.255.255.255"); 
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, masterAddress, masterPort);

            socket.send(packet);
            System.out.println("Slave sent REGISTER message to master: " + message);
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void listenForMasterCommands() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Slave listening for master commands on port " + port);

            while (true) {
                Socket masterSocket = serverSocket.accept();
                new Thread(() -> handleMasterCommand(masterSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleMasterCommand(Socket masterSocket) {
        try (DataInputStream in = new DataInputStream(masterSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(masterSocket.getOutputStream())) {

            String command = in.readUTF();
            if (command.startsWith("replied_partition ")) {
                String[] parts = command.split(" ", 4);
                String fileName = parts[1];
                int partitionIndex = Integer.parseInt(parts[2]);
                String fileContent = parts[3];

                storePartition(fileName, partitionIndex, fileContent);

                System.out.println("Partition stored and ACK sent to master.");

            } if (command.startsWith("partition ")) {
                String[] parts = command.split(" ", 5);
                String fileName = parts[1];
                int partitionIndex = Integer.parseInt(parts[2]);
                String replicationList = parts[3];
                String fileContent = parts[4];

                storePartition(fileName, partitionIndex, fileContent);
                out.writeUTF("ACK:" + ip + ":" + port + ":" + fileName + ":" + partitionIndex);

                if(!replicationList.equals(",")) {
                    String[] slaves = replicationList.split(",");
                    Random rand = new Random();
                    int index = rand.nextInt(slaves.length);
                    String[] info = slaves[index].split(":");
                    String ip = info[0]; 
                    int port = Integer.parseInt(info[1]);
                    String message = "replied_partition " + fileName + " " + partitionIndex + " " + fileContent;
                    sendMessage(ip, port, message);
                    out.writeUTF("REPLICATED_ACK:" + ip + ":" + port + ":" + fileName + ":" + partitionIndex);
                }

                System.out.println("Partition stored, replicate and ACK sent to master.");

            } else if (command.startsWith("get_partition ")) {
                String[] parts = command.split(" ", 3);
                String fileName = parts[1];
                int partitionIndex = Integer.parseInt(parts[2]);
    
                String partitionData = getPartitionData(fileName, partitionIndex);
                if (partitionData != null) {
                    out.writeUTF("PARTITION_DATA " + partitionData);
                } else {
                    out.writeUTF("ERROR: Partition not found.");
                }
            } else if (command.startsWith("delete_partition ")) {
                String fileName = command.substring(17);
                deletePartition(fileName);
                out.writeUTF("ACK"); 
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendMessage(String ip, int port, String message) {
        try (Socket socket = new Socket(ip, port);
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
                out.writeUTF(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getPartitionData(String fileName, int partitionIndex) {
        try {
            File partitionFile = new File(localRoot, fileName + "_part" + partitionIndex + ".txt");
            if (!partitionFile.exists()) {
                return null;
            }
    
            StringBuilder data = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(partitionFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    data.append(line).append("\n");
                }
            }
            return data.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void deletePartition(String fileName) {
        File directory = new File(localRoot);
        File[] files = directory.listFiles((dir, name) -> name.startsWith(fileName + "_part"));
        if (files != null) {
            for (File file : files) {
                if (file.delete()) {
                    System.out.println("Deleted partition: " + file.getName());
                } else {
                    System.out.println("Failed to delete partition: " + file.getName());
                }
            }
        }
    }

    private void storePartition(String fileName, int partitionIndex, String fileContent) {
        try {
            File fileDir = new File(localRoot);
            if (!fileDir.exists()) {
                fileDir.mkdirs();
            }

            File partitionFile = new File(fileDir, fileName + "_part" + partitionIndex + ".txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(partitionFile))) {
                writer.write(fileContent);
            }

            System.out.println("Stored partition: " + fileName + " (Index: " + partitionIndex + ")");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Thread(() -> {
            Slave slave = new Slave(1);
            if (slave != null) {
                slave.sendRegisterMessage();
                slave.listenForMasterCommands();
            }
        }).start();
        new Thread(() -> {
            Slave slave = new Slave(2);
            if (slave != null) {
                slave.sendRegisterMessage();
                slave.listenForMasterCommands();
            }
        }).start();
    }
}
