package server;

import java.io.*;
import java.net.*;
import java.util.Properties;

public class Slave {
    private String ip;
    private int port;
    private String localRoot;
    private int masterPort;

    public Slave(String configFile) {
        loadConfig(configFile);
    }

    public void loadConfig(String configFile) {
        try {
            Properties prop = new Properties();
            FileInputStream input = new FileInputStream(configFile);
            prop.load(input);

            ip = prop.getProperty("slave_ip");
            port = Integer.parseInt(prop.getProperty("slave_port"));
            localRoot = prop.getProperty("local_root");
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

            InetAddress masterAddress = InetAddress.getByName("255.255.255.255"); // Broadcast address
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
            if (command.startsWith("partition ")) {
                String[] parts = command.split(" ", 4);
                String fileName = parts[1];
                int partitionIndex = Integer.parseInt(parts[2]);
                String fileContent = parts[3];

                // Store the partition locally
                storePartition(fileName, partitionIndex, fileContent);

                // Send acknowledgment to the master
                out.writeUTF("ACK:" + ip + ":" + port + ":" + fileName + ":" + partitionIndex);
                System.out.println("Partition stored and ACK sent to master.");
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
                out.writeUTF("ACK"); // Ensure ACK is sent
            }
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
            return data.toString().trim();
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
            Slave slave = new Slave("../conf/slave1.conf");
            if (slave != null) {
                slave.sendRegisterMessage();
                slave.listenForMasterCommands();
            }
        }).start();
        new Thread(() -> {
            Slave slave = new Slave("../conf/slave2.conf");
            if (slave != null) {
                slave.sendRegisterMessage();
                slave.listenForMasterCommands();
            }
        }).start();
    }
}
