package server;

import java.io.*;
import java.net.*;
import java.util.*;

public class Master {
    String ip;
    int port;
    List<String> slaves;
    Map<String, List<String>> filePartitionMap; // Persistent data

    public Master(String configFile) {
        loadConfig(configFile);
        slaves = new ArrayList<>();
        filePartitionMap = new HashMap<>();
        loadPersistence();
    }

    public void loadConfig(String configFile) {
        try {
            Properties prop = new Properties();
            FileInputStream input = new FileInputStream(configFile);
            prop.load(input);

            ip = prop.getProperty("master_ip");
            port = Integer.parseInt(prop.getProperty("master_port"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listenForRegisterMessages() {
        try {
            DatagramSocket socket = new DatagramSocket(port);
            byte[] buffer = new byte[1024];

            System.out.println("Master listening for REGISTER messages on port " + port);
            Scanner scanner = new Scanner();
            while (true) {
                if(scanner.nextLine())
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String message = new String(packet.getData(), 0, packet.getLength());
                if (message.startsWith("REGISTER:")) {
                    String slaveDetails = message.substring(9);
                    slaves.add(slaveDetails);
                    System.out.println("Registered slave: " + slaveDetails);
                } 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int getPartition() {
        int max = 0;
        for(String key : filePartitionMap.keySet()) {
            for(String map : filePartitionMap.get(key)) {
                String[] values = map.split(",");
                int value = Integer.parseInt(values[0]);
                if(max < value) {
                    max = value;
                }
            }
        }
        return max + 1;
    }

    void handleGetRequest(String fileName, DataOutputStream clientOut) throws IOException {
        if (!filePartitionMap.containsKey(fileName)) {
            clientOut.writeUTF("ERROR: File not found on master.");
            return;
        }
     
        List<String> partitions = filePartitionMap.get(fileName);
        Map<Integer, String> reassembledFile = new TreeMap<>();
    
        for (String partitionInfo : partitions) {
            String[] details = partitionInfo.split(",");
            int partitionIndex = Integer.parseInt(details[0].trim());
            String slaveIp = details[1].trim();
            int slavePort = Integer.parseInt(details[2].trim());
    
            String partitionData = requestPartitionFromSlave(slaveIp, slavePort, fileName, partitionIndex);
            if (partitionData != null) {
                reassembledFile.put(partitionIndex, partitionData);
            }
        }
        
        // Reassemble the file content
        StringBuilder fileContent = new StringBuilder();
        int partition = getPartition();
        for (int i = 0; i < partition; i++) {
            if(reassembledFile.get(i) == null) {
                clientOut.writeUTF("Failed to reassemble file: Unreachable slave");
                return;
            }
            fileContent.append(reassembledFile.get(i)).append("\n");
        }
    
        clientOut.writeUTF("SUCCESS");
        clientOut.writeUTF(fileContent.toString());
        clientOut.writeUTF("EOF");
    }
    
    String requestPartitionFromSlave(String slaveIp, int slavePort, String fileName, int partitionIndex) {
        try (Socket slaveSocket = new Socket(slaveIp, slavePort);
             DataOutputStream out = new DataOutputStream(slaveSocket.getOutputStream());
             DataInputStream in = new DataInputStream(slaveSocket.getInputStream())) {
    
            out.writeUTF("get_partition " + fileName + " " + partitionIndex);
    
            String response = in.readUTF();
            if (response.startsWith("PARTITION_DATA")) {
                return response.substring(14); 
            }
        } catch (IOException e) {
            System.out.println("Unreachable slave: " + slaveIp + ":" + slavePort + ". Failed to load partition");
        }
        return null;
    }

    public void handleClientRequests() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Master listening for client connections on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void handleClient(Socket clientSocket) {
        try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {

            String command = in.readUTF();
            if (command.startsWith("put ")) {
                String filePath = command.substring(7);
                System.out.println("Received upload request for file: " + filePath);
                partitionAndDistributeFile(filePath, in);
            } else if (command.equals("ls")) {
                sendFilePartitionList(out);
            } else if (command.equals("connect")) {
                System.out.println("Client registered.");
            } else if (command.startsWith("get ")) {
                String[] parts = command.split(" ", 3);
                String fileName = parts[1];
                handleGetRequest(fileName, out);
            } else if (command.startsWith("rm ")) {
                String[] parts = command.split(" ", 3);
                String fileName = parts[1];
                handleRmRequest(fileName, out);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void sendFilePartitionList(DataOutputStream out) throws IOException {
        if (filePartitionMap.isEmpty()) {
            out.writeUTF("No files uploaded.");
            out.writeUTF("END_OF_RESPONSE");
            return;
        }
    
        for (Map.Entry<String, List<String>> entry : filePartitionMap.entrySet()) {
            String fileName = entry.getKey();
            List<String> partitions = entry.getValue();
    
            StringBuilder partitionInfo = new StringBuilder();
            partitionInfo.append("- " + fileName).append(": ").append(getPartition()).append(" partitions\n");
            
            for (String partition : partitions) {
                String[] parts = partition.split(",");
                partitionInfo.append("Partition ").append(parts[0] + "-> " + parts[1] + ":" + parts[2]).append("\n");
            }
    
            out.writeUTF(partitionInfo.toString());
        }
        out.writeUTF("END_OF_RESPONSE");
    }

    void sendPartitionToSlave(String slaveIp, int slavePort, String line, String filePath, int index) {
        try (Socket slaveSocket = new Socket(slaveIp, slavePort);
                DataOutputStream out = new DataOutputStream(slaveSocket.getOutputStream());
                DataInputStream in = new DataInputStream(slaveSocket.getInputStream())) {
            if (slaves.isEmpty()) {
                System.out.println("No active slaves to distribute the file.");
                return;
            }

            String replicationList = "";

            for(int i = 0; i < slaves.size(); i++) {
                String[] slaveDetails = slaves.get(i).split(":");
                String slaveip = slaveDetails[0];
                int slaveport = Integer.parseInt(slaveDetails[1]);
                if(!slaveip.equals(slaveIp) || slaveport != slavePort) {
                    replicationList += slaveip + ":" + slaveport; 
                    if(i < slaves.size() && slaves.size() > 2)
                        replicationList += ",";
                }
            }
            if(replicationList.isEmpty()) replicationList = ",";
            
            out.writeUTF("partition " + filePath + " " + index + " " + replicationList + " "+ line);
            String slaveResponse = in.readUTF();
            if(!filePartitionMap.containsKey(filePath)) {
                filePartitionMap.put(filePath, new ArrayList<>());
            }
            filePartitionMap.get(filePath).add(index + "," +slaveIp + "," + slavePort);
            savePersistence();
            System.out.println(slaveResponse);
           
            if(!replicationList.equals(",")) {
                slaveResponse = in.readUTF();
                if (slaveResponse.startsWith("REPLICATED_ACK"))  {
                    String[] response = slaveResponse.split(":");
                    slaveIp = response[1];
                    slavePort = Integer.parseInt(response[2]);
                    index = Integer.parseInt(response[4]);

                    if(!filePartitionMap.containsKey(filePath)) {
                        filePartitionMap.put(filePath, new ArrayList<>());
                    }
                    filePartitionMap.get(filePath).add(index + "," +slaveIp + "," + slavePort);
                    savePersistence();
                    System.out.println(slaveResponse);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void loadPersistence() {
        try (BufferedReader reader = new BufferedReader(new FileReader("../register/master_data.dat"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                String fileName = parts[0].trim();
                String partitionData = parts[1].trim();

                List<String> partitions = new ArrayList<>();
                partitionData = partitionData.substring(1, partitionData.length() - 1);
                String[] partitionEntries = partitionData.split("], \\[");
                for (String entry : partitionEntries) {
                    partitions.add(entry.replaceAll("\\[", "").replaceAll("]", ""));
                }

                filePartitionMap.put(fileName, partitions);
            }
        } catch (FileNotFoundException e) {
            System.out.println("No persistence file found. Starting fresh.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void savePersistence() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("../register/master_data.dat"))) {
            for (String key : filePartitionMap.keySet()) {
                List<String> entry = filePartitionMap.get(key);
                writer.write(key + "={");
                int length = entry.size();
                for(int i = 0; i < length; i++) {
                    writer.write("[" + entry.get(i) + "]");
                    if(i < length - 1) {
                        writer.write(", ");
                    }
                }
                writer.write("}");
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void partitionAndDistributeFile(String filePath, DataInputStream in) throws IOException {
        String fileName = new File(filePath).getName(); // Extract filename
        List<String> lines = new ArrayList<>();
        String line;

        while (!(line = in.readUTF()).equals("EOF")) {
            lines.add(line);
        }

        if (slaves.isEmpty()) {
            System.out.println("No active slaves to distribute the file.");
            return;
        }

        int totalLines = lines.size();
        int totalSlaves = slaves.size();
        int linesPerPartition = totalLines / totalSlaves;
        int remainingLines = totalLines % totalSlaves;

        List<String> partitions = new ArrayList<>();
        int currentLineIndex = 0;

        for (int i = 0; i < totalSlaves; i++) {
            int partitionSize = linesPerPartition + (i < remainingLines ? 1 : 0);
            List<String> partitionLines = lines.subList(currentLineIndex, currentLineIndex + partitionSize);
            currentLineIndex += partitionSize;

            String slave = slaves.get(i);
            String[] slaveDetails = slave.split(":");
            String slaveIp = slaveDetails[0];
            int slavePort = Integer.parseInt(slaveDetails[1]);

            sendPartitionToSlave(slaveIp, slavePort, String.join("\n", partitionLines), fileName, i);

            partitions.add("[" + i + "," + slaveIp + "," + slavePort + "]");
        }
    }

    private void handleRmRequest(String fileName, DataOutputStream clientOut) throws IOException {
        if (!filePartitionMap.containsKey(fileName)) {
            clientOut.writeUTF("ERROR: File not found.");
            return;
        }
    
        List<String> partitions = filePartitionMap.get(fileName);
    
        for (String partitionInfo : partitions) {
            String[] details = partitionInfo.split(",");
            String slaveIp = details[1].trim();
            int slavePort = Integer.parseInt(details[2].replaceAll("\\[|\\]", "").trim());
    
            sendDeleteCommandToSlave(slaveIp, slavePort, fileName);
        }
    
        filePartitionMap.remove(fileName);
        savePersistence();
    
        clientOut.writeUTF("SUCCESS: File " + fileName + " removed.");
    }
    
    private void sendDeleteCommandToSlave(String slaveIp, int slavePort, String fileName) {
        DataOutputStream out = null;
        try {
            Socket slaveSocket = new Socket(slaveIp, slavePort);
            out = new DataOutputStream(slaveSocket.getOutputStream());
            DataInputStream in = new DataInputStream(slaveSocket.getInputStream());
    
            out.writeUTF("delete_partition " + fileName);
            String response = in.readUTF();
            if (response.equals("ACK")) {
                System.out.println("Slave at " + slaveIp + ":" + slavePort + " deleted file: " + fileName);
            } else {
                System.out.println("Error deleting file on slave at " + slaveIp + ":" + slavePort);
            }
    
        } catch (IOException e) {
            try {
                out.writeUTF("Unreachable slave: " + slaveIp + ":" + slavePort + ". Failed to remove partition"); 
            } catch(IOException e1) {}
        }
    }

    public static void main(String[] args) {
        Master master = new Master("../conf/master.conf");
        new Thread(master::listenForRegisterMessages).start();
        master.handleClientRequests();
    }
}
