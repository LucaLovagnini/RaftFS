package PAD.RaftFS.Client;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.*;

/**
 * Created by luca on 29/03/15.
 */
public class MainClient {
    public static void main(String[] args){
        try {
            LocateRegistry.createRegistry(1099);
            System.out.println("\u001B[32m___________________________________________");
            System.out.println("___________________________________________");
            System.out.println("||                                       ||");
            System.out.println("||           WELCOME TO RAFT FS          ||");
            System.out.println("||                                       ||");
            System.out.println("___________________________________________");
            System.out.println("___________________________________________\u001B[0m");
            System.out.println();
            Yaml yaml = new Yaml();
            //the dns file contains pairs "serverName:ipAddress"
            //it's used by the client in order to contact the servers in the cluster
            System.out.println(yaml.dump(yaml.load(new FileInputStream(new File("dns.yaml")))));
            Map<String, Object> values = (Map<String, Object>) yaml
                        .load(new FileInputStream(new File("dns.yaml")));
            Scanner scanner = new Scanner(System.in);
            String myAddress;//the client's IP address
            int replicationFactor = 2;//default number of servers where each file is replicated
            //the client address can be given as program arg or given manually at runtime
            if(args.length>0){
                myAddress = args[0];
                if(args.length>1)
                    try {
                        replicationFactor = Integer.parseInt(args[0]);
                    } catch (NumberFormatException e) {
                        System.out.println("Replication factor must be a number");
                        return;
                    }
            }else {
                System.out.println("Host addr: " + InetAddress.getLocalHost().getHostAddress());  // often returns "127.0.0.1"
                System.out.println("Network interfaces:");
                Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
                HashSet<String> addresses = new HashSet<>();
                for (; n.hasMoreElements(); ) {
                    NetworkInterface e = n.nextElement();
                    System.out.println("Interface: " + e.getName());
                    Enumeration<InetAddress> a = e.getInetAddresses();
                    for (; a.hasMoreElements(); ) {
                        InetAddress addr = a.nextElement();
                        System.out.println("  " + addr.getHostAddress());
                        addresses.add(addr.getHostAddress());
                    }
                }
                System.out.println("Write your IP address for callbacks:");
                myAddress = scanner.nextLine();
                if(!addresses.contains(myAddress)){
                    System.out.println(myAddress+" isn't a valid IP");
                    return;
                }
            }
            System.setProperty("java.rmi.server.hostname", myAddress);
            //print the client menu
            printUsage();
            while (true){
                String[] commandArgs = scanner.nextLine().split(" ");
                String commandName = commandArgs[0];
                if(commandName.equals("help"))
                    printUsage();
                else if (!commandName.matches("ls|rm|mkdir|get|put|replicate"))
                    System.out.println("Command " + commandName + " doesn't exist");
                else if(!values.containsKey(commandArgs[1]))
                    System.out.println("dns.yaml doesn't contain "+commandArgs[1]);
                else {
                    String serverName = commandArgs[1];
                    String serverAddress = (String) values.get(serverName);
                    if (commandName.matches("ls|rm|mkdir") && commandArgs.length == 3 ||
                            commandName.matches("get|replicate") && commandArgs.length == 4 ||
                            commandName.equals("put") && (commandArgs.length == 5 || commandArgs.length == 4) ||//replication factor is optional
                            commandName.equals("help") && commandArgs.length == 1
                            ) {
                        try {
                            SingleClientRequest clientRequest;
                            switch (commandName) {//command keyword
                                case "ls":
                                case "rm":
                                case "mkdir":
                                    System.out.println("Creating client request");
                                    clientRequest = new SingleClientRequest(serverName, serverAddress, commandName, commandArgs[2]);
                                    break;
                                case "get":
                                    clientRequest = new SingleClientRequest(serverName, serverAddress, commandArgs[2], new File(commandArgs[3]));
                                    break;
                                case "replicate":
                                    clientRequest = new SingleClientRequest(serverName, serverAddress, commandArgs[2], Integer.parseInt(commandArgs[3]));
                                    break;
                                case "put":
                                    if(commandArgs.length>4)//replication factor is specified
                                        clientRequest = new SingleClientRequest(serverName, serverAddress, commandArgs[2], new File(commandArgs[3]), Integer.parseInt(commandArgs[4]));
                                    else//use the default value
                                        clientRequest = new SingleClientRequest(serverName, serverAddress, commandArgs[2], new File(commandArgs[3]), replicationFactor);
                                    break;
                                default:
                                    System.out.println("Command doesn't exists");
                                    continue;
                            }
                            System.out.println("Starting "+commandName+"...");
                            clientRequest.start();
                            System.out.println("Waiting "+commandName+"...");
                            clientRequest.join();
                            System.out.println(commandName+" submitted, now waiting for callback...");
                        } catch (NumberFormatException e) {
                            System.err.println("replicationFactor must be a number");
                        } catch (InterruptedException e) {
                            System.err.println("Client interrupted during "+commandName);
                        }
                    } else
                        System.out.println("Command " + commandName + " wrong format");
                }
            }
        }
        catch (RemoteException e) {
            System.err.println("Error creating registry");
        } catch (FileNotFoundException e) {
            System.err.println("File dns.yaml doesn't exist");
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println();
        System.out.println("Commands usage:");
        System.out.println("put server localFile remoteFile [replicationFactor]");
        System.out.println("mkdir server remoteDirectory");
        System.out.println("rm server remoteFile");
        System.out.println("ls server remoteDirectory");
        System.out.println("get server localFile remoteFile");
        System.out.println("replicate server remoteFile replicationFactor");
        System.out.println("help");
        System.out.println();
        System.out.println("where:");
        System.out.println("server: cluster server which will receive client's request. It will eventually redirect the client to the actual leader");
        System.out.println("localFile: a relative or absolute path of a file on the client system");
        System.out.println("\tput: file that the client wants to upload on the cloud");
        System.out.println("\tget: where the client wants to save the file downloaded");
        System.out.println("remoteFile: an absolute path of a file on the cloud");
        System.out.println("\tput: where the client want to save the file on the cloud");
        System.out.println("\tget: where the client want to save the file downloaded from the cloud");
        System.out.println("\treplicate: where the file to replicate is saved on the cloud");
        System.out.println("replicationFactor: The maximum number of server where to replicate the file");
        System.out.println("\tput: optional parameter (2 by default). on how many servers the file will be replicated EVENTUALLY (see documentation)");
        System.out.println("\treplicate: on how many other servers the file will be replicated EVENTUALLY  (see documentation)");
        System.out.println("remoteDirectory: an absolute path of a directory in the FS");
        System.out.println("\tmkdir: name of the directory which the client wants to create");
        System.out.println("\tls: name of the directory which the client want to know the contents");
        System.out.println("help: print this usage");
        System.out.println();
    }
}
