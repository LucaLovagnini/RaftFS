package PAD.RaftFS.Server;

import PAD.RaftFS.Client.SingleClientRequest;
import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.LogEntry;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class MainLocal {


    public static void blockFromNodeToNode (ServerRMI serverRMI , String blockedNode)
    {
        serverRMI.blockedNodes.add(blockedNode);
    }

    private static void unblockFromNodeToNode (ServerRMI serverRMI, String blockedNode) {
        if(serverRMI.blockedNodes.indexOf(blockedNode)!=-1)
            serverRMI.blockedNodes.remove(blockedNode);
    }

    private static int networkPartition(List<String> commands , List<String> serversId , List<ServerRMI> serverRMIList, int networkSize , boolean heal) {
        List<String> partitionNodes = new ArrayList<>();
        for (int i = 1; i<commands.size();i++) {
            if (serversId.contains(commands.get(i)))
                partitionNodes.add(commands.get(i));
            else
                break;
        }
        for (int i=0 ; i < networkSize ; i++) {
            ServerRMI partitionMember = serverRMIList.get(i);
            if(!partitionNodes.contains(partitionMember.id))//if the node isn't a partition member
                continue;
            //if the node belongs to the partition, then block/unblock communication
            //with any node which isn't a partition member
            for (int j = 0; j < networkSize; j++) {
                ServerRMI partitionOutsider = serverRMIList.get(j);
                if (!partitionNodes.contains(partitionOutsider.id)) {
                    if (!heal) {
                        System.out.println("Block from " + partitionMember.id + " to " + partitionOutsider.id);
                        blockFromNodeToNode(partitionMember, partitionOutsider.id);
                        blockFromNodeToNode(partitionOutsider, partitionMember.id);
                    } else {
                        System.out.println("Unblock from " + partitionMember.id + " to " + j);
                        unblockFromNodeToNode(partitionMember, partitionOutsider.id);
                        unblockFromNodeToNode(partitionOutsider, partitionMember.id);
                    }
                }
            }
        }
        return partitionNodes.size()+1;//+1 because command keyword
    }

    public static void main(String[] args)
    {
        if(args.length<1){
            System.err.println("usage: servers [debugLvl] [replicationFactor] [diskSpace] [lowerBoundElectionTimeout] [randomElectionTimeout]");
            System.err.println("servers: number of servers in the cluster");
            System.err.println("debugLvl: output message quantity (default: normal):");
            System.err.println("\tsilent: no message");
            System.err.println("\tnormal: some message (who becomes leader, who is candidate, log's print on update...");
            System.err.println("\tdebug: some more message (remote methods print...)");
            System.err.println("\tcrazy: totally verbose (and not understandable)");
            System.err.println("replicationFactor: on how many servers each file be replicated (default: 2)");
            System.err.println("diskSpace: disk space (in byte) of each server (default: 500MB)");
            System.err.println("lowerBoundElectionTimeout: minimum time before node switch starts a new election (default 0.5 seconds)");
            System.err.println("randomElectionTimeout: random time added to lowerBoundElectionTimeout (default 1 second)");
            return;
        }
        System.setProperty("log4j.configuration","file:log4j.properties");
        final Logger logger = Logger.getLogger(MainLocal.class.getName());
        //-------------------------------------------SERVERS SETUP------------------------------------------------------
        try {
            logger.info("Welcome aboard!");
            List<String> serversId = new ArrayList<>();
            int networkSize;
            int replicationFactor = 2;
            int diskSpace = 500000000;//500MB
            int lowerBoundElectionTimeout = 500;
            int randomElectionTimeout = 1000;
            ServerRMI.DEBUG_LVL debugLvl = ServerRMI.DEBUG_LVL.normal;
            try {
                networkSize = Integer.parseInt(args[0]);
                for(int i=0;i<networkSize;i++)
                    serversId.add("Server"+i);
            }catch (NumberFormatException e) {
                System.err.println("networkSize must be a number");
                return;
            }
            if(args.length>1)
                try {
                    debugLvl = ServerRMI.DEBUG_LVL.valueOf(args[1]);
                } catch (IllegalArgumentException e) {
                    System.err.println("debugLvl value "+args[1]+" not valid");
                    return;
                }
            if(args.length>2)
                try{
                    replicationFactor = Integer.parseInt(args[2]);
                    if(replicationFactor>networkSize){
                        System.err.println("replicationFactor must be smaller than networkSize");
                    }
                }catch (NumberFormatException e){
                    System.err.println("replicationFactor must be a number");
                    return;
                }
            if(args.length>3)
                try {
                    diskSpace = Integer.parseInt(args[3]);
                } catch (NumberFormatException e) {
                    System.err.println("diskSpace must be a number");
                    return;
                }
            if (args.length>4)
                try {
                     lowerBoundElectionTimeout = Integer.parseInt(args[4]);
                } catch (NumberFormatException e) {
                    System.err.println("lowerBoundElectionTimeout must be a number");
                    return;
                }
            if (args.length>5)
                try {
                    randomElectionTimeout = Integer.parseInt(args[5]);
                } catch (NumberFormatException e) {
                    System.err.println("randomElectionTimeout must be a number");
                    return;
                }
            System.out.println("Debug: "+debugLvl);
            boolean parallelClient = true;
            final int timeoutMillis = 100;//in localHost it has no sense to set a different value
            RMISocketFactory.setSocketFactory( new RMISocketFactory()
            {
                public Socket createSocket( String host, int port )
                        throws IOException
                {
                    Socket socket = new Socket();
                    socket.setSoTimeout(timeoutMillis);
                    socket.connect(new InetSocketAddress(host, port), timeoutMillis);
                    return socket;
                }

                public ServerSocket createServerSocket( int port )
                        throws IOException
                {
                    return new ServerSocket( port );
                }
            } );
            Registry registry = LocateRegistry.createRegistry(1099);
            List<ServerRMI> serverRMIList = new ArrayList<>();
            List<Executor> executorList = new ArrayList<>();
            SingleClientRequest singleClientRequest;
            for (String serverName : serversId) {
                List<ClusterElement> cluster = new ArrayList<>();
                ClusterElement myElement = null;
                int ftpServerPort = 8080;
                //creating cluster reference
                for(String server : serversId){
                    ClusterElement clusterElement = new ClusterElement(diskSpace, server, "127.0.1.1", ftpServerPort++);
                    cluster.add(clusterElement);
                    if(serverName.equals(server))
                        myElement = clusterElement;
                }
                Executor executor = new Executor();
                executorList.add(executor);
                ServerRMI serverRMI = new ServerRMI(executor, networkSize, replicationFactor, myElement, cluster, logger,debugLvl, lowerBoundElectionTimeout, randomElectionTimeout);
                serverRMIList.add(serverRMI);
                executor.setInitialState(serverRMI);
                ServerInterface stub =
                        (ServerInterface) UnicastRemoteObject.exportObject(serverRMI, 0);
                registry.rebind(serverName, stub);
                System.out.println(serverName + " bound");
            }
            //start the entire cluster!
            for(int i=0;i<networkSize;i++)
                executorList.get(i).start();
            //--------------------------------------------SYSTEM TESTING----------------------------------------------------
            Scanner scanner = new Scanner(System.in);
            List <String> commands = new ArrayList<>();
            while (true) {
                int commandSize = 2;//used to define how many commands token delete at the end of the cycle (usually 2)
                if(commands.size()==0) {
                    System.out.println("Commands chain empty. Insert command:");
                    Collections.addAll(commands, scanner.nextLine().split(" "));
                }
                try {
                    String server = commands.get(1);
                    Executor serverExecutor= null;
                    ServerRMI serverRMI = null;
                    int serverIndex = -1;
                    if (!commands.get(0).equals("sleep") && !commands.get(0).equals("parallelClient")) {
                        if(!serversId.contains(server)) {
                            System.err.println("Server " + server + " doesn't exists");
                            break;
                        }
                        else{
                            for(Executor executor : executorList)
                                if(executor.serverRMI.id.equals(server)) {
                                    serverExecutor = executor;
                                    serverRMI = executor.serverRMI;
                                    serverIndex = executorList.indexOf(executor);
                                    break;
                                }
                            if(serverExecutor == null || serverRMI == null) {
                                System.err.println("Executor or serverRMI not found, something is wrong!");
                                break;
                            }
                        }

                    }
                    switch (commands.get(0)) {
                        case "stop":
                            try {
                                serverExecutor.interrupt();
                                registry.unbind(server);
                                UnicastRemoteObject.unexportObject((ServerInterface) serverRMI, true);
                            }
                            catch(NotBoundException e){
                                System.err.println(server+" doesn't exists anymore");
                            }
                            break;
                        case "timeout":
                            if(serverExecutor.isDown)
                                System.err.println("Server"+server+" is down");
                            else
                                serverExecutor.changeState(new Candidate(serverRMI));
                            break;
                        case "resume":
                            if (serverExecutor.isDown) {//cannot use "isInterrupted" since testing it (already done in Executor.run()) reset the flag
                                Executor executor = new Executor();
                                executor.setInitialState(serverRMI);
                                serverRMI.setExecutor(executor);
                                executorList.set(serverIndex, executor);
                                ServerInterface stub =
                                        (ServerInterface) UnicastRemoteObject.exportObject(serverRMI, 0);
                                registry.rebind(server, stub);
                                executor.start();
                            } else
                                System.err.println("Server " + server + " is not down!");
                            break;
                        case "lossOutputProbability":
                            serverRMI.setLossOutputProbability(Float.parseFloat(commands.get(2)));
                            commandSize = 3;
                            break;
                        case "lossInputProbability":
                            serverRMI.setLossInputProbability(Float.parseFloat(commands.get(2)));
                            commandSize = 3;
                            break;
                        case "blockFromNodeToNode":
                            blockFromNodeToNode(serverRMI,commands.get(2));
                            commandSize = 3;
                            break;
                        case "unblockFromNodeToNode":
                            unblockFromNodeToNode(serverRMI, commands.get(2));
                            commandSize = 3;
                            break;
                        case "createPartition":
                            commandSize = networkPartition(commands,serversId,serverRMIList,networkSize ,false);
                            break;
                        case "healPartition":
                            commandSize = networkPartition(commands,serversId,serverRMIList,networkSize,true);
                            break;
                        case "ls":
                        case "get":
                        case "rm":
                        case "mkdir":
                            if(commands.get(0).equals("get")) {
                                singleClientRequest = new SingleClientRequest(server,"127.0.0.1", commands.get(2), new File(commands.get(3)));
                                commandSize = 4;
                            }
                            else {
                                singleClientRequest = new SingleClientRequest(server,"127.0.0.1", commands.get(0), commands.get(2));
                                commandSize = 3;
                            }
                            singleClientRequest.start();
                            if(!parallelClient) {
                                System.out.println("Waiting...");
                                singleClientRequest.join();
                                System.out.println("Done!");
                            }
                            break;
                        case "replicate":
                            singleClientRequest = new SingleClientRequest(server,"127.0.0.1",commands.get(2) , Integer.parseInt(commands.get(3)));
                            singleClientRequest.start();
                            commandSize = 4;
                            break;
                        case "put"://put command include the file size
                            singleClientRequest = new SingleClientRequest(server,"127.0.0.1",commands.get(2),new File(commands.get(3)), Integer.parseInt(commands.get(4)));
                            singleClientRequest.start();
                            commandSize = 5;
                            break;
                        case "logPrint":
                            System.out.print("Server" + server + " log's print:");
                            for(LogEntry log : serverRMI.getLog())
                                System.out.print(log.getTerm()+" ");
                            System.out.println("");
                            break;
                        case "sleep":
                            Thread.sleep(Integer.parseInt(server));//here server is how long we want to sleep
                            break;
                        case "parallelClient":
                            parallelClient = (server.equals("true"));
                            System.out.println("parallelClient="+parallelClient);
                            break;
                        default:
                            System.err.println("Command "+commands.get(0)+" doesn't exists");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("The second command parameter must be a number");
                }
                commands.subList(0,commandSize).clear();
            }
        }
        catch (Exception e) {
            System.err.println("ServerRMI exception:");
            e.printStackTrace();
        }
    }


}
