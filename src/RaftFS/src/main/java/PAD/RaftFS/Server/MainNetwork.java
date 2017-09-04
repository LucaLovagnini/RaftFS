package PAD.RaftFS.Server;

import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.LogEntry;
import PAD.RaftFS.Utility.ServerSetupException;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Created by luca on 19/03/15.
 */
public class MainNetwork {
    public static void main (String[] args) {
        try {
            String  log4jProperties = "log4j.properties",
                    servers = "servers.yaml";
            if(args.length == 0)
                System.out.println("Using default arguments (see documentation)");
            else {
                if (args.length > 0)
                    log4jProperties = args[0];
                if (args.length > 1)
                    servers = args[1];
            }
            System.setProperty("log4j.configuration","file:"+log4jProperties);
            final Logger logger = Logger.getLogger(MainNetwork.class.getName());
            logger.info("Welcome aboard!");
            //---------------------------------------READING YAML CONF FILE---------------------------------------------
            Yaml yaml = new Yaml();
            Map<String, Map<String, Object>> values = (Map<String, Map<String, Object>>) yaml
                    .load(new FileInputStream(new File(servers)));
            System.out.println("values print:");
            System.out.println(values);
            String myName = InetAddress.getLocalHost().getHostName();
            if(!values.containsKey(myName)) {
                System.err.println("Error: "+myName+" doesn't belong to the cluster!");
                return;
            }
            //-----------------------------------SETTING (OPTIONAL) RAFT PARAMETERS-------------------------------------
            final int rmiTimeout;
            int replicationFactor = 2,
                lowerBoundElectionTimeout = 500,
                randomElectionTimeout = 1000;
            ServerRMI.DEBUG_LVL debugLvl = ServerRMI.DEBUG_LVL.normal;
            int networkSize;
            if(values.keySet().contains("RaftArgs"))
            {
                networkSize = values.size()-1;
                Map<String,Object> params = values.get("RaftArgs");
                rmiTimeout = (int) params.get("rmiTimeout");
                replicationFactor = (int) params.get("replicationFactor");
                lowerBoundElectionTimeout = (int) params.get("lowerBoundElectionTimeout");
                randomElectionTimeout = (int) params.get("randomElectionTimeout");
                debugLvl = ServerRMI.DEBUG_LVL.valueOf((String) params.get("debugLvl"));
            }
            else {
                System.out.println("Using default Raft parameters (see documentation)");
                networkSize = values.size();
                rmiTimeout = 100;
            }
            Scanner scanner = new Scanner(System.in);
            RMISocketFactory.setSocketFactory( new RMISocketFactory()
            {
                public Socket createSocket( String host, int port )
                        throws IOException
                {
                    Socket socket = new Socket();
                    socket.setSoTimeout(rmiTimeout);
                    socket.connect(new InetSocketAddress(host, port), rmiTimeout);
                    return socket;
                }

                public ServerSocket createServerSocket( int port )
                        throws IOException
                {
                    return new ServerSocket( port );
                }
            } );
            //----------------------------------------SERVER RMI SETUP--------------------------------------------------
            Executor executor = new Executor();
            ClusterElement myElement = null;
            List<ClusterElement> cluster = new ArrayList<>();
            for(String serverName : values.keySet()) {
                if(serverName.equals("RaftArgs"))
                    continue;
                Map<String,Object> params = values.get(serverName);
                int ftpServerPort = (int) params.get("ftpPort");
                int diskSpace = (int) params.get("diskSpace");
                String serverAddress = (String) params.get("ip");
                if (serverAddress == null) {
                    System.out.print("Insert " + serverName + "'s IP address:");
                    serverAddress = scanner.nextLine();
                }
                System.out.println("Getting " + serverName + "'s registry");
                ClusterElement clusterElement = new ClusterElement(diskSpace, serverName, serverAddress, ftpServerPort);
                if (serverName.equals(myName)) {
                    System.setProperty("java.rmi.server.hostname", serverAddress);
                    myElement = clusterElement;
                }
                cluster.add(clusterElement);
            }
            Registry registry = LocateRegistry.createRegistry(1099);
            System.out.println("creating ServerRMI");
            ServerRMI serverRMI = new ServerRMI(executor, networkSize , replicationFactor , myElement , cluster, logger, debugLvl, lowerBoundElectionTimeout, randomElectionTimeout);
            executor.setInitialState(serverRMI);
            System.out.println(myName+" exporting stub...");
            ServerInterface stub =
                    (ServerInterface) UnicastRemoteObject.exportObject(serverRMI, 0);
            System.out.println(myName+" binding stub...");
            registry.rebind(myName, stub);
            System.out.println("Server "+myName+" starts!");
            executor.start();
            //--------------------------------------------SYSTEM TESTING----------------------------------------------------
            List <String> commands = new ArrayList<>();
            while (true) {
                int commandSize = 1;
                if(commands.size()==0) {
                    System.out.println("Commands chain empty. Insert command:");
                    Collections.addAll(commands, scanner.nextLine().split(" "));
                }
                switch (commands.get(0)) {
                    case "timeout":
                        executor.changeState(new Candidate(serverRMI));
                        break;
                    case "lossOutputProbability"://only for simulation purpose!
                        serverRMI.setLossOutputProbability(Float.parseFloat(commands.get(2)));
                        commandSize = 1;
                        break;
                    case "lossInputProbability"://only for simulation purpose!
                        serverRMI.setLossInputProbability(Float.parseFloat(commands.get(2)));
                        commandSize = 2;
                        break;
                    case "logPrint":
                        System.out.print("Server" + myName + " log's print:");
                        for(LogEntry log : serverRMI.getLog())
                            System.out.print(log.getTerm()+" ");
                        System.out.println("");
                        break;
                    default:
                        System.err.println("Command "+commands.get(0)+" doesn't exists");
                }
                commands.subList(0,commandSize).clear();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ServerSetupException e) {
            e.printStackTrace();
        }
        catch (NumberFormatException e){
            System.err.println("Error parsing a number");
            e.printStackTrace();
        }
    }
}
