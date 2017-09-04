package PAD.RaftFS.Client;

import PAD.RaftFS.Server.ServerInterface;
import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.RequestFailed;
import PAD.RaftFS.Utility.FSCommand.*;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.*;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by luca on 24/02/15.
 */
public class SingleClientRequest extends Thread implements ClientInterface {

    static int identifier = 0;//used in order to give an unique name to the client's stub
    //fields used by simple or composed commands (like the put or replicate ones)
    ClientInterface stub;//the stub used by the leader to call back the client
    ServerInterface serverInterface;//the stub used by the client to contact a (not necessarily leader) node
    Registry registry;
    String commandName , commandPath;
    long fileSize;//during the putGetCandidates the client gives the fileSize, then it's used again with putCommit
    int replicationFactor;//used only for put and replicate operations
    private boolean replicateCommand;//if false, then it's a get command OR put command
    private List<ClusterElement> startingServers;//list of servers where the file is stored (used only by replicate command)
    private File localFile;//reference of the file that the client wants to upload

    /**
     * Constructor used by replicate command.
     * @param serverName server to contact
     * @param commandPath absolute path of the file to replicate
     * @param replicationFactor number of servers where the file has to be replicated with this command
     */
    public SingleClientRequest(String serverName, String serverAddress , String commandPath , int replicationFactor) {
        this(serverName, serverAddress,"replicate",commandPath);
        this.replicationFactor = replicationFactor;
    }

    /**
     * Constructor used by put command
     * @param serverName server to contact
     * @param commandPath absolute path where we want to save the file in the FS
     * @param localFile file to be upload
     * @param replicationFactor upper bound of number of server where the file will be uploaded
     */
    public SingleClientRequest(String serverName, String serverAddress, String commandPath, File localFile, int replicationFactor) {
        this(serverName,serverAddress,"put",commandPath);
        this.replicationFactor = replicationFactor;
        this.localFile = localFile;
        this.fileSize = localFile.length();
    }

    /**
     * Constructor used by get command.
     * @param serverName server to contact
     * @param commandPath absolute path where the file is stored in the FS
     * @param localFile file to download (doesn't still exist)
     */
    public SingleClientRequest(String serverName, String serverAddress, String commandPath, File localFile) {
        this(serverName,serverAddress,"get",commandPath);
        this.localFile = localFile;
        this.fileSize = localFile.length();
    }

    /**
     * Constructor used by any other kind of command
     * @param serverName the name of the server to contact (if not the leader, then redirected)
     * @param commandName the command to submit
     * @param commandPath the absolute path used by the command
     */
    public SingleClientRequest(String serverName, String serverAddress, String commandName, String commandPath)
    {
        try {
            this.registry = LocateRegistry.getRegistry();
            ClientInterface clientRMI = this;
            stub = (ClientInterface) UnicastRemoteObject.exportObject(clientRMI, 0);
            registry.bind("Client"+(identifier++),stub);
            Registry serverRegistry = LocateRegistry.getRegistry(serverAddress);
            serverInterface = (ServerInterface) serverRegistry.lookup(serverName);
            this.commandPath = commandPath;
            this.commandName = commandName;
        } catch (Exception e) {e.printStackTrace();}
    }

    public void run()
    {
        try {
            FSCommand command;
            switch (commandName){
                case "put":
                    replicateCommand = false;
                    command = new PutGetCandidates(stub , commandPath , fileSize , replicationFactor, replicateCommand);
                    break;
                case "replicate":
                    replicateCommand = true;
                    command = new Get(stub , commandPath);
                    break;
                case "rm":
                    command = new Remove(stub , commandPath);
                    break;
                case "mkdir":
                    command = new MkDir(stub, commandPath);
                    break;
                case "get":
                    command = new Get(stub,commandPath);
                    break;
                case "ls":
                    command = new Ls(stub,commandPath);
                    break;
                default: {
                    System.err.println("Command doesn't exists");
                    return;
                }
            }
            //TODO: introduce some kind of reliable timeout
            serverInterface.clientRequest(command);
        } catch (PathFormatException e){
            System.err.println(e.getMessage());
        }
        catch (java.rmi.ServerException |UnmarshalException e){
            //ignore timeout messages
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void requestFailed(RequestFailed answer) throws RemoteException {
        try {
            if(answer.isWrongLeader()) {//the contacted server is not the leader: the client is redirected to it
                ServerInterface serverInterface;
                System.out.println("Leader is "+answer.getLeaderId()+" trying to get registry..."+answer.getLeaderAddress());
                Registry serverRegistry = LocateRegistry.getRegistry(answer.getLeaderAddress());
                System.out.println("looking up for "+answer.getLeaderId());
                serverInterface = (ServerInterface) serverRegistry.lookup(answer.getLeaderId());
                System.out.println("Submitting command");
                serverInterface.clientRequest(answer.getCommand());
            }
            else
                System.err.println(answer.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageResult(String message) throws RemoteException {
        System.out.println("\u001B[32m" + message + "\u001B[0m");
    }


    @Override
    public void getResult(List<ClusterElement> servers, Long fileSize) throws RemoteException {
        System.out.print("\u001B[32m"+"Get result: ");
        for(ClusterElement server : servers)
            System.out.print(server.getId()+" ");
        System.out.println("\u001B[0m");
        if(replicateCommand){//now that the client knows where the file is stored, it needs a list of servers where to replicate it
            try {
                this.startingServers = servers;//these are the servers where the file to replicate is stored
                this.fileSize = fileSize;//it will be used with putCommit
                serverInterface.clientRequest(new PutGetCandidates(stub , commandPath , fileSize , replicationFactor, replicateCommand));
            } catch (PathFormatException e) {//never thrown
                System.err.println(e.getMessage());
            }
        }
        else {//it's a normal get: download the file
            //try sequentially to download the file from each server that stores it
            //TODO: download the file from the most "idle" machine
            for(ClusterElement clusterElement : servers){
                try (FileOutputStream file = new FileOutputStream(localFile)) {
                    try {
                        FTPClient ftpClient = createClient(clusterElement.getAddress(),clusterElement.getPort());
                        String fileName = commandPath.replace('/', '@');
                        if (ftpClient.retrieveFile(fileName, file)) {
                            System.out.println("\u001B[32m" + localFile + " downloaded successfully" + "\u001B[0m");
                            return;
                        } else
                            System.err.println("Error downloading " + localFile.getName() + " from "+clusterElement.getId()+", trying with the next one");
                    } catch (IOException e) {
                        System.err.println("Error while downloading "+localFile+" from "+clusterElement.getId());
                    }
                } catch (IOException e) {
                    System.err.println("Error creating "+localFile.getName()+" output stream");
                }
            }
        }
    }

    @Override
    public void lsResult(List<String> files) throws RemoteException {
        System.out.print("\u001B[32m"+"List result: ");
        for(String file : files)
            System.out.print(file+" ");
        System.out.println("\u001B[0m");
    }

    @Override
    public void putGetCandidateResult(List<ClusterElement> result) throws RemoteException {
        List<ClusterElement> servers = new ArrayList<>();//list of servers where the file is successfully put/replicated
        String fileName = commandPath.replace('/', '@');//so all the files will be uploaded on the same FS level
        try {
            if(!replicateCommand)//put command
            {
                //uploads the file in parallel with a timeout (1 minute):
                //if the timeout elapses removes each file not totally uploaded
                List<UploadTask> uploadTasks = new ArrayList<>();
                ExecutorService taskExecutor = Executors.newFixedThreadPool(result.size());
                for(ClusterElement clusterElement : result)
                    uploadTasks.add(new UploadTask(localFile, fileName, clusterElement));
                //TODO: bigger files to upload
                //TODO: create a pipeline process like hadoop
                List<Future<ClusterElement>> uploadResult = taskExecutor.invokeAll(uploadTasks, 5, TimeUnit.MINUTES);
                System.out.println("Timeout or all upload finished!");
                for(int i=0;i<uploadResult.size();i++)
                {
                    Future<ClusterElement> future = uploadResult.get(i);
                    ClusterElement clusterElement = result.get(i);
                    try {
                        if(future.isCancelled()) {//if upload not finished, then future cancelled (interrupted)
                            System.out.println("File not totally uploaded on "+clusterElement.getId()+": removing it");
                            removeFile(clusterElement.getAddress(),clusterElement.getPort(),fileName+".tmp",clusterElement);
                        }
                        else {
                            //upload finished (in this case get isn't a blocking operation for sure)
                            if(future.get()!=null)//successfully uploaded
                            {
                                System.out.println("Successfully uploaded on "+clusterElement.getId());
                                servers.add(clusterElement);
                            }
                            else//upload failed
                                System.out.println("Uploading failed on "+clusterElement.getId());
                        }
                    }catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
            else {//replicate command
                //TODO: replicate in parallel like the upload operation, introduce a timeout for replication
                FTPClient startingServer, //one of the servers where the file is stored
                        destinationServer;//one of the candidates where the file can be replicated
                for(ClusterElement startingElement : startingServers){
                    try {
                        startingServer = createClient(startingElement.getAddress(),startingElement.getPort());
                        for(ClusterElement destinationElement : result){
                            try {
                                //if the file was already replicated on this server, skip it
                                //always false since putGetCandidates returns servers where the file is not replicated yet
                                if(servers.contains(destinationElement))
                                    continue;
                                System.out.println("\u001B[32m trying to replicate " + commandPath + " from " + startingElement.getId() + " to " + destinationElement.getId()+"\u001B[0m");
                                destinationServer = createClient(destinationElement.getAddress(),destinationElement.getPort());
                                destinationServer.enterRemotePassiveMode();
                                startingServer.enterRemoteActiveMode(InetAddress.getByName(destinationServer.getPassiveHost()),
                                        destinationServer.getPassivePort());

                                if (startingServer.remoteRetrieve(fileName) &&
                                    destinationServer.remoteStoreUnique(fileName+".tmp")//so the Garbage Collector will not delete the file during the upload
                                    )//if the upload it's successful, rename it
                                {
                                    System.out.println("Entering in passive mode");
                                    destinationServer.enterLocalPassiveMode();
                                    FTPClient renameClient = createClient(destinationElement.getAddress(),destinationElement.getPort());
                                    renameClient.enterLocalPassiveMode();
                                    System.out.println("Renamed successfully");
                                    if(renameClient.rename(fileName+".tmp", fileName)) {
                                        System.out.println("Renamed successfully");
                                        System.out.println("\u001B[32m" + commandPath + " replicated successfully from " + startingElement.getId() + " to " + destinationElement.getId() + "\u001B[0m");
                                        servers.add(destinationElement);
                                    }
                                    else
                                        System.out.println("Renaming failed");
                                    startingServer.completePendingCommand();
                                    destinationServer.completePendingCommand();
                                }
                                else
                                    System.err.println("Error replicating "+commandPath+" from " + startingElement.getId() + " to " + destinationElement.getId());
                            } catch (IOException e) {
                                System.out.println("Error connecting to "+destinationElement.getId());
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Error connecting to "+startingElement.getId());
                    }
                }


            }
            try {
                if(serverInterface == null)
                    System.err.println("Something bad happened to serverInterface");
                else if(servers.isEmpty())
                    System.err.println("Uploading failed on all candidates");
                else//if the file was put/replicated on at least one candidate, the commit the operation (possible failure)
                    serverInterface.clientRequest(new PutCommit(stub , commandPath , fileSize , servers));
            } catch (PathFormatException e) {
                System.err.println(e.getMessage());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeFileResult(List<ClusterElement> servers) throws RemoteException {
        System.out.println("\u001B[32m file "+commandPath+" successfully removed logically, now removing from servers...\u001B[0m");
        String fileName = commandPath.replace('/', '@');
        //remove sequentially the file from each server where it's stored
        //TODO: remove in parallel like the upload operation
        for(ClusterElement clusterElement : servers)
            removeFile(clusterElement.getAddress(), clusterElement.getPort(), fileName,clusterElement);
    }

    /**
     * Method used both by put and rm commands. It removes the file from the given server
     * @param address the IP address of the server where the file is stored
     * @param port the FTP port of the given server
     * @param fileName the file to remove
     * @param clusterElement used only for print
     */
    public void removeFile(String address, int port, String fileName, ClusterElement clusterElement){
        try {
            FTPClient ftpClient = createClient(address,port);
            ftpClient.enterLocalPassiveMode();
            if(ftpClient.deleteFile(fileName))
                System.out.println("\u001B[32m"+fileName+" removed successfully on "+clusterElement.getId()+"\u001B[0m");
            else
                System.err.println(fileName+" not removed on "+clusterElement.getId());
        } catch (IOException e) {
            System.err.println("Error while removing "+fileName+" on "+clusterElement.getId());
        }
    }

    /**
     * This method initialize a client connection with a given server (connection, login, file type settings)
     */
    public static FTPClient createClient(String address,int port) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.setConnectTimeout(5000);//time within the client tries to connect to a cluster element
        ftpClient.connect(address, port);
        ftpClient.login("anonymous","");
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        return ftpClient;
    }

}
