package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import PAD.RaftFS.Utility.ClusterElement;

import java.rmi.RemoteException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by luca on 08/03/15.
 */
public class PutCommit extends FSCommand{
    private final long fileSize;
    private int replicated;//used by callAnswer(), it's the number of committed servers
    private List<ClusterElement> servers;//list of servers where the file was already uploaded

    public PutCommit(ClientInterface stub, String commandPath, long fileSize, List<ClusterElement> servers) throws PathFormatException{
        super(true,stub,commandPath);
        this.fileSize = fileSize;
        this.servers = servers;
    }

    /**
     * This method is called by a Client when he has successfully uploaded the file on the servers obtained by
     * executing PutGetCandidates. Notice that the disk space of a server is updated in this method. This means
     * that it's possible that a Client which has already uploaded the file receives a failed answer. A scenario
     * where this is possible is when there is a set of servers with a lot of free disk space, and a lot of clients
     * uploads big files at the same time: the PutGetCandidates will answer to all these clients with the same set
     * of servers, but when they call this method (after they have already uploaded their files), they will receive
     * an error (since there is not enough "logical" space). Notice that all the files can be uploaded because
     * the logical space (denoted by the ClusterElement object) could be smaller than the real disk space.
     * This issue is solved by the GarbageCollector implemented in RaftFS (since it is already used when a client
     * goes down after uploading the files but before executing PutCommit command). Notice finally that during the upload
     * process the parent directory (which existed when PutGetCandidates was called) could have been deleted during the
     * upload process, or even that an element with the same name of the uploaded file was created.
     * As last resort, when the disk space is insufficient a round of Garbage Collector could have be ran in order to
     * (hopefully) free some disk space, but this feature isn't implemented since it could reduce the performance
     */
    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        List<ClusterElement> successfullyUploaded = new ArrayList<>();
        if(stateMachineApplier.containsFile(commandPath)&& //the file have been already fully replicated (depending on replicationFactor)
                stateMachineApplier.isFullyReplicated(commandPath))
            checkResult(false,stateMachineApplier,serverRMI,"File already fully replicated");
        else if(stateMachineApplier.containsDir(commandPath))//check if a directory already exists
            checkResult(false,stateMachineApplier,serverRMI,null);
        else {
            String fileName = commandPath.substring(commandPath.lastIndexOf("/") + 1, commandPath.length());
            String parentDirectoryPath = commandPath.lastIndexOf("/")==0 ? "/" : commandPath.substring(0,commandPath.lastIndexOf("/"));
            if(!stateMachineApplier.containsDir(parentDirectoryPath))
                checkResult(false,stateMachineApplier,serverRMI,"Error putting"+commandPath+": the parent directory doesn't exist");
            else {
                //for each server where the file was uploaded, try store its logical representation
                for(ClusterElement server : servers) {
                    List<ClusterElement> loadBalancer = serverRMI.getCluster();
                    if(!loadBalancer.contains(server))//never happen
                        System.err.println("loadBalancer doesn't contain "+server.getId());
                    else {
                        ClusterElement s = loadBalancer.get(loadBalancer.lastIndexOf(server));
                        if (s.takeSpace(fileSize)) {//if there is enough space on the disk (maybe not! see method's description)
                            serverRMI.updateCluster();//takeSpace if success updates s, so write it on file
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                                System.out.println(serverRMI.id+" taken "+fileSize+" from "+s.getId()+", free space="+s.getDiskSpace());
                            //if the parent directory doesn't contains the file already
                            if (!stateMachineApplier.dirContainsElement(parentDirectoryPath,fileName))
                                stateMachineApplier.putDirElement(parentDirectoryPath,fileName);
                            successfullyUploaded.add(server);
                        }
                        else
                            if(serverRMI.debugLvl.compareTo(ServerRMI.DEBUG_LVL.normal)>=0)
                                System.out.println(serverRMI.id+" no "+fileSize+" available on "+s.getId()+", free space="+s.getDiskSpace());
                    }
                }
                if(successfullyUploaded.isEmpty())//the file was not replicated at all
                    checkResult(true,stateMachineApplier,serverRMI,"Error putting"+commandPath+": the candidates are not feasible");
                else {
                    //if the file was already contained in the fs (which means that we are simpling replicating it
                    if(stateMachineApplier.containsFile(commandPath))
                        stateMachineApplier.addReplicationList(commandPath,successfullyUploaded);
                    else//otherwise create a new entry
                        stateMachineApplier.createFile(commandPath,new AbstractMap.SimpleImmutableEntry<>(fileSize, successfullyUploaded));
                    replicated = successfullyUploaded.size();
                    checkResult(true,stateMachineApplier,serverRMI,null);
                }
            }
        }

    }

    @Override
    public void callAnswer() throws RemoteException {
        stub.messageResult("file "+commandPath+" successfully added on "+ replicated +" Servers");
    }
}
