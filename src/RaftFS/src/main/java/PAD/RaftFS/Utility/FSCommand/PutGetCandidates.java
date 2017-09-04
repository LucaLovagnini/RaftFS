package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import PAD.RaftFS.Utility.ClusterElement;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by luca on 16/03/15.
 */
public class PutGetCandidates extends FSCommand {
    long size;
    int replication;
    List<ClusterElement> candidates;
    private boolean replicateCommand;

    public PutGetCandidates(ClientInterface stub, String commandPath, long size, int replication, boolean replicateCommand) throws PathFormatException {
        super(false, stub, commandPath);
        this.size = size;
        this.replication = replication;
        this.replicateCommand = replicateCommand;
        candidates = new ArrayList<>();
    }

    /**+
     * This method find a number of feasible candidates equals to the replication field. Notice that it is not necessarily
     * equals to the ServerRMI.getReplicationFactor().Notice that a candidate has to be a reachable node: this is necessary
     * since otherwise the loadBalancer would always choose that node as a possible candidate (in fact since no one can
     * upload a file on it, and so update the free space through the PutCommit command, its free space never change)
     * and a loop situation could occur.
     * @param stateMachineApplier
     * @param serverRMI
     */
    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        this.replication = Math.min(serverRMI.getReplicationFactor(), replication);
        String parentDirectoryPath = commandPath.lastIndexOf("/")==0 ? "/" : commandPath.substring(0,commandPath.lastIndexOf("/"));
        //Regardless that this command is a Put or Replicate operation, it must not exists a Directory with the same name
        if(stateMachineApplier.containsDir(commandPath))
            checkResult(false,stateMachineApplier,serverRMI,null);
        //if the  parent directory doesn't exists
        else if (!stateMachineApplier.containsDir(parentDirectoryPath)) //the parent directory doesn't exists
            checkResult(false, stateMachineApplier, serverRMI, "Error putting " + commandPath + ": the parent directory doesn't exist");
        //if it's a Put command then the file should not exist
        else if(!replicateCommand && stateMachineApplier.containsFile(commandPath))
            checkResult(false,stateMachineApplier,serverRMI,null);
        //if it's a Replicate command the file should exist
        else if(replicateCommand && !stateMachineApplier.containsFile(commandPath))
            checkResult(false,stateMachineApplier,serverRMI,"Error replicating "+commandPath+": file doesn't exists");
        else {//else everything is all right
        /*we know for sure that at least the at least the majority of the nodes must be reachable. In addiction, we
        * want the top k servers in term of free disk space (where k is defined as the minimum between
        * ServerRMI.getReplicationFactor() and replication). In conclusion, if there aren't at least k Server which
        * are reachable AND have sufficient disk space AND where the file is not already replicated between the firsts
        * networkSize/2+1+k Servers, then the file cannot be completely replicated (at the moment)*/
            List<ClusterElement> possibleCandidates = com.google.common.collect.Ordering.natural().greatestOf(serverRMI.getCluster(),
                    serverRMI.getNetworkSize() / 2 + 1 + replication);
            for (ClusterElement clusterElement : possibleCandidates) {
                if (serverRMI.getReachableNodes().contains(clusterElement.getId()) && //the node is reachable
                        clusterElement.getDiskSpace() >= size &&//the node has sufficient space
                        (!stateMachineApplier.containsFile(commandPath) ||//file not yet replicated
                        !stateMachineApplier.isFileReplicatedOnMachine(commandPath,clusterElement)//the clusterElement doesn't contain the file already (used only for replicate)
                        )) {
                    candidates.add(clusterElement);
                    if (candidates.size() >= replication)
                        break;
                }
            }
            //if there isn't a reachable node with sufficient space and where the file isn't already stored, then the command failed
            if (candidates.isEmpty())
                checkResult(false, stateMachineApplier, serverRMI, "Error putting" + commandPath + ": no feasible candidates");
            else {
                System.out.print("Chosen server for " + commandPath + ": ");
                for (ClusterElement candidate : candidates)
                    System.out.print(candidate.getId() + " ");
                System.out.println();
                checkResult(true, stateMachineApplier, serverRMI, null);
            }
        }
    }

    @Override
    public void callAnswer() throws RemoteException {
        //utGetCandidateResult is a call-back which will call the PutCommit methods
        stub.putGetCandidateResult(candidates);
    }

}
