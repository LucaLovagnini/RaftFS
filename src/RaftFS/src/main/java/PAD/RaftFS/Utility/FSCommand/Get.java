package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import PAD.RaftFS.Utility.ClusterElement;

import java.rmi.RemoteException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;


/**
 * Created by luca on 08/03/15.
 */
public class Get extends FSCommand {

    List<ClusterElement> servers;
    Long fileSize;

    public Get(ClientInterface stub , String commandPath) throws PathFormatException {
        super(false,stub,commandPath);
    }
    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        //used an immutable object for security reason (if the system will be extended)
        //if the file is stored in the file system, a pair (fileSize,serversList) is returned, null otherwise
        AbstractMap.SimpleImmutableEntry <Long, List<ClusterElement>> entry = stateMachineApplier.getEntry(commandPath);//size is not important
        if(entry!=null)
        {
            fileSize = entry.getKey();
            servers = entry.getValue();
            checkResult(true,stateMachineApplier,serverRMI,null);
        }
        else
            checkResult(false,stateMachineApplier,serverRMI,null);
    }

    @Override
    public void callAnswer() throws RemoteException {
        stub.getResult(servers,fileSize);
    }

}
