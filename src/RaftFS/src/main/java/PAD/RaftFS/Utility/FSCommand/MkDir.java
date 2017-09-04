package PAD.RaftFS.Utility.FSCommand;

import PAD.RaftFS.Client.ClientInterface;
import PAD.RaftFS.Server.ServerRMI;
import PAD.RaftFS.Server.StateMachineApplier;
import com.google.common.collect.ImmutableList;

import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * Created by luca on 08/03/15.
 */
public class MkDir extends FSCommand {

    public MkDir(ClientInterface stub, String commandPath) throws PathFormatException {
        super(true,stub,commandPath);
    }

    @Override
    public void executeCommand(StateMachineApplier stateMachineApplier, ServerRMI serverRMI) {
        if(stateMachineApplier.containsFile(commandPath)||
            stateMachineApplier.containsDir(commandPath))//check if the element already exists
            checkResult(false,stateMachineApplier,serverRMI,null);
        else {
            String fileName = commandPath.substring(commandPath.lastIndexOf("/")+1 , commandPath.length());
            String parentDirectoryPath = commandPath.lastIndexOf("/")==0 ? "/" : commandPath.substring(0,commandPath.lastIndexOf("/"));
            if(!stateMachineApplier.containsDir(parentDirectoryPath))
                checkResult(false,stateMachineApplier,serverRMI,"Error making directory "+commandPath+": the parent directory doesn't exists");
            else if(stateMachineApplier.dirContainsElement(parentDirectoryPath,fileName))
                checkResult(false,stateMachineApplier,serverRMI,"Error making directory "+commandPath+": the element already appear in parent directory");
            else {
                stateMachineApplier.createDir(commandPath);//create a new entry in fsStructure
                stateMachineApplier.putDirElement(parentDirectoryPath,fileName);//add fileName to parent's entry value in fsStructure
                checkResult(true, stateMachineApplier, serverRMI, null);//operation succeeded
            }
        }
    }

    @Override
    public void callAnswer() throws RemoteException {
        stub.messageResult("directory "+commandPath+" successfully created");
    }
}
