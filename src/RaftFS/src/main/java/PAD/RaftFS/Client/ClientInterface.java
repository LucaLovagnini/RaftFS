package PAD.RaftFS.Client;

import PAD.RaftFS.Utility.ClusterElement;
import PAD.RaftFS.Utility.RequestFailed;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by luca on 24/02/15.
 */
public interface ClientInterface extends Remote {
    /**
     * Used when ANY FS operation failed (during consistency  protocol or executing the operation itself)
     * @param answer the answer generated
     * @throws java.rmi.RemoteException
     */
    public void requestFailed(RequestFailed answer) throws RemoteException;

    /**
     * Used as successful result of mkdir and put (in the final phase) operations.
     * @param message contains the (successful) message result of the relative operation
     * @throws java.rmi.RemoteException
     */
    public void messageResult(String message) throws RemoteException;

    /**
     * This call back is used either by get and replicate commands. fileSize is used only by the replicate command.
     * If it's called as result of a get command, then the client will download the file from a server in server
     * (it tries to download the file until success or fails to download it from all the servers)
     * If it's a replicate command, then it submits a putGetCandidatesResult in order to obtain
     * a list of feasible servers where to replicate the file
     * @param server the list of servers where the file is stored
     * @param fileSize used since the client doesn't know the size of the file to be replicated
     * @throws java.rmi.RemoteException
     */
    public void getResult(List<ClusterElement> server, Long fileSize) throws RemoteException;

    /**
     * Call back called as list operation result
     * @param files list of files and directories contained in the directory used as parameter
     * @throws java.rmi.RemoteException
     */
    public void lsResult(List<String> files) throws RemoteException;

    /**This method is called as callback only for put and replicate commands. Before upload the file on the FS
     * (and then submit a PutCommit command), the client needs a list of feasible candidates (and the best ones w.r.t.
     * their free disk space). In this method the client actually uploads (replicate) the file on the candidates, and
     * then (eventually) commits the operation to the FS. The commits operation can be successful or not (if not
     * the Garbage Collector will delete the file uploaded on the Server)
     * @param result the list of servers where the file can be uploaded
     * @throws RemoteException
     */
    public void putGetCandidateResult(List<ClusterElement> result) throws RemoteException;

    /**
     * Since the remove operation is the inverse of the put one, after a file is logically removed from the FS
     * then it can be removed from each servers that stores it. If the order was the inverse, it could happen that
     * a get operation returns a list of servers where the file is not uploaded anymore!
     * @param servers the list of servers where the file is actually stored
     * @throws RemoteException
     */
    public void removeFileResult(List<ClusterElement> servers) throws RemoteException;

}