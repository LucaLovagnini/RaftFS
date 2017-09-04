package PAD.RaftFS.Utility;

import PAD.RaftFS.Server.ServerInterface;

import java.io.Serializable;

/**
 * Created by luca on 16/03/15.
 */
public class ClusterElement implements Comparable<ClusterElement> , Serializable {
    private final String serverName;//an unique identifier. In the network version it's the hostname
    private final String serverAddress;//the server IP address
    private long diskSpace;//the logical space expressed in byte
    private final int ftpServerPort;//the port where the Apache Mina FTPServer will listen to

    public ClusterElement(long diskSpace, String serverName, String serverAddress, int ftpServerPort) {
        this.diskSpace = diskSpace;
        this.ftpServerPort = ftpServerPort;
        this.serverName = serverName;
        this.serverAddress = serverAddress;
    }

    /**
     * This constructor is used by the garbage collector. It checks that the file that it is scanning (during the
     * foreach operation) is contained in the FS (and if it's not, then delete it). In order to do that, it checks
     * the list associated to the file name in StateMachineApplier.fileLocation contains the ClusterElement relative
     * to the server which it is working for (and in order to do so, the only thing needed is the serverName).
     * This procedure exploit the overridden equals() method
     * @param serverName
     */
    public ClusterElement(String serverName){
        this.serverName = serverName;
        ftpServerPort = -1;
        serverAddress = null;
    }

    /**
     * Since each cluster element is uniquely identified by its name,
     * two servers are considered to be equal iff they have the same name.
     * @param other the server to compare
     * @return true if they have the same name, false otherwise
     */
    @Override
    public boolean equals(Object other) {
        if(other == null)
            return false;
        else {
            try {
                ClusterElement otherClusterElement = (ClusterElement) other;
                return this.serverName.equals(otherClusterElement.getId());
            } catch (ClassCastException e) {
                return false;
            }
        }
    }

    @Override
    public int hashCode() {
        return 31* serverName.hashCode();
    }


    /**
     * Used in order to implement a load balancer, in particular to take the top-k most empty servers.
     * A cluster element is considered "bigger" than another one if it has more free disk space
     * (used to define possibleCandidates in PutGetCandidates.executeCommand())
     * @param clusterElement the cluster element to compare
     * @return
     */
    @Override
    public int compareTo(ClusterElement clusterElement) {
        return (int) (diskSpace-clusterElement.diskSpace);
    }

    public long getDiskSpace() {
        return diskSpace;
    }

    public String getId() {
        return serverName;
    }

    public boolean takeSpace(long fileSize) {
        if(diskSpace-fileSize>=0) {
            diskSpace -= fileSize;
            return true;
        }
        else
            return false;
    }

    public void freeDiskSpace(Long spaceToFree) {
        diskSpace += spaceToFree;
    }

    public String getAddress() {
        return serverAddress;
    }

    public int getPort() {
        return ftpServerPort;
    }
}
