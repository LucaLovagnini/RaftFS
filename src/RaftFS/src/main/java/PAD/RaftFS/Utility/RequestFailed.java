package PAD.RaftFS.Utility;

import PAD.RaftFS.Utility.FSCommand.FSCommand;

import java.io.Serializable;

/**
 * Created by luca on 23/02/15.
 */
public class RequestFailed implements Serializable {
    private String leaderAddress;//used in network version
    private String leaderId;//used in local version
    private FSCommand command;
    private String message;
    private boolean wrongLeader;

    /**
     * Used when a FS operation fails or when there is no leader at the moment or when the client entry was overwritten
     * @param message
     */
    public RequestFailed(String message) {
        this.message = message;
        this.leaderAddress = null;//not important
        this.leaderId = null;
        this.command = null;
        this.wrongLeader = false;
    }

    /**
     * Used when the contacted node is not the actual Leader
     * @param leaderId
     * @param command
     */
    public RequestFailed(String leaderId, String leaderAddress, FSCommand command) {
        this.message = "The actual leader is "+leaderId+" with address "+leaderAddress;
        this.leaderAddress = leaderAddress;
        this.leaderId = leaderId;
        this.command = command;
        this.wrongLeader = true;

    }

    public FSCommand getCommand() {
        return command;
    }

    public String getMessage() {
        return message;
    }

    public boolean isWrongLeader() {
        return wrongLeader;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }

    public String getLeaderId() {
        return leaderId;
    }
}
