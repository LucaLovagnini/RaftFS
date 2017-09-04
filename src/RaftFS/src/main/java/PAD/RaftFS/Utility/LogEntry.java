package PAD.RaftFS.Utility;

import PAD.RaftFS.Utility.FSCommand.FSCommand;

import java.io.Serializable;

/**
 * Created by luca-kun on 10/02/15.
 */
public class LogEntry implements Serializable {
    int term;
    FSCommand fsCommand;
    public LogEntry(int term , FSCommand fsCommand)
    {
        this.term = term;
        this.fsCommand = fsCommand;
    }
    public int getTerm(){
        return term;
    }
    public FSCommand getFsCommand() { return fsCommand; }
}
