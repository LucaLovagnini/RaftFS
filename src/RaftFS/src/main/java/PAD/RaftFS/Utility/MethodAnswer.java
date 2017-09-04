package PAD.RaftFS.Utility;

import java.io.Serializable;

/**
 * Created by luca-kun on 10/02/15.
 */
public class MethodAnswer implements Serializable{

    final int term;
    final boolean success;
    final boolean lost;
    final boolean ready;

    public MethodAnswer(int term, boolean success, boolean ready){
        this.term = term;
        this.success = success;
        this.lost=false;
        this.ready = ready;
    }

    public MethodAnswer(int term, boolean success) {
        this.term = term;
        this.success = success;
        this.lost = false;
        this.ready = true;
    }

    public MethodAnswer(int term) {
        this.term = term;
        this.success = false;
        this.lost = true;
        this.ready = true;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isLost() {
        return lost;
    }

    public boolean isReady() { return ready; }

}
