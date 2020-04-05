/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoin;

    static int buffPerJoin;

    /**
     * BufferManager Constructor
     *
     * @param numBuffer total number of buffers
     * @param numJoin   total number of Join operators
     */
    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        if (numJoin == 0) {
            buffPerJoin = numBuffer;
        } else {
            buffPerJoin = numBuffer / numJoin;
        }
    }

    /**
     * getter method
     *
     * @return number of buffers per Join operator
     */
    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

}
