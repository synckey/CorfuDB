package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.smr.legacy.CorfuDBObject;
import org.corfudb.runtime.stream.IStream;

import java.util.UUID;

/**
 * Created by crossbach on 5/29/15.
 */
public class LPBTNode<K extends Comparable<K>, V> implements ICorfuDBObject<TreeNode> {

    transient ISMREngine<TreeNode> smr;
    UUID streamID;

    @SuppressWarnings("unchecked")
    public LPBTNode(IStream stream, Class<? extends ISMREngine> smrClass) {
        try {
            streamID = stream.getStreamID();
            smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, TreeNode.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public LPBTNode(IStream stream) {
        streamID = stream.getStreamID();
        smr = new SimpleSMREngine<TreeNode>(stream, TreeNode.class);
    }

    @Override
    public Class<?> getUnderlyingType() {
        return TreeNode.class;
    }

    @Override
    public UUID getStreamID() {
        return streamID;
    }

    @Override
    public ISMREngine<TreeNode> getUnderlyingSMREngine() {
        return smr;
    }

    @Override
    public void setUnderlyingSMREngine(ISMREngine<TreeNode> engine) {
        this.smr = engine;
    }

    /**
     * Set the stream ID
     *
     * @param streamID The stream ID to set.
     */
    @Override
    public void setStreamID(UUID streamID) {
        this.streamID = streamID;
    }


    /**
     * read the child count
     *
     * @return the number of children in the given node
     */
    public int
    readChildCount() {
        return accessorHelper((node, opts) -> node.m_nChildren);
    }

    /**
     * write the child count
     *
     * @param n
     */
    public void
    writeChildCount(int n) {
        mutatorHelper((node, opts) -> node.m_nChildren = n);
    }

    /**
     * getChild
     *
     * @param index
     * @return
     */
    protected UUID getChild(int index) {
        return accessorHelper((node, opts) -> {
            UUID result = CorfuDBObject.oidnull;
            if (index >= 0 && index < node.m_nChildren)
                result = node.m_vChildren[index];
            return result;
        });
    }

    /**
     * apply an indexed read child operation
     *
     * @param index
     * @return
     */
    public UUID
    readChild(int index) {
        return getChild(index);
    }

    /**
     * apply a write child operation
     *
     * @param n
     * @param _oid
     */
    public void
    writeChild(int n, UUID _oid) {
        mutatorHelper((node, opts) -> {
            if (n >= 0 && n < node.m_vChildren.length)
                node.m_vChildren[n] = _oid;
            return null;
        });
    }

    /**
     * toString
     *
     * @return
     */
    @Override
    public String toString() {
        return (String) accessorHelper((node, opts) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("N");
            sb.append(streamID);
            boolean first = true;
            for (int i = 0; i < node.m_nChildren; i++) {
                boolean last = i == node.m_nChildren - 1;
                if (first) {
                    sb.append("[");
                } else {
                    sb.append(", ");
                }
                sb.append("c");
                sb.append(i);
                sb.append("=");
                sb.append(getChild(i));
                if (last) sb.append("]");
                first = false;
            }
            return sb.toString();
        });
    }

}
