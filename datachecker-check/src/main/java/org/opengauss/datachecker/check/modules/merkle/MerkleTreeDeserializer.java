package org.opengauss.datachecker.check.modules.merkle;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import net.openhft.hashing.LongHashFunction;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.common.util.ByteUtil;

/**
 * 默克尔树反序列化
 * bucket 桶反序列化实现
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class MerkleTreeDeserializer {
    
    /**
     * 反序列化根据{@link MerkleTree#serialize()}返回的字节数组实现
     * Serialization format ：
     * header (magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
     * [(node type:byte)(signature length:int)(signature:byte)]
     *
     * @param serializerTree
     * @return
     */
    public static MerkleTree deserialize(byte[] serializerTree) {
        ByteBuffer buffer = ByteBuffer.wrap(serializerTree);

        // 字节数组头校验
        if (buffer.getInt() != MerkleTree.MAGIC_HDR) {
            throw new IllegalArgumentException("序列化字节数组没有已合法的Magic Header开头");
        }
        // 读取头信息
        int totalNodes = buffer.getInt();
        int depth = buffer.getInt();
        int leafLength = buffer.getInt();

        // 读取 root 节点
        Node root = new Node()
                .setType(buffer.get())
                .setSignature(readNextSingature(buffer));
        if (root.getType() == MerkleTree.LEAF_SIG_TYPE) {
            throw new IllegalArgumentException("首个序列化节点为叶子节点");
        }

        Queue<Node> queue = new ArrayDeque<>(totalNodes / 2 + 1);

        Node currentNode = root;
        for (int i = 1; i < totalNodes; i++) {
            Node child = new Node()
                    .setType(buffer.get())
                    .setSignature(readNextSingature(buffer));
            queue.add(child);
            // 处理节点已提升的不完整树 : (如果currentNode 和child节点的签名一致)
            if (ByteUtil.isEqual(currentNode.getSignature(), child.getSignature())) {
                currentNode.setLeft(child);
                currentNode = queue.remove();
                continue;
            }
            if (currentNode.getLeft() == null) {
                currentNode.setLeft(child);
            } else {
                currentNode.setRight(child);
                currentNode = queue.remove();
            }

        }
        return new MerkleTree(root, totalNodes, depth, leafLength);
    }


    private static byte[] readNextSingature(ByteBuffer buffer) {
        byte[] singatureBytes = new byte[buffer.getInt()];
        buffer.get(singatureBytes);
        return singatureBytes;
    }
}
