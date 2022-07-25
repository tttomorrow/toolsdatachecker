package org.opengauss.datachecker.check.modules.merkle;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.common.util.ByteUtil;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.Adler32;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
public class MerkleTree {

    public static final int MAGIC_HDR = 0Xcdaace99;
    private static final int INT_BYTE = 4;
    public static final int LONG_BYTE = 8;
    /**
     * 默克尔树 节点类型 叶子节点
     */
    public static final byte LEAF_SIG_TYPE = 0x0;
    /**
     * 默克尔树 节点类型 内部节点
     */
    public static final byte INTERNAL_SIG_TYPE = 0x01;
    /**
     * Serialization format ：(magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
     * [(node type:byte)(signature length:int)(signature:byte)]
     */
    private static final int MAGIC_HEADER_BYTE_LENGTH = INT_BYTE;
    private static final int LEAF_SIGNATURE_BYTE_LENGTH = INT_BYTE;
    private static final int NUM_NODES_BYTE_LENGTH = INT_BYTE;
    private static final int TREE_DEPTH_BYTE_LENGTH = INT_BYTE;
    private static final int NODE_TYPE_BYTE_LENGTH = 1;

    /**
     * Adler32 进行校验
     */
    private volatile static Adler32 crc;

    static {
        crc = new Adler32();
    }

    /**
     * 叶子节点字节长度 默克尔树在序列化及反序列化时使用。
     */
    private int leafSignatureByteLength;
    /**
     * 根节点
     */
    private Node root;
    private int depth;
    private int nnodes;

    /**
     * 默克尔树构造函数
     *
     * @param bucketList 桶列表
     */
    public MerkleTree(List<Bucket> bucketList) {
        constructTree(bucketList);
    }


    /**
     * 根据反序列化结果构造默克尔树
     *
     * @param treeRoot   树根节点
     * @param totalNodes 总节点数
     * @param depth      树深度
     * @param leafLength 叶子节点长度
     */
    public MerkleTree(Node treeRoot, int totalNodes, int depth, int leafLength) {
        this.root = treeRoot;
        this.nnodes = totalNodes;
        this.depth = depth;
        this.leafSignatureByteLength = leafLength;
    }


    /**
     * 构造默克尔树
     *
     * @param bucketList 桶列表
     */
    private void constructTree(List<Bucket> bucketList) {
        if (bucketList == null || bucketList.size() < MerkleConstant.CONSTRUCT_TREE_MIN_SIZE) {
            throw new IllegalArgumentException("ERROR:Fail to construct merkle tree ! leafHashes data invalid !");
        }
        this.nnodes = bucketList.size();
        List<Node> parents = buttomLevel(bucketList);
        this.nnodes += parents.size();
        this.depth = 1;
        while (parents.size() > 1) {
            parents = constructInternalLevel(parents);
            this.depth++;
            this.nnodes += parents.size();
        }
        this.root = parents.get(0);
    }


    /**
     * 底部层级叶子节点构建
     *
     * @param bucketList 桶列表
     * @return 节点列表
     */
    private List<Node> buttomLevel(List<Bucket> bucketList) {
        List<Node> parents = new ArrayList<>(bucketList.size() / MerkleConstant.EVEN_NUMBER);
        for (int i = 0; i < bucketList.size() - 1; i = i + MerkleConstant.EVEN_NUMBER) {
            Node leaf1 = constructLeafNode(bucketList.get(i));
            Node leaf2 = constructLeafNode(bucketList.get(i + 1));

            Node parent = constructInternalNode(leaf1, leaf2);
            parents.add(parent);
        }
        if (bucketList.size() % MerkleConstant.EVEN_NUMBER == 1) {
            Node leaf1 = constructLeafNode(bucketList.get(bucketList.size() - 1));
            // 奇数个节点的情况，复制最后一个节点
            Node parent = constructInternalNode(leaf1, null);
            parents.add(parent);
        }
        // 设置叶子节点签名字节长度
        this.leafSignatureByteLength = parents.get(0).getLeft().getSignature().length;
        return parents;
    }

    /**
     * 内部节点构建
     *
     * @param children 子节点
     * @return 内部节点集合
     */
    private List<Node> constructInternalLevel(List<Node> children) {
        List<Node> parents = new ArrayList<>(children.size() / MerkleConstant.EVEN_NUMBER);
        for (int i = 0; i < children.size() - 1; i = i + MerkleConstant.EVEN_NUMBER) {
            Node parent = constructInternalNode(children.get(i), children.get(i + 1));
            parents.add(parent);
        }

        if (children.size() % MerkleConstant.EVEN_NUMBER == 1) {
            // 奇数个节点的情况，只对left节点进行计算
            Node parent = constructInternalNode(children.get(children.size() - 1), null);
            parents.add(parent);
        }
        return parents;
    }

    /**
     * 构建叶子节点
     *
     * @param bucket 桶节点
     * @return 默克尔节点
     */
    private Node constructLeafNode(Bucket bucket) {
        return new Node().setType(LEAF_SIG_TYPE)
                .setBucket(bucket)
                .setSignature(bucket.getSignature());
    }

    /**
     * 构建内部节点
     *
     * @param left  左侧节点
     * @param right 右侧节点
     * @return 默克尔节点
     */
    private Node constructInternalNode(Node left, Node right) {
        return new Node().setType(INTERNAL_SIG_TYPE)
                .setLeft(left)
                .setRight(right)
                .setSignature(internalSignature(left, right));
    }

    /**
     * 计算内部节点签名
     *
     * @param left  左侧节点
     * @param right 右侧节点
     * @return 内部节点签名
     */
    private byte[] internalSignature(Node left, Node right) {
        if (right == null) {
            return left.getSignature();
        }
        // 这里采用Deler32进行签名
        crc.reset();
        crc.update(left.signature);
        crc.update(right.signature);
        return ByteUtil.toBytes(crc.getValue());
    }

    /**
     * Serialization format ：
     * header (magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
     * [(node type:byte)(signature length:int)(signature:byte)(bucket length:int)(bucket:byte)]
     * <p>
     *  bucket 桶序列化实现
     *
     * @return 返回序列化字节流
     */
    public byte[] serialize() {
        int header = MAGIC_HEADER_BYTE_LENGTH + NUM_NODES_BYTE_LENGTH + TREE_DEPTH_BYTE_LENGTH + LEAF_SIGNATURE_BYTE_LENGTH;
        int maxSignatureByteLength = Math.max(leafSignatureByteLength, LONG_BYTE);

        int spaceOfNodes = (NODE_TYPE_BYTE_LENGTH + NUM_NODES_BYTE_LENGTH + maxSignatureByteLength) * nnodes;

        int capacity = header + spaceOfNodes;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        //header (magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
        buffer.putInt(MAGIC_HDR).putInt(nnodes).putInt(depth).putInt(leafSignatureByteLength);
        serializeNode(buffer);

        byte[] serializeTree = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(serializeTree);
        return serializeTree;
    }

    private void serializeNode(ByteBuffer buffer) {
        Queue<Node> queue = new ArrayDeque<>(nnodes / 2 + 1);
        queue.add(root);
        while (!queue.isEmpty()) {
            Node node = queue.remove();
            buffer.put(node.type).putInt(node.signature.length).put(node.signature);
            if (node.getLeft() != null) {
                queue.add(node.getLeft());
            }
            if (node.getRight() != null) {
                queue.add(node.getRight());
            }
        }
    }

    /**
     * Merkle Tree 节点
     */
    @Data
    @Accessors(chain = true)
    public static class Node {

        /**
         * LEAF_SIG_TYPE,INTERNAL_SIG_TYPE
         */
        private byte type;
        private Node left;
        private Node right;
        /**
         * 当前节点签名 signature
         */
        private byte[] signature;
        private Bucket bucket;

        @Override
        public String toString() {
            return " Node{" +
                    "type=" + type +
                    ",signature=" + Arrays.toString(signature).replace(",", "") +
                    ",left=" + left +
                    ",right=" + right +
                    '}';
        }

        public boolean signatureEqual(Node other) {
            int length = this.getSignature().length;
            int length1 = other.getSignature().length;
            if (length != length1) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (this.getSignature()[i] != other.getSignature()[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public String toString() {
        return "MerkleTree{" +
                "nnodes=" + nnodes +
                ",depth=" + depth +
                ",leafSignatureByteLength=" + leafSignatureByteLength +
                ",root=" + root +
                '}';
    }

    public String toSimpleString() {
        return "MerkleTree{" +
                "nnodes=" + nnodes +
                ",depth=" + depth +
                ",leafSignatureByteLength=" + leafSignatureByteLength +
                '}';
    }

    interface MerkleConstant {
        int CONSTRUCT_TREE_MIN_SIZE = 2;
        int EVEN_NUMBER = 2;
    }
}
