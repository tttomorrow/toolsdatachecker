package org.opengauss.datachecker.common.exception;

/**
 * 校验服务 数据量产生过大差异，导致构建默克尔树高度不一致，无法进行校验。
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class MerkleTreeDepthException extends LargeDataDiffException {

    public MerkleTreeDepthException(String message) {
        super(message);
    }

}
