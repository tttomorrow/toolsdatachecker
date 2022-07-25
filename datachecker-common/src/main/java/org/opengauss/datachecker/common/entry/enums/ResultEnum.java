package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Getter
public enum ResultEnum {

    //自定义系列 校验异常
    //校验服务异常：
    CHECKING(1000, "verification service exception："),
    //校验服务地址端口冲突
    CHECKING_ADDRESS_CONFLICT(1001, "verify service address port conflict."),
    //校验服务Meta数据异常
    CHECK_META_DATA(1002, "verification service meta data exception."),
    //校验服务-表数据差异过大，无法校验
    LARGE_DATA_DIFF(1003, "verification service - the table data difference is too large to be verified."),
    //校验服务-默克尔树高度不一致
    MERKLE_TREE_DEPTH(1004, "verification service - height of Merkel tree is inconsistent."),

    //自定义系列 抽取异常
    //抽取服务异常
    EXTRACT(2000, "extraction service exception:"),
    //创建KafkaTopic异常：
    CREATE_TOPIC(2001, "create kafka topic exception:"),
    //当前实例正在执行数据抽取服务，不能重新开启新的校验。
    PROCESS_MULTIPLE(2002, "The current instance is executing the data extraction service and cannot restart the new verification."),
    //数据抽取服务，未找到待执行抽取任务
    TASK_NOT_FOUND(2003, "data extraction service, no extraction task to be executed found."),
    //数据抽取服务，当前表对应元数据不存在
    TABLE_NOT_FOUND(2004, "data extraction service. The metadata corresponding to the current table does not exist."),
    //Debezium配置错误
    DEBEZIUM_CONFIG_ERROR(2005, "debezium configuration error"),

    //自定义系列 抽取异常
    //Feign客户端异常
    FEIGN_CLIENT(3000, "feign client exception"),
    //调度Feign客户端异常
    DISPATCH_CLIENT(3001, "scheduling feign client exception"),

    SUCCESS(200, "SUCCESS"),
    SERVER_ERROR(400, "ERROR"),
    //400系列
    //请求的数据格式不符
    BAD_REQUEST(400, "The requested data format does not match!"),
    //登录凭证过期!
    UNAUTHORIZED(401, "login certificate expired!"),
    //抱歉，你无权限访问!
    FORBIDDEN(403, "Sorry, you have no access!"),
    //请求的资源找不到!
    NOT_FOUND(404, "The requested resource cannot be found!"),
    //参数丢失
    PARAM_MISSING(405, "Parameter missing"),
    //参数类型不匹配
    PARAM_TYPE_MISMATCH(406, "Parameter type mismatch"),
    //请求方法不支持
    HTTP_REQUEST_METHOD_NOT_SUPPORTED_ERROR(407, "request method is not supported"),
    //非法参数异常
    SERVER_ERROR_PRARM(408, "illegal parameter exception"),

    //500系列
    //服务器内部错误!
    INTERNAL_SERVER_ERROR(500, "server internal error!"),
    //服务器正忙，请稍后再试!
    SERVICE_UNAVAILABLE(503, "the server is busy, please try again later!"),

    //未知异常
    UNKNOWN(7000, "Unknown exception!");
    private final int code;
    private final String description;

    ResultEnum(int code, String description) {
        this.code = code;
        this.description = description;
    }

}
