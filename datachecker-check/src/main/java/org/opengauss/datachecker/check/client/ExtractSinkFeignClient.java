package org.opengauss.datachecker.check.client;

import org.springframework.cloud.openfeign.FeignClient;

/**
 * 创建一个内部类，声明被调用方的api接口，假如被调用方接口异常就会回调异常类进行异常声明
 * name可以声明value,datachecker-extract-sink 是服务名称直接调用该系统，名称一般采用eureka注册信息，我们未引入eurka,配置url进行调用
 * ExtractSinkFallBack 这是如果fegin调用失败需要熔断以及提示错误信息的类
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@FeignClient(name = "datachecker-extract-sink", url = "${data.check.sink-uri}")
public interface ExtractSinkFeignClient extends ExtractFeignClient {

}