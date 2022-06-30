package org.opengauss.datachecker.extract.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.parameters.HeaderParameter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springdoc.core.customizers.OpenApiCustomiser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.servlet.config.annotation.InterceptorRegistration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.lang.reflect.Field;
import java.util.List;

/**
 * swagger2配置
 * http://localhost:8080/swagger-ui/index.html
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */

/**
 * 2021/8/13
 */

@Configuration
public class SpringDocConfig implements WebMvcConfigurer {
    @Bean
    public OpenAPI mallTinyOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("数据打桩服务")
                        .description("数据打桩服务 自动化执行测试表创建以及数据插入 API")
                        .version("v1.0.0"));
    }

    /**
     * 通用拦截器排除设置，所有拦截器都会自动加springdoc-opapi相关的资源排除信息，不用在应用程序自身拦截器定义的地方去添加，算是良心解耦实现。
     */
    @SuppressWarnings("unchecked")
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        try {
            Field registrationsField = FieldUtils.getField(InterceptorRegistry.class, "registrations", true);
            List<InterceptorRegistration> registrations = (List<InterceptorRegistration>) ReflectionUtils.getField(registrationsField, registry);
            if (registrations != null) {
                for (InterceptorRegistration interceptorRegistration : registrations) {
                    interceptorRegistration.excludePathPatterns("/springdoc**/**");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}