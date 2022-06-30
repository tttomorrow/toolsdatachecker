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
 * swagger2 configuration
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
                        .title("Data extraction")
                        .description("Data validation tool data extraction API")
                        .version("v1.0.0"));
    }

    /**
     * add global request header parameters.
     */
    @Bean
    public OpenApiCustomiser customerGlobalHeaderOpenApiCustomiser() {
        return openApi -> openApi.getPaths().values().stream().flatMap(pathItem -> pathItem.readOperations().stream())
                .forEach(operation -> {
                    operation.addParametersItem(new HeaderParameter().$ref("#/components/parameters/myGlobalHeader"));
                });
    }

    /**
     * general interceptor exclusion settings.all interceptors automatically add springdoc-opapi-related
     * resource exclusion information.
     * you do not need to add it to the interceptor definition of the application.
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