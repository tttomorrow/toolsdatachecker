package org.opengauss.datachecker.common.web;

import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Tag(name = "API 接口消息返回结果封装类")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result<T> {

    @Schema(name = "code", description = "消息响应码")
    private int code;

    @Schema(name = "message", description = "消息内容")
    private String message;

    @Schema(name = "data", description = "接口返回数据")
    private T data;


    public static <T> Result<T> success() {
        return new Result<>(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getDescription(), null);
    }

    public static <T> Result<T> of(T data) {
        return new Result<>(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getDescription(), data);
    }


    public static <T> Result<T> of(T data, int code, String message) {
        return new Result<>(code, message, data);
    }

    public static <T> Result<T> fail(ResultEnum resultEnum) {
        return new Result(resultEnum.getCode(), resultEnum.getDescription(), null);
    }

    public static <T> Result<T> fail(ResultEnum resultEnum, String message) {
        return new Result(resultEnum.getCode(), resultEnum.getDescription() + " " + message, null);
    }

    public boolean isSuccess() {
        return code == ResultEnum.SUCCESS.getCode();
    }

    public static <T> Result<T> success(T data) {
        Result<T> result = new Result<>();
        result.setCode(ResultEnum.SUCCESS.getCode());
        result.setMessage(ResultEnum.SUCCESS.getDescription());
        result.setData(data);
        return result;
    }

    public static <T> Result<T> error(String message) {
        Result<T> result = new Result<>();
        result.setCode(ResultEnum.SERVER_ERROR.getCode());
        result.setMessage(message);
        return result;
    }
}
