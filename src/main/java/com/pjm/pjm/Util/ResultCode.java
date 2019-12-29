package com.pjm.pjm.Util;


/**
 * 响应码枚举，参考HTTP状态码的语义
 */

public enum ResultCode {

    SUCCESS(200),//成功

    FAIL(400),//失败

    UNAUTHORIZED(401),//请求超时

    AUTHFAILED(402),//该用户没有权限登录此系统

    NOTHISUSER(403),//用户不存在

    NOT_FOUND(404),//接口不存在

    ERRSYSTEM(405),//错误的系统

    ERRPWD(406),//密码错误

    INTERNAL_SERVER_ERROR(500);//服务器内部错误


    private final int code;


    ResultCode(int code) {

        this.code = code;

    }


    public int code() {

        return code;

    }

}
