package com.github.lisama569.resilientedgegatewayjava.gateway.controller;

import com.github.lisama569.resilientedgegatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyResponse;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * 反向代理入口：/proxy/**。
 * 请求特征全部来自请求头，便于本地复现：
 * X-Caller（调用方）、X-Session-Id（会话标识）、X-Request-Id（请求幂等键）。
 */
@RestController
public class GatewayController {

    private final GatewayService gatewayService;

    public GatewayController(GatewayService gatewayService) {
        this.gatewayService = gatewayService;
    }

    @RequestMapping(value = "/proxy/**", method = {
            RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT,
            RequestMethod.DELETE, RequestMethod.PATCH, RequestMethod.HEAD,
            RequestMethod.OPTIONS})
    public ProxyResponse proxy(
            @RequestHeader(value = "X-Caller", required = false) String caller,
            @RequestHeader(value = "X-Session-Id", required = false) String sessionId,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            @RequestHeader(value = "X-Budget-Ms", required = false) Long budgetMs,
            jakarta.servlet.http.HttpServletRequest httpRequest) {
        String path = httpRequest.getRequestURI();
        ProxyRequest request = new ProxyRequest(caller, requestId, sessionId,
                httpRequest.getMethod(), path, budgetMs);
        return gatewayService.proxy(request);
    }
}
