package com.github.lisama569.resilientedgegatewayjava.gateway.upstream;

import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyResponse;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;

public interface UpstreamInvoker {
    ProxyResponse invoke(UpstreamNode node, ProxyRequest request);
}
