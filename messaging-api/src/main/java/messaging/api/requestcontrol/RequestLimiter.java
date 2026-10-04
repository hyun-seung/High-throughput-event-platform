package messaging.api.requestcontrol;

import messaging.api.requestcontrol.result.RequestLimitResult;

public interface RequestLimiter {

    RequestLimitResult tryAcquire(Long userId);
}