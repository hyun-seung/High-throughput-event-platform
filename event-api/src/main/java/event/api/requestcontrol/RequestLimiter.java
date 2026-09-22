package event.api.requestcontrol;

import event.api.requestcontrol.result.RequestLimitResult;

public interface RequestLimiter {

    RequestLimitResult tryAcquire(Long userId);
}