package event.event.api.requestcontrol;

import event.event.api.requestcontrol.result.RequestLimitResult;

public interface RequestLimiter {

    RequestLimitResult tryAcquire(Long userId);
}