package auth.api.user.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum UserStatus {

    ACTIVE("활성"),
    BLOCKED("차단"),
    INACTIVE("비활성");

    private final String desc;
}