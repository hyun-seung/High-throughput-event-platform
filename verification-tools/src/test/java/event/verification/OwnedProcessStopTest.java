package event.verification;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OwnedProcessStopTest {
    @Test void stopsDirectChildAndLeavesReapingToOwner() throws Exception {
        Process child = new ProcessBuilder("sleep", "30").start();
        try {
            OwnedProcessStop.signal(child.pid(), ProcessHandle.current().pid(), "terminate");
            child.waitFor();
            assertFalse(child.isAlive());
        } finally {
            child.destroyForcibly();
            child.waitFor();
        }
    }

    @Test void refusesToStopProcessNotOwnedByCaller() throws Exception {
        Process child = new ProcessBuilder("sleep", "30").start();
        try {
            assertThrows(IllegalArgumentException.class, () -> OwnedProcessStop.signal(child.pid(),
                    ProcessHandle.current().pid() + 1, "terminate"));
        } finally {
            child.destroyForcibly();
            child.waitFor();
        }
    }
}
