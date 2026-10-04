package messaging.verification;

/** Signal only a direct child of the calling harness; its parent must wait and reap it. */
final class OwnedProcessStop {
    private OwnedProcessStop() {}

    static void run(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: stop-owned-process pid terminate|kill");
        long owner = ProcessHandle.current().parent().orElseThrow().pid();
        signal(Long.parseLong(args[0]), owner, args[1]);
        System.out.println("{\"signalSent\":true}");
    }

    static void signal(long pid, long owner, String action) {
        if (pid <= 0 || owner <= 0 || !(action.equals("terminate") || action.equals("kill")))
            throw new IllegalArgumentException("Invalid process stop arguments");
        ProcessHandle child = ProcessHandle.of(pid).orElse(null);
        if (child == null || !child.isAlive()) return;
        if (child.parent().map(ProcessHandle::pid).orElse(-1L) != owner)
            throw new IllegalArgumentException("Process is not owned by the calling harness: " + pid);
        boolean sent = action.equals("kill") ? child.destroyForcibly() : child.destroy();
        if (!sent && child.isAlive()) throw new IllegalStateException("Could not signal process: " + pid);
    }
}
