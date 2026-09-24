package io.astropipe;

import java.util.Objects;

public final class ArchiveTask implements Runnable {
    private final Runnable operation;

    public ArchiveTask(Runnable operation) {
        this.operation = Objects.requireNonNull(operation, "operation");
    }

    @Override
    public void run() {
        operation.run();
    }
}
