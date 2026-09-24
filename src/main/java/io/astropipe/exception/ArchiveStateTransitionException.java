package io.astropipe.exception;

import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;

public final class ArchiveStateTransitionException extends IncorrectUpdateSemanticsDataAccessException {
    private final String fileName;
    private final String transition;
    private final int affectedRows;

    public ArchiveStateTransitionException(String fileName, String transition, int affectedRows) {
        super("Could not transition " + fileName + " to " + transition
                + "; expected one active row but updated " + affectedRows);
        this.fileName = fileName;
        this.transition = transition;
        this.affectedRows = affectedRows;
    }

    public String fileName() {
        return fileName;
    }

    public String transition() {
        return transition;
    }

    public int affectedRows() {
        return affectedRows;
    }
}
