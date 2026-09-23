package io.astropipe.exception;

public final class InvalidSatelliteImageNameException extends IllegalArgumentException {
    public InvalidSatelliteImageNameException(String message) {
        super(message);
    }

    public InvalidSatelliteImageNameException(String message, Throwable cause) {
        super(message, cause);
    }
}
