package event.common.tcp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** One exchange per connection: 4-byte big-endian length followed by UTF-8 JSON. */
public final class TcpFrames {
    public static final int MAX_BYTES = 1024 * 1024;
    private TcpFrames() { }

    public static byte[] read(InputStream input) throws IOException {
        int length = new DataInputStream(input).readInt();
        if (length < 1 || length > MAX_BYTES) throw new InvalidFrameException();
        byte[] body = input.readNBytes(length);
        if (body.length != length) throw new EOFException("Incomplete TCP frame");
        return body;
    }

    public static void write(OutputStream output, byte[] body) throws IOException {
        if (body.length < 1 || body.length > MAX_BYTES) throw new InvalidFrameException();
        var stream = new DataOutputStream(output);
        stream.writeInt(body.length);
        stream.write(body);
        stream.flush();
    }

    public static class InvalidFrameException extends IOException {
        public InvalidFrameException() { super("TCP frame length is outside the allowed range"); }
    }
}
