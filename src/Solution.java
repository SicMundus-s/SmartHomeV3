import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

public class Solution {
    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            return;
        }
        final SmartHubService smartHubService = new SmartHubService(args[0], Integer.parseInt(args[1], 16));
        try {
            smartHubService.setUp();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class SmartHubService {
        private static final HttpClient httpClient = HttpClient.newHttpClient();
        private static final Base64Decoder base64Decoder = new Base64Decoder();

        private final Requests requests;
        private final Cache cache;

        SmartHubService(String url, int address) {
            this.requests = new Requests(url, address);
            this.cache = new Cache(address);
        }

        public void setUp() throws IOException {
            URI uri = requests.getURI();
            HttpServer server = HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
            server.createContext("/", exchange -> {
                if (Objects.equals(exchange.getRequestMethod(), "POST")) {
                    lever(exchange);
                }
            });

            httpClient.sendAsync(requests.createWhoIsHereFromHub(), HttpResponse.BodyHandlers.ofString());

            server.start();
        }

        private void lever(HttpExchange exchange) throws IOException {
            String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Packet decodedPacket = base64Decoder.decodingPacket(request);
            cache.add(decodedPacket.payload().src(), decodedPacket.payload().cmd_body());

            String response = decodedPacket + "\n";
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(response.getBytes());
            }
        }
    }
    interface Encodable {
        byte[] encoding();

        int sumBytes();
    }

    record Packet(TByte length, Payload payload, TByte crc8) implements Encodable {
        @Override
        public byte[] encoding() {
            final byte[] payloadBytes = payload.encoding();
            final byte[] lengthBytes = new TByte(payloadBytes.length).encoding();
            final byte[] crc8Bytes = new TByte(Utils.checkCrc8(payloadBytes)).encoding();

            return Utils.concatByteArrays(lengthBytes, payloadBytes, crc8Bytes);
        }

        @Override
        public int sumBytes() {
            return length.sumBytes() + payload.sumBytes() + crc8.sumBytes();
        }
    }

    record Payload(Varuint src, Varuint dst, Varuint serial, DevType dev_type, Cmd cmd, CmdBody cmd_body)
            implements Encodable {
        @Override
        public byte[] encoding() {
            return Utils.concatByteArrays(src.encoding(), dst.encoding(), serial.encoding(), dev_type.encoding(), cmd.encoding(), cmd_body.encoding());
        }

        @Override
        public int sumBytes() {
            return src.sumBytes() + dst.sumBytes() + serial.sumBytes() + dev_type.sumBytes() + cmd.sumBytes() + cmd_body.sumBytes();
        }
    }

    interface CmdBody extends Encodable {
        CmdBody EMPTY = new CBEmpty();
    }

    record CBEmpty() implements CmdBody {
        @Override
        public byte[] encoding() {
            return new byte[0];
        }

        @Override
        public int sumBytes() {
            return 0;
        }
    }

    record CBTick(Varuint timestamp) implements CmdBody {
        @Override
        public byte[] encoding() {
            return timestamp.encoding();
        }

        @Override
        public int sumBytes() {
            return timestamp.sumBytes();
        }
    }

    record CBOnlyDevName(TString dev_name) implements CmdBody {
        @Override
        public byte[] encoding() {
            return dev_name.encoding();
        }

        @Override
        public int sumBytes() {
            return dev_name.sumBytes();
        }
    }

    record CBSensorWhere(TString dev_name, DP dev_props) implements CmdBody {
        @Override
        public byte[] encoding() {
            return Utils.concatByteArrays(dev_name.encoding(), dev_props.encoding());
        }

        @Override
        public int sumBytes() {
            return dev_name.sumBytes() + dev_props.sumBytes();
        }
    }

    record CBSwitchWhere(TString dev_name, DPNameList dev_names) implements CmdBody {
        @Override
        public byte[] encoding() {
            return Utils.concatByteArrays(dev_name.encoding(), dev_names.encoding());
        }

        @Override
        public int sumBytes() {
            return dev_name.sumBytes() + dev_names.sumBytes();
        }
    }

    record CBValue(TByte value) implements CmdBody {
        @Override
        public byte[] encoding() {
            return value.encoding();
        }

        @Override
        public int sumBytes() {
            return value.sumBytes();
        }
    }

    record CBValues(TArray<Varuint> values) implements CmdBody {
        @Override
        public byte[] encoding() {
            return values.encoding();
        }

        @Override
        public int sumBytes() {
            return values.sumBytes();
        }
    }

    record DP(TByte sensors, TArray<Trigger> triggers) implements Encodable {
        @Override
        public byte[] encoding() {
            return Utils.concatByteArrays(sensors.encoding(), triggers.encoding());
        }

        @Override
        public int sumBytes() {
            return sensors.sumBytes() + triggers.sumBytes();
        }
    }

    record Trigger(TByte op, Varuint value, TString name) implements Encodable {

        public static Trigger of(byte[] bytes) {
            int index = 0;

            final TByte op = new TByte(bytes[index++]);

            final Varuint value = new Varuint(Utils.arrCopyFrom(bytes, index));
            index += value.sumBytes();

            final TString name = new TString(Utils.arrCopyFrom(bytes, index));

            return new Trigger(op, value, name);
        }

        @Override
        public byte[] encoding() {
            return Utils.concatByteArrays(op.encoding(), value.encoding(), name.encoding());
        }

        @Override
        public int sumBytes() {
            return op.sumBytes() + value.sumBytes() + name.sumBytes();
        }
    }

    record DPNameList(TArray<TString> dev_names) implements Encodable {
        @Override
        public byte[] encoding() {
            return dev_names.encoding();
        }

        @Override
        public int sumBytes() {
            return dev_names.sumBytes();
        }
    }


    interface Type<T> extends Encodable {
        T val();
    }

    static final class TByte implements Type<Integer> {
        private final Integer value;

        TByte(byte b) {
            this.value = Byte.toUnsignedInt(b);
        }

        TByte(Integer value) {
            this.value = value;
        }

        @Override
        public Integer val() {
            return value;
        }

        @Override
        public byte[] encoding() {
            return new byte[]{value.byteValue()};
        }

        @Override
        public int sumBytes() {
            return 1;
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    static final class TString implements Type<String> {
        private final String value;
        private final int countBytes;

        TString(byte[] bytes) {
            final StringBuilder builder = new StringBuilder();
            int count = Byte.toUnsignedInt(bytes[0]);
            for (int i = 1; i <= count; i++) {
                builder.appendCodePoint(bytes[i]);
            }

            this.value = builder.toString();
            this.countBytes = count + 1;
        }

        TString(String value) {
            this.value = value;
            this.countBytes = value.length() + 1;
        }

        @Override
        public String val() {
            return value;
        }

        @Override
        public byte[] encoding() {
            final byte[] bytes = new byte[value.length() + 1];
            bytes[0] = Integer.valueOf(value.length()).byteValue();
            byte[] valBytes = value.getBytes();
            System.arraycopy(valBytes, 0, bytes, 1, valBytes.length);
            return bytes;
        }

        @Override
        public int sumBytes() {
            return countBytes;
        }

        @Override
        public String toString() {
            return value;
        }
    }


    record Requests(URI uri, long address) {
        private static final Duration TIMEOUT_DURATION = Duration.ofMillis(300);
        private static final long BROADCAST_ADDRESS_VALUE = 16383;
        private static final TString HUB_NAME_VALUE = new TString("HUB01");

        private static long serial = 0;

        public Requests(String url, int address) {
            this(URI.create(url), address);
        }

        public HttpRequest createWhoIsHereFromHub() {
            final Payload payload = new Payload(new Varuint(address),
                    new Varuint(BROADCAST_ADDRESS_VALUE),
                    new Varuint(incrementSerial()),
                    DevType.SmartHub,
                    Cmd.WHO_IS_HERE,
                    new CBOnlyDevName(HUB_NAME_VALUE));
            return createRequest(new Packet(null, payload, null));
        }

        private HttpRequest createRequest(Packet packet) {
            final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(uri)
                    .POST(HttpRequest.BodyPublishers.ofString(Base64Encoder.encodePacket(packet)))
                    .timeout(TIMEOUT_DURATION);
            return requestBuilder.build();
        }

        private long incrementSerial() {
            return ++serial;
        }

        public URI getURI() {
            return uri;
        }
    }

    record Cache(Map<Long, CmdBody> cache, long hubAddress) {
        public Cache(int hubAddress) {
            this(new HashMap<>(), hubAddress);
        }
        public void add(Varuint varuint, CmdBody body) {
            if (varuint.val() != hubAddress) {
                cache.put(varuint.val(), body);
            }
        }
    }

    static final class Varuint implements Type<Long> {
        private final Long value;
        private final int countBytes;

        Varuint(byte[] bytes) {
            long value = 0;
            int bitSize = 0;
            int read;

            int index = 0;
            do {
                read = bytes[index++];
                value += ((long) read & 0x7f) << bitSize;
                bitSize += 7;
                if (bitSize >= 64) {
                    throw new ArithmeticException("ULEB128 value surpass maximum");
                }
            } while ((read & 0x80) != 0);

            this.value = value;
            this.countBytes = index;
        }

        Varuint(Long value) {
            this.value = value;
            this.countBytes = -1;
        }

        @Override
        public Long val() {
            return value;
        }

        @Override
        public byte[] encoding() {
            long val = value;
            List<Byte> bytes = new ArrayList<>();
            do {
                byte b = (byte) (val & 0x7f);
                val >>= 7;
                if (val != 0) {
                    b |= 0x80;
                }
                bytes.add(b);
            } while (val != 0);

            byte[] ret = new byte[bytes.size()];
            for (int i = 0; i < bytes.size(); i++) {
                ret[i] = bytes.get(i);
            }
            return ret;
        }

        @Override
        public int sumBytes() {
            return countBytes;
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    static final class TArray<T extends Encodable> implements Type<T> {
        private final List<T> list;
        private final int sumBytes;

        TArray(byte[] bytes, Function<byte[], T> mapper) {
            int readIndex = 0;
            int count = new TByte(bytes[readIndex++]).val();
            final List<T> arrayList = new ArrayList<>(count);
            for (; count > 0; count--) {
                T element = mapper.apply(Utils.arrCopyFrom(bytes, readIndex));
                readIndex += element.sumBytes();
                arrayList.add(element);
            }

            this.list = arrayList;
            this.sumBytes = readIndex;
        }

        TArray(List<T> list) {
            this.list = list;
            this.sumBytes = -1;
        }

        public List<T> list() {
            return list;
        }

        @Override
        public T val() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] encoding() {
            final byte[] size = new byte[]{Integer.valueOf(list.size()).byteValue()};
            final List<byte[]> objects = list.stream().map(Encodable::encoding).toList();

            final byte[] bytes = new byte[objects.stream().mapToInt(arr -> arr.length).sum() + 1];

            int index = Utils.concatBytes(bytes, size, 0);
            for (byte[] arr : objects) {
                index = Utils.concatBytes(bytes, arr, index);
            }
            return bytes;
        }

        @Override
        public int sumBytes() {
            return sumBytes;
        }

        @Override
        public String toString() {
            return list.toString();
        }
    }

    enum Cmd implements Encodable {
        WHO_IS_HERE(),
        I_AM_HERE(),
        GET_STATUS(),
        STATUS(),
        SET_STATUS(),
        TICK();

        Cmd() {
        }

        public static Cmd of(int b) {
            return values()[b - 1];
        }

        @Override
        public byte[] encoding() {
            return new byte[]{Integer.valueOf(ordinal() + 1).byteValue()};
        }

        @Override
        public int sumBytes() {
            return 1;
        }
    }

    enum DevType implements Encodable {
        SmartHub(),
        EnvSensor(),
        Switch(),
        Lamp(),
        Socket(),
        Clock();

        DevType() {
        }

        public static DevType of(int b) {
            return values()[b - 1];
        }

        @Override
        public byte[] encoding() {
            return new byte[]{Integer.valueOf(ordinal() + 1).byteValue()};
        }

        @Override
        public int sumBytes() {
            return 1;
        }
    }

    static final class Base64Decoder {
        private static final Base64.Decoder decoder = Base64.getUrlDecoder();

        public Packet decodingPacket(String base64) {
            try {
                final byte[] bytes = decoder.decode(base64.getBytes(StandardCharsets.UTF_8));
                final TByte length = new TByte(bytes[0]);
                final Payload payload = decodingPayload(Utils.arrCopyFrom(bytes, 1));
                final TByte crc8 = new TByte(bytes[bytes.length - 1]);
                return new Packet(length, payload, crc8);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
        }

        private Payload decodingPayload(byte[] bytes) {
            int readIndex = 0;
            final Varuint source = new Varuint(Utils.arrCopyFrom(bytes, readIndex));
            readIndex += source.sumBytes();

            final Varuint destination = new Varuint(Utils.arrCopyFrom(bytes, readIndex));
            readIndex += destination.sumBytes();

            final Varuint serial = new Varuint(Utils.arrCopyFrom(bytes, readIndex));
            readIndex += serial.sumBytes();

            final DevType devType = DevType.of(new TByte(bytes[readIndex++]).val());

            final Cmd cmd = Cmd.of(new TByte(bytes[readIndex++]).val());

            final CmdBody cmdBody = decodingCmdBody(Utils.arrCopyFrom(bytes, readIndex), devType, cmd);

            return new Payload(source, destination, serial, devType, cmd, cmdBody);
        }

        private CmdBody decodingCmdBody(byte[] bytes, DevType dev_type, Cmd cmd) {
            switch (cmd) {
                case WHO_IS_HERE, I_AM_HERE:
                    if (dev_type == DevType.EnvSensor) {
                        return decodeCmdBodySensor(bytes);
                    } else if (dev_type == DevType.Switch) {
                        return decodeCmdBodySwitch(bytes);
                    } else {
                        return new CBOnlyDevName(new TString(bytes));
                    }
                case STATUS:
                    if (dev_type == DevType.EnvSensor) {
                        return new CBValues(new TArray<>(bytes, Varuint::new));
                    } else if (dev_type == DevType.Switch || dev_type == DevType.Lamp || dev_type == DevType.Socket) {
                        return new CBValue(new TByte(bytes[0]));
                    } else {
                        return CmdBody.EMPTY;
                    }
                case SET_STATUS:
                    return new CBValue(new TByte(bytes[0]));
                case TICK:
                    return new CBTick(new Varuint(bytes));
                default:
                    return CmdBody.EMPTY;
            }
        }

        private CBSensorWhere decodeCmdBodySensor(byte[] bytes) {
            int index = 0;
            TString deviceName = new TString(bytes);
            index += deviceName.sumBytes();
            TByte sensorCount = new TByte(bytes[index++]);
            TArray<Trigger> sensorTriggers = new TArray<>(Utils.arrCopyFrom(bytes, index), Trigger::of);
            return new CBSensorWhere(deviceName, new DP(sensorCount, sensorTriggers));
        }

        public CBSwitchWhere decodeCmdBodySwitch(byte[] bytes) {
            TString dev_name = new TString(bytes);
            TArray<TString> names = new TArray<>(Utils.arrCopyFrom(bytes, dev_name.sumBytes()), TString::new);
            return new CBSwitchWhere(dev_name, new DPNameList(names));
        }
    }

    static final class Base64Encoder {
        private static final Base64.Encoder encoder = Base64.getUrlEncoder();
        public static String encodePacket(Packet packet) {
            final byte[] bytes = encoder.encode(packet.encoding());
            String encodedString = new String(bytes, StandardCharsets.UTF_8).replace("=", "");

            StringBuilder fancyString = new StringBuilder();
            for (char c : encodedString.toCharArray()) {
                fancyString.append("*").append(c).append("*");
            }

            return fancyString.toString();
        }
    }

    static final class Utils {

        public static int concatBytes(byte[] src, byte[] dst, int index) {
            for (int i = 0; i < dst.length; i++, index++) {
                src[index] = dst[i];
            }
            return index;
        }

        public static byte[] concatByteArrays(byte[]... arrays) {
            if (arrays.length == 0) {
                return new byte[0];
            }

            final int length = Arrays.stream(arrays).mapToInt(arr -> arr.length).sum();
            final byte[] bytes = new byte[length];

            int index = Utils.concatBytes(bytes, arrays[0], 0);
            for (int i = 1; i < arrays.length; i++) {
                index = Utils.concatBytes(bytes, arrays[i], index);
            }

            return bytes;
        }

        public static byte[] arrCopyFrom(byte[] bytes, int from) {
            return Arrays.copyOfRange(bytes, from, bytes.length);
        }

        public static int checkCrc8(byte[] bytes) {
            int crc = 0;

            for (byte b : bytes) {
                crc ^= b;
                for (int j = 0; j < 8; j++) {
                    if ((crc & 0x80) != 0) {
                        crc = ((crc << 1) ^ 0x1D);
                    } else {
                        crc <<= 1;
                    }
                }
                crc &= 0xFF;
            }

            return crc & 0xFF;
        }
    }
}