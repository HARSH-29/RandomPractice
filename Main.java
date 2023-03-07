import java.nio.ByteBuffer;
import java.util.*;

class Record {
    private Integer mapId;
    private String key;
    private String value;

    public Record(Integer mapId, String key, String value) {
        this.mapId = mapId;
        this.key = key;
        this.value = value;
    }

    public Integer getPartitionId() {
        return this.mapId;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }
}

class JsonObject {
    public void add(JsonArray array) {
        // WOW
    }
}
class JsonArray {
    List<Record> records;
    public JsonArray() {
        this.records = new ArrayList<>();
    }

    public void put(Record e) {
        records.add(e);
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public Record get(int index) { return records.get(index); }

    public Iterator<Record> getIterator() {
        return records.iterator();
    }
}

public class Main {
    private static boolean isBufferFull(JsonArray jsonArray, Record record, int bufferSize) {
        JsonObject jsonObject = new JsonObject();
        // JsonArray newJsonArray = new JsonArray(jsonArray);
        // newJsonArray.add(record);
        jsonObject.add(jsonArray);
        return ByteBuffer.wrap(jsonObject.toString().getBytes()).position() > bufferSize;
    }
    private static void write(int numPartitions, int bufferSize, List<Record> records) {
        Map<Integer, JsonArray> partitionIdToJsonArrayMap = new HashMap<Integer, JsonArray>();
        for (int i = 0; i < numPartitions; i++ ) {
            partitionIdToJsonArrayMap.put(i, new JsonArray());
        }
        for (Record r: records) {
            partitionIdToJsonArrayMap.get(r.getPartitionId()).put(r);
        }
        // Now partitionIdToJsonArrayMap contains all records for each partition id
        for (int i = 0; i < numPartitions; i++ ) {
            if (!partitionIdToJsonArrayMap.containsKey(i)) continue;
            if (partitionIdToJsonArrayMap.get(i).isEmpty()) continue;
            Iterator<Record> recordIterator = partitionIdToJsonArrayMap.get(i).getIterator();
            JsonArray localJsonArray = new JsonArray();
            while(recordIterator.hasNext()) {
                Record record = recordIterator.next();
                if (isBufferFull(localJsonArray, record, bufferSize)) {
                    // dataService.write()
                    // clear local json array
                }
                localJsonArray.put(record);
            }
        }
    }

    public static void main(String[] args) {
        List<Record> records = new ArrayList<Record>();
        records.add(new Record(1,"harsh", "gupta"));
        records.add(new Record(2, "ayush", "raj"));
        write(10, 128*1024*1024, records);
    }
}