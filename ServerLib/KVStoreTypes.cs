namespace Maelstorm.ServerLib;

// types used in challenge #6

public class VersionedValue {
    public int value {get; set;}
    public int[] version {get; set;}
}

public class ReplictionTask {
    public Dictionary<int, VersionedValue> tx {get; set;}
    public string dest {get; set;}
}
