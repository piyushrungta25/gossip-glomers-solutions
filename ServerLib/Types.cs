using System.Text.Json;
using System.Text.Json.Serialization;

namespace Maelstorm.ServerLib;

public class Message {
    public string src {get; set;}
    public string dest {get; set;}
    public MessageBody body {get; set;}

    public Message() {
        this.src = "";
        this.dest = "";
        this.body = new MessageBody();
    }
}

[JsonDerivedType(typeof(GCounterMessageBody))]
public class MessageBody {
    // base
    public string? type {get; set;}
    public long? msg_id {get; set;}
    public long? in_reply_to {get; set;}
    
    // Init
    public string? node_id {get;set;}
    public string[]? node_ids {get; set;}

    // echo
    public string? echo {get; set;}

    // uid
    public string? id {get; set;}

    // broadcast
    public int? message {get; set;}
    public List<int>? messages {get; set;}
    public Dictionary<string, string[]>? topology {get; set;}
    public List<int>? messageBatch {get; set;}

    // gcounter
    public string? value {get; set;}
    public string? key {get; set;}
    public long? delta {get; set;}

    // seq-kv, lin-kv
    public string? from {get; set;}
    public string? to {get; set;}
    public bool? create_if_not_exists {get; set;}

    // error
    public int? code {get; set;}
    public string? text {get; set;}

    // kafka
    public long? msg {get; set;}
    public long? offset {get; set;}
    public Dictionary<string, long>? offsets {get; set;}
    public Dictionary<string, List<long[]>>? msgs {get; set;}
    public List<string>? keys {get; set;}

    // kvstore
    public List<List<JsonElement>>? txn {get; set;}
    public ReplictionTask? syncVal {get; set;}
}

public class MsgType {
    public static string Init = "init";
    public static string Init_Ok = "init_ok";

    public static string Echo = "echo";
    public static string EchoOK = "echo_ok";

    public static string Generate = "generate";
    public static string GenerateOk = "generate_ok";

    public static string Broadcast = "broadcast";
    public static string BroadcastOk = "broadcast_ok";

    public static string Read = "read";
    public static string ReadOk = "read_ok";

    public static string Write = "write";
    public static string WriteOk = "write_ok";

    public static string Cas = "cas";
    public static string CasOk = "cas_ok";

    public static string Send = "send";
    public static string SendOk = "send_ok";

    public static string Poll = "poll";
    public static string PollOk = "poll_ok";

    public static string CommitOffsets = "commit_offsets";
    public static string CommitOffsetsOk = "commit_offsets_ok";

    public static string ListCommittedOffsets = "list_committed_offsets";
    public static string ListCommittedOffsetsOk = "list_committed_offsets_ok";

    public static string Topology = "topology";
    public static string TopologyOk = "topology_ok";

    public static string Add = "add";
    public static string AddOk = "add_ok";

    public static string KafkaSendSync = "kafka_send_sync";
    public static string KafkaSendSyncOk = "kafka_send_sync_ok";

    public static string Txn = "txn";
    public static string TxnOk = "txn_ok";

    public static string TxnSync = "txn_sync";
    public static string TxnSyncOk = "txn_sync_ok";

    public static string Error = "error";
}

public class RPCTimeoutException: Exception {}
