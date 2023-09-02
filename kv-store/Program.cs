using System.Collections.Concurrent;
using System.Formats.Asn1;
using System.Text.Json;
using System.Text.Json.Serialization;
using Maelstorm.KVStore;
using Maelstorm.ServerLib;
using Microsoft.VisualBasic;

namespace Maelstorm.KVStore;

class Program
{
    static async Task Main(string[] args)
    {
        var server = new KVStoreServer();
        await server.Start();
    }
}


enum Ordering
{
    GREATER,
    LESS,
    CONCURRENT
}




class KVStoreServer : Server
{
    ConcurrentDictionary<int, VersionedValue> ds = new();
    BlockingCollection<ReplictionTask> replictionTasks = new BlockingCollection<ReplictionTask>();
    Task ReplicationTaskAwaitable;

    object serializableLock = new object();

    ConcurrentDictionary<int, object> locks = new();

    public KVStoreServer()
    {
        // override the base init handler to initialize the vector clock
        this.On(MsgType.Txn, this.HandleTxn);
        this.On(MsgType.TxnSync, this.HandleWriteReplication);

        this.ReplicationTaskAwaitable = Task.Run(async () => await this.ReplicationJob());
    }

    public async Task<Message?> HandleTxn(Message message)
    {
        var txns = message.body.txn ?? throw new ArgumentException("txn null");

        Dictionary<int, VersionedValue> writes = new();

        lock (this.serializableLock)
        {
            foreach (var txn in txns)
            {

                var op = txn[0].GetString().ToLower();
                var key = txn[1].GetInt32();
                var valPresent = this.ds.TryGetValue(key, out var versionedValue);

                if (op == "r")
                {
                    if (valPresent)
                    {
                        txn[2] = JsonSerializer.SerializeToElement(versionedValue.value);
                        this.Log($"TS: {JsonSerializer.Serialize(versionedValue.version)}");
                    }
                }
                else if (op == "w")
                {
                    var ts = valPresent ? versionedValue.version : new int[this.NodeIds.Length];
                    this.IncrementTs(ts);
                    this.Log($"TS: {JsonSerializer.Serialize(ts)}");
                    var newVersionedValue = new VersionedValue
                    {
                        version = ts,
                        value = txn[2].GetInt32()
                    };

                    this.ds[key] = newVersionedValue;

                    // queue for replication
                    writes[key] = newVersionedValue;
                }
            }
        }

        // foreach (var kvp in writes)
        if (writes.Count > 0)
            this.QueueReplication(writes);

        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.TxnOk,
                txn = txns
            }
        };
    }

    private async Task<Message?> HandleWriteReplication(Message message)
    {
        lock (this.serializableLock)
        {
            var syncVal = message.body.syncVal ?? throw new ArgumentException("txn sync null");

            foreach (var kvp in syncVal.tx)
            {
                var key = kvp.Key;
                var val = kvp.Value;

                this.Log($"Try sync key: {key}, {key.GetType().Name}, value: {val.value}, ver: {val.version[0]}, {val.version[1]}");


                var valPresent = this.ds.TryGetValue(key, out var versionedValue);

                if (!valPresent)
                {
                    // just write in our data store
                    this.Log("Key not present, overwriting");
                    this.ds[key] = val;
                }
                else
                {
                    var localVersion = versionedValue.version;
                    this.Log($"Key: {key}. Comparing local: {JsonSerializer.Serialize(localVersion)}, sync: {JsonSerializer.Serialize(val.version)}");

                    var res = CompareTimestamp(localVersion, val.version);
                    var newVersion = SyncVersions(localVersion, val.version);
                    int newValue; // = versionedValue.value;

                    if (res == Ordering.CONCURRENT)
                    {
                        if (AsInt(this.NodeId) > AsInt(message.src))
                        {
                            this.Log("Concurrent, keeping local");
                            newValue = versionedValue.value;
                        }
                        else
                        {
                            this.Log("Concurrent, keeping remote");
                            newValue = val.value;
                        }

                    }
                    else if (res == Ordering.GREATER)
                    {
                        this.Log("Greater, keeping local");
                        newValue = versionedValue.value;
                    }
                    else
                    {
                        this.Log("Less, keeping remote");
                        newValue = val.value;
                    }

                    this.ds[key] = new VersionedValue
                    {
                        version = newVersion,
                        value = newValue
                    };

                }

            }

            return new Message
            {
                body = new MessageBody { type = MsgType.TxnSyncOk }
            };
        }

    }

    private int[] SyncVersions(int[] localTs, int[] receivedTs)
    {
        for (int i = 0; i < receivedTs.Length; i++)
            receivedTs[i] = Math.Max(receivedTs[i], localTs[i]);

        return receivedTs;
    }

    private int AsInt(string a)
    {
        return int.Parse(this.NodeId.Substring(1));
    }

    private Ordering CompareTimestamp(int[] val1, int[] val2)
    {
        bool equals = true;
        bool val1Greater = true;
        bool val2Greater = true;


        for (int i = 0; i < val1.Length; i++)
        {
            equals &= (val1[i] == val2[i]);
            val1Greater &= (val1[i] >= val2[i]);
            val2Greater &= (val1[i] <= val2[i]);
        }

        if (equals)
            return Ordering.CONCURRENT;

        if (val1Greater)
            return Ordering.GREATER;

        if (val2Greater)
            return Ordering.LESS;

        return Ordering.CONCURRENT;
    }

    private async Task ReplicationJob()
    {
        List<Task> taskList = new();

        try
        {
            while (true)
            {
                this.Log($"Queue length: {replictionTasks.Count}");
                var task = replictionTasks.Take();
                this.Log($"Got task: {task}");

                taskList.Add(Task.Run(async () =>
                {
                    try
                    {
                        var resp = await this.SendRequestSync(new Message
                        {
                            dest = task.dest,
                            body = new MessageBody
                            {
                                type = MsgType.TxnSync,
                                syncVal = task
                            }
                        });

                        if (resp.body.type != MsgType.TxnSyncOk)
                            throw new Exception($"Unexpected resp {resp.body.type}");
                    }
                    catch (Exception e)
                    {
                        this.Log($"Rep req failed {e}");
                        // ignore and retry later
                        this.replictionTasks.Add(task);
                    }
                }));

                taskList.RemoveAll(i => i.IsCompleted);
            }
        }
        catch (Exception e)
        {
            this.Log($"bad ex: {e}");
            this.Log("Early Exiting replication job");
        }

        this.Log("Exiting replication job");
    }

    private void QueueReplication(Dictionary<int, VersionedValue> tx)
    {
        foreach (var nodeId in this.NodeIds)
            if (nodeId != this.NodeId)
                this.replictionTasks.Add(new ReplictionTask
                {
                    tx = tx,
                    dest = nodeId
                });
    }

    private int[] IncrementTs(int[] ts)
    {
        int nodeId = AsInt(this.NodeId);
        ts[nodeId] += 1;
        return (int[])ts.Clone();
    }
}