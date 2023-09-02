using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Maelstorm.ServerLib;

namespace Maelstorm.Kafka;


public struct Entry
{
    public long offset { get; set; }
    public long message { get; set; }
}

public class EntryComparer : IComparer<Entry>
{
    public int Compare(Entry x, Entry y)
    {
        return x.offset.CompareTo(y.offset);
    }
}

// solution inspired from https://github.com/teivah/gossip-glomers/blob/main/challenge-5c-kafka-log/main.go

class KafkaServer : Server
{
    private const string prefixCommit = "commit_";
    private const string prefixLatest = "latest_";
    private const string prefixEntry = "entry_";

    private LinKVClient linKV { get; set; }

    private ConcurrentDictionary<string, SemaphoreSlim> locks = new();

    public KafkaServer()
    {
        this.On(MsgType.Send, this.HandleSend);
        this.On(MsgType.Poll, this.HandlePoll);
        this.On(MsgType.CommitOffsets, this.HandleCommitOffsets);
        this.On(MsgType.ListCommittedOffsets, this.HandleListCommitOffsets);

        this.linKV = new LinKVClient(this);
    }

    private async Task<Message?> HandleListCommitOffsets(Message msg)
    {
        var keys = msg.body.keys ?? throw new Exception();
        var res = new Dictionary<string, long>();

        foreach (var key in keys)
        {
            var offset = await this.ReadClientCommitOffset(key);
            if (offset != null) res[key] = (long)offset;
        }

        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.ListCommittedOffsetsOk,
                offsets = res
            }
        };
    }


    private async Task<Message?> HandleCommitOffsets(Message msg)
    {
        var offsets = msg.body.offsets ?? throw new Exception();
        foreach (var item in offsets)
            await this.CommitClientOffset(item.Key, item.Value);

        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.CommitOffsetsOk
            }
        };
    }

    private async Task<Message?> HandlePoll(Message msg)
    {
        var offsets = msg.body.offsets ?? throw new Exception();
        Dictionary<string, List<long[]>> res = new Dictionary<string, List<long[]>>();

        foreach (KeyValuePair<string, long> item in offsets)
        {

            var reads = "";
            try
            {
                reads = await this.linKV.ReadString($"{prefixEntry}{item.Key}");
            }
            catch (KVClientKeyNotFoundException) { }

            var logs = this.ToEntries(reads);
            var startingOff = item.Value;
            var ind = logs.BinarySearch(new Entry
            {
                offset = startingOff
            }, new EntryComparer());

            if (ind < 0) ind = ~ind;

            res[item.Key] = new List<long[]>();
            for (; ind < logs.Count(); ind++)
            {
                res[item.Key].Add(new long[] { logs[ind].offset, logs[ind].message });
            }
        }

        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.PollOk,
                msgs = res
            }
        };
    }

    private async Task<Message?> HandleSend(Message msg)
    {
        string key = msg.body.key ?? throw new Exception();
        long value = msg.body.msg ?? throw new Exception();


        var ivalue = int.Parse(key);


        if (ivalue % this.NodeIds.Count() != int.Parse(this.NodeId.Substring(1)))
        {
            // forward it
            var neigh = $"n{ivalue % this.NodeIds.Count()}";
            var resp = await this.SendRequestSync(new Message
            {
                dest = neigh,
                body = new MessageBody
                {
                    type = MsgType.Send,
                    key = msg.body.key,
                    msg = msg.body.msg
                }
            });

            return new Message
            {
                body = new MessageBody
                {
                    type = MsgType.SendOk,
                    offset = resp.body.offset
                }
            };
        }



        var semaphore = this.locks.GetOrAdd(key, new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync();

        try {
            var keyLatest = $"{prefixLatest}{key}";
            long off = 0;
            try
            {
                off = await this.linKV.Read(keyLatest) + 1;
            }
            catch (KVClientKeyNotFoundException) { }

            await this.linKV.Write(keyLatest, off);

            var keyEntry = $"{prefixEntry}{key}";
            var v = "";
            try
            {
                v = await this.linKV.ReadString(keyEntry);
            }
            catch (KVClientKeyNotFoundException) { };

            v = v + $"{off}={value};";

            await this.linKV.WriteString(keyEntry, v);

            return new Message
            {
                body = new MessageBody
                {
                    type = MsgType.SendOk,
                    offset = off
                }
            };
        }
        finally {
            semaphore.Release();
        }
    }

    private async Task<long?> ReadClientCommitOffset(string key)
    {
        var storeKey = $"client_offset_{key}";

        try
        {
            return await this.linKV.Read(storeKey);
        }
        catch (KVClientKeyNotFoundException)
        {
            return null;
        }

    }

    private async Task CommitClientOffset(string key, long offset)
    {
        await this.linKV.Write($"client_offset_{key}", offset);
    }

    private List<Entry> ToEntries(string v)
    {
        if (v == "") return new List<Entry>();

        var entries = new List<Entry>();
        foreach (var e in v.Split(";"))
        {
            if (e.Length != 0)
            {
                var parts = e.Split("=");
                var end = new Entry
                {
                    offset = long.Parse(parts[0]),
                    message = long.Parse(parts[1])
                };
                entries.Add(end);
            }

        }

        return entries;
    }


}

class Program
{
    static async Task Main(string[] args)
    {
        var server = new KafkaServer();
        await server.Start();
    }
}
