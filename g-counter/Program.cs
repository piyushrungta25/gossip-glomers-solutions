using System.Timers;
using Maelstorm.ServerLib;

namespace Maelstorm.GCounter;



class CounterServer : Server
{
    private SemaphoreSlim DeltaLock = new SemaphoreSlim(1, 1);

    private SeqKVClient seqKVClient;
    long Delta = 0;
    long Counter = 0;
    string StoreKey = "gcounter";

    public CounterServer()
    {
        this.On(MsgType.Add, this.AddHandler);
        this.On(MsgType.Read, this.ReadHandler);

        this.SetupBackGroundJob(this.FlushDelta, 200);

        this.seqKVClient =  new SeqKVClient(this);
    }

    private void FlushDelta(object? sender, ElapsedEventArgs e)
    {
        long DeltaToFlush = Delta;

        try {
            this.Counter = long.Parse(this.seqKVClient.ReadStringOrDefault(StoreKey, 0).Result);
            this.seqKVClient.CAS(StoreKey, this.Counter, this.Counter + DeltaToFlush, true).Wait();
            this.DecrementDelta(DeltaToFlush).Wait();
        }
        catch(KVClientCASWriteException) {}
        
    }

    private async Task<Message?> AddHandler(Message msg) {
        long delta = (long)msg.body.delta!;
        await this.IncrementDelta(delta);

        return new Message {
            body = new MessageBody {
                type = MsgType.AddOk
            }
        };
    }


    private async Task<Message?> ReadHandler(Message msg)
    {
        return new Message {
            body = new GCounterMessageBody {
                type = MsgType.ReadOk,
                value = this.Counter
            }
        };
    }

    private async Task IncrementDelta(long val) {
        await DeltaLock.WaitAsync();
        Delta += val;
        DeltaLock.Release();
        
    }

    private async Task DecrementDelta(long val) {
        await DeltaLock.WaitAsync();
        Delta -= val;
        DeltaLock.Release();
    }

}

class Program
{
    static async Task Main(string[] args)
    {
        var server = new CounterServer();
        await server.Start();
    }
}
