using System.Timers;
using Maelstorm.ServerLib;

namespace Maelstorm.Broadcast;

class BroadcastServer : Server
{
    private HashSet<int> data;
    private string[] neighbors;

    private Dictionary<long, Message> Messages;
    private HashSet<int> batchedData;

    public BroadcastServer()
    {
        this.data = new HashSet<int>();
        this.batchedData = new HashSet<int>();
        this.neighbors = new string[0];
        Messages = new Dictionary<long, Message>();

        this.On(MsgType.Broadcast, BroadcastHandler);
        this.On(MsgType.Read, ReadHandler);
        this.On(MsgType.Topology, TopologyHandler);
        this.On(MsgType.BroadcastOk, BroadcastOkHandler);
        this.SetupBackGroundJob(SendBatchedJob, 750);

    }

    private void SendBatchedJob(object? sender, ElapsedEventArgs e)
    {
        lock(Messages) {
            foreach (var m in Messages)
                this.SendMessage(m.Value).Wait();
        }
        

        List<int> batchedMessages;
        lock(this.batchedData) {
            batchedMessages = this.batchedData.ToList();
            this.batchedData.Clear();
        }

        if(!batchedMessages.Any()) return;
    
        var filteredNeighbors = this.NodeIds.Where(x => x != this.NodeId).ToList();

        foreach (var neighbor in filteredNeighbors)
        {
            var nextMsgId = this.getNextMessageId();

            var m = new Message
            {
                dest = neighbor,
                body = new MessageBody
                {
                    type = MsgType.Broadcast,
                    messageBatch = batchedMessages,
                    msg_id = nextMsgId,
                }
            };

            // add to message map
            lock(Messages) Messages.Add(nextMsgId, m);
            SendMessage(m).Wait();
        }
            
        
    }

    private async Task<Message?> BroadcastOkHandler(Message msg)
    {
        if (msg.body.in_reply_to == null) return null;
        var msgId = (long)msg.body.in_reply_to;

        lock(Messages) {
            if (!this.Messages.ContainsKey(msgId)) return null;
            Messages.Remove(msgId);
        }
        

        return null;
    }


    public async Task<Message?> BroadcastHandler(Message message)
    {

        if(message.body.message != null) {
            int data = (int)message.body.message;

            if (!this.data.Contains(data))
            {
                this.StoreData(data);

                // if coming from client
                if(message.src.StartsWith("c")) {
                    lock(this.batchedData) {
                        this.batchedData.Add(data);
                    }
                }
            }
        }
        
        if(message.body.messageBatch != null)
            StoreDataBatched(message.body.messageBatch);


        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.BroadcastOk,
            }
        };
    }


    public async Task<Message?> ReadHandler(Message message)
    {
        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.ReadOk,
                messages = GetDataAsList()
            }
        };
    }

    public async Task<Message?> TopologyHandler(Message message)
    {
        var topology = message.body.topology;
        if (topology == null || !topology.ContainsKey(this.NodeId))
        {
            this.Log("Invalid topology data");
            return new Message
            {
                body = new MessageBody
                {
                    type = MsgType.Error
                }
            };
        }

        this.neighbors = topology[this.NodeId];

        return new Message
        {
            body = new MessageBody
            {
                type = MsgType.TopologyOk
            }
        };
    }


    public void StoreData(int d)
    {
        lock(this.data)
            this.data.Add(d);
    }

    public void StoreDataBatched(List<int> batchedData) {
        lock(this.data)
            this.data.UnionWith(batchedData);

    }

    public HashSet<int> GetData()
    {
        return this.data;
    }

    public List<int> GetDataAsList() {
        lock(this.data) // so that the underlying data does not change while it is being serialized
            return this.GetData().ToList();
    }


    public async Task ResendMessageJob(object? sender, ElapsedEventArgs e)
    {
        foreach (var m in Messages)
        {
            await this.SendMessage(m.Value);
        }

    }


}

class Program
{
    static async Task Main(string[] args)
    {
        var server = new BroadcastServer();
        await server.Start();
    }
}
