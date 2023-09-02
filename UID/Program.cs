using Maelstorm.ServerLib;

namespace Maelstorm.UID;


class Program
{
    public static async Task<Message?> UIDHandler(Message message) {
        return new Message {
            body = new MessageBody {
                type = MsgType.GenerateOk,
                id = Guid.NewGuid().ToString()
            }
        };

    }

    static async Task Main(string[] args)
    {
        var server = new Server();
        
        server.On(MsgType.Generate, UIDHandler);
        await server.Start();
    }
}
