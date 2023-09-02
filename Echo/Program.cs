using Maelstorm.ServerLib;

namespace Maelstorm.Echo;


class Program
{
    public static async Task<Message?> EchoHandler(Message message) {
        return new Message {
            body = new MessageBody {
                type = MsgType.EchoOK,
                echo = message.body.echo
            }
        };

    }

    static async Task Main(string[] args)
    {
        var server = new Server();
        
        server.On(MsgType.Echo, EchoHandler);
        await server.Start();
    }
}
