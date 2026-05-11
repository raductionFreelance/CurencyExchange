using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace CurencyExchangeClient
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string serverIp = "127.0.0.1";
            int port = 5000;

            using TcpClient client = new TcpClient();

            try
            {
                Console.WriteLine("Connecting to server...");
                await client.ConnectAsync(serverIp, port);
                using NetworkStream stream = client.GetStream();

                await ReceiveMessage(stream);

                Console.WriteLine("Enter login:password");
                string authInfo = Console.ReadLine();
                await SendMessage(stream, authInfo);

                string authResponse = await ReceiveMessage(stream);
                if (authResponse.Contains("Error"))
                {
                    Console.WriteLine("Доступ закрито. Перевірте дані.");
                    return;
                }


                while(true)
                {
                    Console.WriteLine("Enter currency pair (e.g., USD : EUR) or 'exit' to quit:");
                    string input = Console.ReadLine();

                    await SendMessage(stream, input);

                    if (input.ToLower() == "exit") break;

                    string response = await ReceiveMessage(stream);
                    Console.WriteLine($"Exchange rate: {response}");

                    if (response.Contains("Banned")) break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            Console.WriteLine("Disconnected from server.");
            Console.ReadLine();
        }

        static async Task<string> ReceiveMessage(NetworkStream stream)
        {
            byte[] buffer = new byte[1024];
            int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            if (bytesRead == 0) return "Disconnected";

            string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            return response;
        }

        static async Task SendMessage(NetworkStream stream, string message)
        {
            byte[] data = Encoding.UTF8.GetBytes(message);
            await stream.WriteAsync(data, 0, data.Length);
        }
    }
}
