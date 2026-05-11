using System.Collections.Concurrent;
using System.Data;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;


namespace CurencyExchangeServer
{
    public class CurrencyExchangeServer
    {
        private readonly ConcurrentDictionary<string, DateTime> _bannedClients = new();
        private const int maxNumRequests = 5;
        private const int BanDurationSeconds = 1;

        private const int MaxConcurrentClients = 10; 
        private readonly SemaphoreSlim _connectionSemaphore;

        private readonly TcpListener _listener;
        private bool _isRunning;

        public event Action<string>? OnLog;

        public CurrencyExchangeServer(int port)
        {
            _listener = new TcpListener(IPAddress.Any, port);
            _connectionSemaphore = new SemaphoreSlim(MaxConcurrentClients, MaxConcurrentClients);
        }

        private readonly Dictionary<string, string> _users = new()
        {
            { "admin", "12345" },
            { "user1", "qwerty" },
            { "stud", "pass2026" }
        };

        public async Task StartAsync()
        {
            _listener.Start();

            Log("Currency Exchange Server is running on port 5000...");

            _isRunning = true;
            while (_isRunning)
            {
                await _connectionSemaphore.WaitAsync();

                var client = await _listener.AcceptTcpClientAsync();
                string clientIp = ((IPEndPoint)client.Client.RemoteEndPoint!).Address.ToString();

                if (_bannedClients.TryGetValue(clientIp, out DateTime banUntil))
                {
                    if (DateTime.Now < banUntil)
                    {
                        Log($"[REFUSED] Client {clientIp} is still banned until {banUntil:HH:mm:ss}");
                        client.Close();
                        _connectionSemaphore.Release();
                        continue;
                    }
                    else
                    {
                        _bannedClients.TryRemove(clientIp, out _); 
                    }
                }


                Log($"Client {client.Client.RemoteEndPoint} connected at {DateTime.Now}.");

                _ = HandleClientAsync(client, clientIp);

            }
        }

        private  async Task HandleClientAsync(TcpClient client, string clientIp)
        {
            try {
                

                int requestCount = 0;

                var course = new Dictionary<string, string> {
                {"UAH : USD", "44,04" },
                {"UAH : EUR", "52,07" },
                {"UAH : GBP", "60,07" },
                {"USD : UAH", "0,023" },
                {"EUR : UAH", "0,019" },
                {"GBP : UAH", "0,017" }
            };

                using (client)
                using (NetworkStream stream = client.GetStream())
                {
                    try
                    {
                        byte[] buffer = new byte[1024];

                        byte[] authPrompt = Encoding.UTF8.GetBytes("Auth required. Send login:password");
                        await stream.WriteAsync(authPrompt, 0, authPrompt.Length);

                        int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                        if (bytesRead == 0) return;

                        string authData = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
                        string[] parts = authData.Split(':');

                        if (parts.Length != 2 || !_users.TryGetValue(parts[0], out string? password) || password != parts[1])
                        {
                            Log($"[AUTH FAILED] {clientIp} tried login with: {authData}");
                            byte[] error = Encoding.UTF8.GetBytes("Error: Invalid login or password.");
                            await stream.WriteAsync(error, 0, error.Length);
                            return; 
                        }

                        while (true)
                        {
                            bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                            if (bytesRead == 0) break;


                            string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);

                            if (message.ToLower().Trim() == "exit")
                            {
                                Log($"Client {client.Client.RemoteEndPoint} disconnected at {DateTime.Now}.");
                                break;
                            }

                            requestCount++;

                            Log($"Received from {client.Client.RemoteEndPoint} about {message} at {DateTime.Now}.");

                            if (requestCount >= maxNumRequests)
                            {
                                Log($"[BAN] Client {clientIp} exceeded limit. Banning for {BanDurationSeconds}s.");

                                _bannedClients[clientIp] = DateTime.Now.AddSeconds(BanDurationSeconds);

                                string banMsg = $"Error: Too many requests. You are banned for {BanDurationSeconds} seconds.";
                                byte[] banData = Encoding.UTF8.GetBytes(banMsg);
                                await stream.WriteAsync(banData, 0, banData.Length);

                                break;
                            }

                            byte[] response;

                            if (course.ContainsKey(message))
                            {
                                response = Encoding.UTF8.GetBytes($"Server response: {message} - {course[message]}");
                            }
                            else
                            {
                                response = Encoding.UTF8.GetBytes("Server response: Unknown currency pair.");
                            }

                            await stream.WriteAsync(response, 0, response.Length);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log($"Error: {ex.Message}");
                    }
                }

                } finally {
                _connectionSemaphore.Release();
                Log($"Client {clientIp} disconnected. Slots free: {_connectionSemaphore.CurrentCount}/{MaxConcurrentClients}");
            }
        }
        private void Log(string msg) => OnLog?.Invoke($"{DateTime.Now:HH:mm:ss} | {msg}");
        public void Stop() { _isRunning = false; _listener.Stop(); }
    }


    internal class Program
    {
        static async Task Main(string[] args)
        {
            var Server = new CurrencyExchangeServer(5000);
            Server.OnLog += msg => Console.WriteLine(msg);

            await Server.StartAsync();
        }
    }
}
