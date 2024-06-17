// See https://aka.ms/new-console-template for more information
using System.Data;
using ConsistentHash;

Console.WriteLine("Hello, World!");

Ring ring = new Ring();
// Add servers in a loop up to 100


for(int j = 0; j < 50; j++) 
{ 
    for (int i = 0; i < 2; i++)
    {
        ring.AddServer(Guid.NewGuid().ToString());
    }

    Random random = new Random(); 
    int randomNumber = random.Next(0, ring.servers.Count-1);
    // print random server
    Console.WriteLine("Random Server: " + ring.servers[randomNumber].Id);
    ring.RemoveServer(ring.servers[randomNumber].Id);
}

// Send 10 requests to the ring
// Send 10 requests to the ring
for (int i = 0; i < 1000000; i++)
{
    //Console.WriteLine("Request: " + i);
    ring.GetServerToHandleRequest(Guid.NewGuid().ToString());
    //Console.WriteLine("Server: " + );
}

// print divider
Console.WriteLine("-------------------------------------------------");

// print ring.requestCountPerServer
foreach (String server in ring.requestCountPerServer.Keys)
{
    Console.WriteLine("Server: " + server + " Request Count: " + ring.requestCountPerServer[server]);
}