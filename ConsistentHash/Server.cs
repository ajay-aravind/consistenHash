using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsistentHash
{
    //todo
    // Add mroe hash functions
    public class Server
    {
        internal String Id
        {
            get;
        }
        int VirtualServerPerServer;
        internal List<String> PhysicalServerToVirtualServersMap
        {
            get;
        }

        public Server(string id, int virtualServerPerServer)
        {
            Id = id;
            VirtualServerPerServer = virtualServerPerServer;
            PhysicalServerToVirtualServersMap = new List<String>();
        }



        internal void GenerateVirtualServers()
        {
            for(int i=0;i< VirtualServerPerServer; i++)
            {
                string virtualServerId = Guid.NewGuid().ToString();
                PhysicalServerToVirtualServersMap.Add(virtualServerId);
            }
            Console.WriteLine("Server: " + Id);
            // print virtual servers
            foreach (string virtualServer in PhysicalServerToVirtualServersMap)
            {
                Console.WriteLine("Virtual Server: " + virtualServer);
            }
        }


        // Bernstein hash

        int GetDjb2Hash(string s)
        {
            int hash = 5381;
            foreach (char c in s)
            {
                    hash = (hash * 33) + c;
            }
            return hash;
        }
    }
}
