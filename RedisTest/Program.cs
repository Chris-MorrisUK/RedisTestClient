using System;

namespace RedisTest
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine(consts.UsageEssay);
                return;
            }
            string host = args[0];
           
                REDISConnector connector = new REDISConnector(host);
                if (connector.IsItThere())
                    Console.WriteLine("All present and correct");
                else
                    Console.WriteLine("Can't connect");
          

            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }
    }
}
