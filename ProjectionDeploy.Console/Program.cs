namespace ProjectionDeploy.Console
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;
    using EventStore.ClientAPI;
    using EventStore.ClientAPI.Common.Log;
    using EventStore.ClientAPI.Projections;
    using EventStore.ClientAPI.SystemData;
    using Newtonsoft.Json;
    using static System.Console;

    class Program
    {
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };

        static void Main(string[] args)
        {
            //DeployProjections().Wait();
            WriteEvents(10).Wait();

            ReadLine();
        }

        private static async Task DeployProjections()
        {
            WriteLine("Creating projections...");

            var projectionsManager = new ProjectionsManager(new ConsoleLogger(), new IPEndPoint(IPAddress.Parse("192.168.99.100"), 2113), TimeSpan.FromMilliseconds(10000));
            var projections = DiscoverProjections();

            foreach (var projection in projections)
            {
                WriteLine($"\tCreating {projection.Name}...");
                await projectionsManager.CreateContinuousAsync(projection.Name, projection.Query, new UserCredentials("admin", "changeit"));
            }

            WriteLine("Finished creating projections");
        }

        private static IEnumerable<Projection> DiscoverProjections()
        {
            return from x in Assembly.GetExecutingAssembly().GetManifestResourceNames()
                   where x.StartsWith("ProjectionDeploy.Console.Projections") && x.EndsWith(".js")
                   let name = x.Replace("ProjectionDeploy.Console.Projections.", "").Replace(".js", "") // fix this shit
                   select new Projection
                   {
                       Name = name,
                       Query = GetQuery(x)
                   };
        }

        private static string GetQuery(string name)
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(name))
            using (var reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        private static async Task WriteEvents(int count)
        {
            WriteLine("Writing events...");

            var random = new Random();
            using (var connection = EventStoreConnection.Create(ConnectionSettings.Default, new Uri("tcp://admin:changeit@192.168.99.100:1113")))
            {
                await connection.ConnectAsync();

                var events = new List<EventData>();
                for (var i = 0; i < count; i++)
                {
                    events.Add(ToEventData(new DummyEventA
                    {
                        Value = random.Next(1, 1000)
                    }));
                }

                await connection.AppendToStreamAsync("DummyEvents", ExpectedVersion.Any, events);
            }

            WriteLine("Finished writing events");
        }


        private static EventData ToEventData(object e)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(e, SerializerSettings));
            var metadata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new Dictionary<string, object>(), SerializerSettings));
            var typeName = e.GetType().Name;

            return new EventData(Guid.NewGuid(), typeName, true, data, metadata);
        }
    }
}