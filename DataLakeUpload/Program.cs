using Azure.Storage.Files.DataLake;
using System;
using System.IO;
using System.Threading.Tasks;

namespace DataLakeUpload
{
    class Program
    {
        private const int _size = 10 * 1024 * 1024;

        static async Task Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("DATALAKE_CONNECTION_STRING");
            
            var serviceClient = new DataLakeServiceClient(connectionString);

            var fileSystemName = Guid.NewGuid().ToString();
            var fileName = Path.GetRandomFileName();

            var fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);
            var fileClient = fileSystemClient.GetFileClient(fileName);

            try
            {
                Console.WriteLine("Creating file system...");
                await fileSystemClient.CreateIfNotExistsAsync();

                Console.WriteLine("Creating file...");
                await fileClient.CreateIfNotExistsAsync();

                using (var stream = new MemoryStream(new byte[_size]))
                {
                    Console.WriteLine("Uploading stream...");
                    var response = await fileClient.UploadAsync(stream, overwrite: true);

                    Console.WriteLine($"Status: {response.GetRawResponse().Status}");
                }
            }
            finally
            {
                Console.WriteLine("Deleting file system...");
                await fileSystemClient.DeleteIfExistsAsync();
            }
        }
    }
}
