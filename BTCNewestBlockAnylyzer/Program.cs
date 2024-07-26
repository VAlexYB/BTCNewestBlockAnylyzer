using CsvHelper;
using Newtonsoft.Json.Linq;
using System.Globalization;

public class Programm
{
    private static readonly HttpClient _httpClient = new HttpClient();
    public static async Task Main(string[] args)
    {
        try
        { 
            string latestBlockUrl = "https://api.blockcypher.com/v1/btc/main";
            var latestBlockResponse = await _httpClient.GetStringAsync(latestBlockUrl);
            var blockInfo = JObject.Parse(latestBlockResponse);
            string latestBlockHash = blockInfo["hash"].ToString();

            string blockUrl = $"https://api.blockcypher.com/v1/btc/main/blocks/{latestBlockHash}";
            var blockResponse = await _httpClient.GetStringAsync(blockUrl);
            var blockData = JObject.Parse(blockResponse);

            int blockHeight = blockData["height"].ToObject<int>();
            string filePath = $"block_{blockHeight}.csv";

            if (File.Exists(filePath))
            {
                await Console.Out.WriteLineAsync("Block already analyzed");
                return;
            }
            var (blockRecord, transactionRecords) = BuildBlockData(blockData);
            SaveBlockDataToCsv(blockRecord, transactionRecords, filePath);

            await Console.Out.WriteLineAsync("Operation succesfully completed");
        }
        catch (Exception ex)
        {
            await Console.Out.WriteLineAsync($"Error: {ex.Message}");
        }
    }

    public static (BlockRecord, List<TransactionRecord>) BuildBlockData(JObject blockData)
    {
        var blockRecord = new BlockRecord
        {
            Hash = blockData["hash"].ToString(),
            Height = blockData["height"].ToObject<int>(),
            Total = blockData["total"].ToObject<long>(),
            Fees = blockData["fees"].ToObject<long>(),
            Size = blockData["size"].ToObject<int>(),
            Time = blockData["time"].ToString(),
            TxCount = blockData["n_tx"].ToObject<int>(),
            PrevBlock = blockData["prev_block"].ToString()
        };

        var transactionRecords = new List<TransactionRecord>();
        var txIds = blockData["txids"];

        foreach (var txId in txIds)
        {
            transactionRecords.Add(new TransactionRecord
            {
                BlockHash = blockRecord.Hash,
                TxId = txId.ToString()
            });
        }

        return (blockRecord, transactionRecords);
    }

    public static void SaveBlockDataToCsv(BlockRecord blockRecord, List<TransactionRecord> transactionRecords, string filePath)
    {
        const int maxRetries = 2;
        const int delayMilliseconds = 3000;

        bool success = false;
        int retryCount = 0;

        while (!success && retryCount < maxRetries)
        {
            try
            {
                using (var writer = new StreamWriter(filePath))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(new List<BlockRecord> { blockRecord });
                    csv.NextRecord();

                    csv.WriteField("BlockHash");
                    csv.WriteField("TxId");
                    csv.NextRecord();
                    foreach (var transaction in transactionRecords)
                    {
                        csv.WriteRecord(transaction);
                        csv.NextRecord();
                    }
                }
                success = true;
            }
            catch (IOException ex)
            {
                retryCount++;

                if(retryCount > maxRetries)
                {
                    Console.WriteLine("Failed to write to file after multiple attempts.");
                    Console.WriteLine($"Exception: {ex.Message}");
                    throw;
                }

                Console.WriteLine($"File is occupied by another process. Retrying in {delayMilliseconds / 1000} seconds...");
                Thread.Sleep(delayMilliseconds);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }
    }
}

public class BlockRecord
{
    public string Hash { get; set; }
    public int Height { get; set; }
    public long Total { get; set; }
    public long Fees { get; set; }
    public int Size { get; set; }
    public string Time { get; set; }
    public int TxCount { get; set; }
    public string PrevBlock { get; set; }
}

public class TransactionRecord 
{
    public string BlockHash { get; set; }
    public string TxId { get; set; }
}
