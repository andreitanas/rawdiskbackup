using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.IO;
using System.Security.Cryptography;
using Newtonsoft.Json.Bson;

namespace ImageBackup
{

    public class Program
    {
        private static Settings settings;
        private const int HASH_LENGTH = 20;

        public static void Main(string[] args)
        {
            settings = JsonConvert.DeserializeObject<Settings>(File.ReadAllText("appsettings.json"));

            long diskSize = 0;
            using (var f = File.OpenRead(settings.DeviceFile))
            {
                f.Seek(0, SeekOrigin.End);
                diskSize = f.Position;
            }

            var numBlocks = diskSize / (settings.BlockSizeKB * 1024);
            if ((diskSize % (settings.BlockSizeKB * 1024)) > 0) numBlocks++;
            Console.WriteLine($"Source is {diskSize / 1024 / 1024} MiB ({numBlocks} blocks)");

            byte[][] hashes = null;
            // read stored block hashes
            var hashFilePath = Path.Combine(settings.BackupDir, settings.FilePrefix + "hash.bin");
            if (File.Exists(hashFilePath))
            {
                var hashFI = new FileInfo(hashFilePath);
                var hashBlocks = hashFI.Length / HASH_LENGTH;
                if (numBlocks != hashBlocks)
                    throw new Exception("Number of hash blocks does not match number of file blocks. Has source file changed?");

                hashes = new byte[numBlocks][];

                var allHashes = File.ReadAllBytes(hashFilePath);
                if (allHashes.Length != hashFI.Length)
                    throw new Exception("WTF?");

                for (int i = 0; i < numBlocks; i++)
                {
                    hashes[i] = new byte[HASH_LENGTH];
                    for (int j = 0; j < HASH_LENGTH; j++)
                        hashes[i][j] = allHashes[i * HASH_LENGTH + j];
                }
            }

            var destFile = Path.Combine(settings.BackupDir, settings.FilePrefix + "full.img");

            if (hashes == null)
            {
                Console.WriteLine("Hash file does not exist");
                if (File.Exists(destFile))
                {
                    Console.WriteLine("Full backup file exists, aborting");
                    Environment.Exit(-1);
                }
                else
                {
                    Console.WriteLine("Full backup file does not exist, will create new one");
                    if ((diskSize % (settings.BlockSizeKB * 1024)) > 0) numBlocks++;
                    hashes = new byte[numBlocks][];

                    using (var dest = File.OpenWrite(destFile))
                    {
                        long blocksWritten = 0;
                        foreach (var block in ReadSource())
                        {
                            try {
                                dest.Write(block.Bytes, 0, block.Length);
                                hashes[block.BlockIndex] = block.Hash;
                                blocksWritten++;
                            }
                            catch (Exception ex) {
                                Console.WriteLine($"{hashes.Length}, {block.BlockIndex}, {block.Length}, {ex.Message}");
                            }
                        }
                        if (blocksWritten != numBlocks)
                            throw new Exception($"Incorrect number of blocks copied ({blocksWritten})");
                    }
                    SaveHashes(hashes, hashFilePath);
                }
            }
            else
            {
                FileStream dest = null;
                BsonWriter bw = null;
                JsonSerializer ser = null;
                var incrementName = IncrementFileName();
                try
                {
                    var changedBlocks = 0;
                    foreach (var block in ReadSource())
                    {
                        if (!CompareHash(hashes[block.BlockIndex], block.Hash))
                        {
                            changedBlocks++;
                            if (dest == null)
                            {
                                dest = File.OpenWrite(incrementName + "inc.bin");
                                bw = new BsonWriter(dest);
                                ser = new JsonSerializer();
                                Console.WriteLine($"Changes detected                       ");
                            }

                            ser.Serialize(bw, block);
                        }
			hashes[block.BlockIndex] = block.Hash;
                    }
                    Console.WriteLine($"{changedBlocks} blocks changed");
                    SaveHashes(hashes, incrementName + "hash.bin");
                }
                finally
                {
                    if (dest != null)
                        dest.Dispose();
                }
            }

            Console.WriteLine("Done");
            Environment.Exit(0);
        }

        private static bool CompareHash(byte[] hash1, byte[] hash2)
        {
            if (hash1.Length != hash2.Length)
                throw new ArgumentException("Hash sizes different?");

            for (int i = 0; i < hash1.Length; i++)
                if (hash1[i] != hash2[i])
                    return false;

            return true;
        }

        private static string IncrementFileName()
        {
            for (int i = 0; ; i++)
            {
                var path = Path.Combine(settings.BackupDir, $"{settings.FilePrefix}{i:0000}-");
                if (!File.Exists(path + "inc.bin"))
                    return path;
            }
        }

        private static IEnumerable<SourceBlock> ReadSource()
        {
            var lastUpdate = DateTime.Now.AddSeconds(-10);
            var start = DateTime.Now;
            long totalRead = 0;
            long blockIdx = 0;
            var buffer = new byte[settings.BlockSizeKB * 1024];

            using (var source = File.OpenRead(settings.DeviceFile))
            {
                int bytesRead;
                do
                {
                    bytesRead = source.Read(buffer, 0, buffer.Length);
                    totalRead += bytesRead;
                    if (bytesRead > 0)
                    {
                        yield return new SourceBlock {
                            Bytes = buffer,
                            Length = bytesRead,
                            Hash = SHA1.Create().ComputeHash(buffer, 0, bytesRead),
                            BlockIndex = blockIdx++,
                        };
                    }
                    if (bytesRead < settings.BlockSizeKB * 1024 || settings.ProgressUpdateSeconds > 0 &&
                        (DateTime.Now - lastUpdate).TotalSeconds >= settings.ProgressUpdateSeconds)
                    {
                        Console.Write($"{totalRead / 1024 / 1024} MiB, {totalRead / 1024 / 1024 / (DateTime.Now - start).TotalSeconds:0.0} MiB/s\r");
                        lastUpdate = DateTime.Now;
                    }
                }
                while (bytesRead == settings.BlockSizeKB * 1024);
            }

            Console.WriteLine();
        }

        private static void SaveHashes(byte[][] hashes, string hashFilePath)
        {
            var tmpFile = hashFilePath + ".tmp";
            using (var hashFile = File.OpenWrite(tmpFile))
                foreach (var hash in hashes)
                    hashFile.Write(hash, 0, hash.Length);

            File.Move(tmpFile, hashFilePath);
        }
    }

    public class Settings
    {
        public string DeviceFile;
        public int BlockSizeKB;
        public string BackupDir;
        public string FilePrefix;
        public double ProgressUpdateSeconds;
    }

    public class SourceBlock
    {
        public byte[] Bytes;
        public int Length;
        public byte[] Hash;
        public long BlockIndex;
    }
}
