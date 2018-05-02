using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageCopier
{
    class Program
    {
        private static string _azCopyPath;
        private static CloudTableClient _sourceTableClient;
        private static CloudTableClient _destinationTableClient;
        private static string _continueFrom;
        private static List<string> _inclusionTableList;
        private static List<string> _exclusionTableList;
        private static string _path;
        private static int _exportNc;
        private static int _importNc;

        static void Main(string[] args)
        {
            try
            {
                // validate args
                ValidateArguments(args);

                // get inclusion tables
                if (_inclusionTableList == null)
                {
                    // get all tables in alphabetical order
                    _inclusionTableList = _sourceTableClient
                        .ListTables()
                        .Select(o => o.Name)
                        .OrderBy(o => o)
                        .ToList();
                }

                // exclude tables prior to continue from
                if (!string.IsNullOrWhiteSpace(_continueFrom))
                {
                    _inclusionTableList.RemoveAll(o => string.CompareOrdinal(o, _continueFrom) < 0);
                }

                // exclude provided list
                _inclusionTableList = _inclusionTableList.Except(_exclusionTableList).ToList();

                // exclude Azure metrics tables
                _inclusionTableList.RemoveAll(o => o.StartsWith("$Metrics"));

                // exclude Azure diagnostics tables
                _inclusionTableList.RemoveAll(o => o.StartsWith("WAD"));

                // delete tables to sync
                __("Deleting tables");
                var deleteTaskList = _inclusionTableList.Select(tableName =>
                {
                    var destinationTable = _destinationTableClient.GetTableReference(tableName);
                    return destinationTable.DeleteIfExistsAsync();
                }).ToList();

                // wait for completion
                while (deleteTaskList.Any(o => o.Status < TaskStatus.RanToCompletion))
                {
                    Thread.Sleep(500);
                }

                // re-create tables
                __("Re-creating tables");
                new Thread(StartAnimation).Start();
                var tablesReadyList = new ConcurrentBag<string>();
                _animationProgress = 0.ToString("p");
                _inclusionTableList.AsParallel().ForAll(tableName =>
                {
                    var success = false;
                    do
                    {
                        try
                        {
                            // attempt creation
                            var destinationTable = _destinationTableClient.GetTableReference(tableName);
                            destinationTable.CreateIfNotExists();

                            // we're good, refresh progress
                            tablesReadyList.Add(tableName);
                            var progress = 1 -
                                           _inclusionTableList.Except(tablesReadyList).Count()/
                                           (double) _inclusionTableList.Count;
                            _animationProgress = progress.ToString("p");

                            success = true;
                        }
                        catch (StorageException ex)
                        {
                            if (ex.Message.Contains("409"))
                            {
                                // a table couldn't be recreated, wait and attempt again
                                Thread.Sleep(10000);
                            }
                            else
                                throw;
                        }
                    } while (!success);
                });
                _isAnimationCancelling = true;

                _inclusionTableList.ForEach(tableName =>
                {
                    // get table references
                    var sourceTable = _sourceTableClient.GetTableReference(tableName);
                    var destinationTable = _destinationTableClient.GetTableReference(tableName);

                    CopyTables(sourceTable, destinationTable);
                });

                Console.Write("Done. Press any key to exit...");
                Console.ReadKey();
                return;

                // get all tables
                var sourceTableList = _sourceTableClient.ListTables().ToList();
                var destinationTableList = _destinationTableClient.ListTables().ToList();

                // synchronize tables between source and destination
                SyncDestinationTables(sourceTableList, ref destinationTableList);

                // clear existing destination tables one by one and repopulate from source
                for (var i = 55; i < destinationTableList.Count; i++)
                {
                    var destinationTable = destinationTableList[i];
                    __("----- {0} ({1} of {2})-----", destinationTable.Name, i + 1, destinationTableList.Count);

                    // Initialize a default TableQuery to retrieve all the entities in the table.
                    TableQuery<DynamicTableEntity> tableQuery = new TableQuery<DynamicTableEntity>
                    {
                        SelectColumns = new List<string>()
                    };

                    // Initialize the continuation token to null to start from the beginning of the table.
                    TableContinuationToken continuationToken = null;

                    do
                    {
                        // Retrieve a segment (up to 1,000 entities).
                        TableQuerySegment<DynamicTableEntity> tableQueryResult =
                            destinationTable.ExecuteQuerySegmented(tableQuery, continuationToken);

                        // Assign the new continuation token to tell the service where to
                        // continue on the next iteration (or null if it has reached the end).
                        continuationToken = tableQueryResult.ContinuationToken;

                        // Print the number of rows retrieved.
                        Console.WriteLine("Rows retrieved {0}", tableQueryResult.Results.Count);

                        // Loop until a null continuation token is received, indicating the end of the table.
                    } while (continuationToken != null);

                    // delete all table rows and recieve initial record count to decided if table needs to be copied
                    var initialRecordCount = TruncateTable(destinationTable);
                    if (initialRecordCount > 0)
                    {
                        // find matching source table and use AzCopy to move data from it to the destination
                        var sourceTable = sourceTableList.First(o => o.Name == destinationTable.Name);
                        CopyTables(sourceTable, destinationTable);
                    }

                    Console.Clear();
                }

                __("Complete");
                Console.ReadKey();
            }
            catch (Exception ex)
            {
                __(ex.Message);
                throw;
            }
        }

        static void ValidateArguments(string[] args)
        {
            // validate AzCopy existence
            Console.Write("AzCopy path: ");
            _azCopyPath = Console.ReadLine();
            if (!File.Exists(_azCopyPath))
                throw new Exception("Couldn't find AzCopy under specified path.");

            // connect to source
            Console.Write("Source: ");
            var sourceConnectionString = Console.ReadLine();
            try
            {
                var storageAccount = CloudStorageAccount.Parse(sourceConnectionString);
                _sourceTableClient = storageAccount.CreateCloudTableClient();
            }
            catch (Exception)
            {
                throw new Exception("Couldn't connect to source.");
            }

            // connect to destination
            Console.Write("Destination: ");
            var destinationConnectionString = Console.ReadLine();
            try
            {
                var storageAccount = CloudStorageAccount.Parse(destinationConnectionString);
                _destinationTableClient = storageAccount.CreateCloudTableClient();
            }
            catch (Exception)
            {
                throw new Exception("Couldn't connect to destination.");
            }

            // parse continue from table name
            Console.Write("Continue from: ");
            _continueFrom = Console.ReadLine();

            // parse inclusion list
            Console.Write("Inclusion table list: ");
            var inclusionList = Console.ReadLine();
            try
            {
                _inclusionTableList = string.IsNullOrWhiteSpace(inclusionList)
                    ? null
                    : inclusionList.Split(new[] {";"}, StringSplitOptions.RemoveEmptyEntries).ToList();
            }
            catch (Exception)
            {
                throw new Exception(
                    "Couldn't parse inclusion list. Remember to split by semi-colon, use capitalization, and omit spaces.");
            }

            // parse exclusion list
            Console.Write("Exclusion table list: ");
            var exclusionList = Console.ReadLine();
            try
            {
                _exclusionTableList = string.IsNullOrWhiteSpace(exclusionList)
                    ? new List<string>()
                    : exclusionList
                        .Split(new[] {",", ";", " "}, StringSplitOptions.RemoveEmptyEntries)
                        .ToList();
            }
            catch (Exception)
            {
                throw new Exception(
                    "Couldn't parse exclusion list. Remember to split by semi-colon, use capitalization, and omit spaces.");
            }

            // parse export NC
            Console.Write("Export NC: ");
            if (!int.TryParse(Console.ReadLine(), out _exportNc))
                _exportNc = 128;

            // parse import NC
            Console.Write("Import NC: ");
            if (!int.TryParse(Console.ReadLine(), out _importNc))
                _importNc = 32;

            // parse path
            Console.Write("Temp directory path: ");
            var path = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(path))
                _path = Path.GetTempPath();
            else
            {
                if (!Directory.Exists(path))
                    throw new Exception("Temp directory does not exist.");

                _path = path;
            }
        }

        static void SyncDestinationTables(List<CloudTable> sourceTableList, ref List<CloudTable> destinationTableList)
        {
            // delete all tables in destination that do not exist in source
            foreach (var destinationTable in destinationTableList)
                if (sourceTableList.All(o => o.Name != destinationTable.Name))
                {
                    __("Deleting destination table \"{0}\"", destinationTable.Name);
                    destinationTable.Delete();
                }

            // create all tables in destination that exist in source only
            foreach (var sourceTable in sourceTableList)
                if (destinationTableList.All(o => o.Name != sourceTable.Name))
                {
                    __("Creating destination table \"{0}\"", sourceTable.Name);
                    var destinationTable = _destinationTableClient.GetTableReference(sourceTable.Name);
                    destinationTable.CreateIfNotExists();
                }

            // refresh destination tables
            destinationTableList = _destinationTableClient.ListTables().ToList();
        }

        private static int TruncateTable(CloudTable destinationTable)
        {
            // get all destination rows sorted by keys for faster operations
            __("Reading table...");
            var query = new TableQuery {SelectColumns = new List<string>()};
            var destinationEntityList = destinationTable.ExecuteQuery(query).ToList();
            var destinationEntityGroups = destinationEntityList
                .GroupBy(o => o.PartitionKey)
                .ToList();

            __("Got {0} entities partitioned into {1} groups",
                destinationEntityList.Count, destinationEntityGroups.Count);

            // delete by batches of 100 / on PK change
            var batchOperation = new TableBatchOperation();
            var batchOperationList = new List<TableBatchOperation>();
            foreach (var entityGroup in destinationEntityGroups)
            {
                // enumerate group
                var entities = entityGroup.ToList();

                // work with 100 operations at a time
                foreach (var entity in entities)
                {
                    if (batchOperation.Any() && batchOperation.Count%100 == 0)
                    {
                        // add and reset batch
                        batchOperationList.Add(batchOperation);
                        batchOperation = new TableBatchOperation();
                    }

                    // add operation to batch
                    batchOperation.Delete(entity);
                }

                // add last batch
                if (batchOperation.Count > 0)
                {
                    batchOperationList.Add(batchOperation);
                    batchOperation = new TableBatchOperation();
                }
            }

            // process batches
            var batchTaskList = batchOperationList
                .Select(destinationTable.ExecuteBatchAsync)
                .Cast<Task>()
                .ToList();

            // wait for all operations to complete
            __("Executing {0} delete batches", batchTaskList.Count);
            while (batchTaskList.Count(o => !o.IsCompleted) > 0)
                Thread.Sleep(250);

            return destinationEntityList.Count;
        }

        private static void CopyTables(CloudTable sourceTable, CloudTable destinationTable)
        {
            // declare commands
            var manifestFileName = $"{sourceTable.Name}.manifest";
            var tempFolderPath = Path.Combine(_path, Path.GetRandomFileName());

            var sourceUrl = sourceTable.Uri.ToString();
            var sourceKey = _sourceTableClient.Credentials.ExportBase64EncodedKey();

            var destinationUrl = destinationTable.Uri.ToString();
            var destinationKey = _destinationTableClient.Credentials.ExportBase64EncodedKey();

            var exportCommand = "" +
                                $"/Source:\"{sourceUrl}\" " +
                                $"/SourceKey:\"{sourceKey}\" " +
                                $"/Dest:\"{tempFolderPath}\" " +
                                $"/Z:\"{tempFolderPath}\" " +
                                $"/Manifest:\"{manifestFileName}\" " +
                                "/NC:128";

            var importCommand = "" +
                                $"/Source:\"{tempFolderPath}\" " +
                                $"/Dest:\"{destinationUrl}\" " +
                                $"/DestKey:\"{destinationKey}\" " +
                                $"/DestType:\"Table\" " +
                                $"/Z:\"{tempFolderPath}\" " +
                                $"/Manifest:\"{manifestFileName}\" " +
                                "/EntityOperation:InsertOrReplace " +
                                "/NC:32";

            // create temp directory to contain AzCopy manifest and table data
            Directory.CreateDirectory(tempFolderPath);

            // run export process
            RunProcess(exportCommand);

            // run import process
            RunProcess(importCommand);

            // delete temp directory
            Directory.Delete(tempFolderPath, true);
        }

        private static void RunProcess(string command)
        {
            // declare process
            var startInfo = new ProcessStartInfo(_azCopyPath)
            {
                Arguments = command,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                UseShellExecute = false
            };
            var process = Process.Start(startInfo);

            // read the process's output
            while (!process.StandardOutput.EndOfStream || !process.StandardError.EndOfStream)
            {
                // output to user
                if (!process.StandardOutput.EndOfStream)
                    Console.WriteLine(process.StandardOutput.ReadLine());
                else
                    Console.WriteLine(process.StandardError.ReadLine());
            }
            process.WaitForExit();
        }

        private static void _(string message, params object[] args)
        {
            // format message
            message = string.Format(message, args);

            // output
            Console.Write(message);
        }

        private static void __(string message, params object[] args)
        {
            // format message
            message = string.Format(message, args);
            message = $"[{DateTime.Now.ToString("G")}] {message}";

            // output
            Console.WriteLine(message);
        }

        private static readonly string[] AnimationList = {"/", "—", "\\", "|"};
        private static string _animationProgress;
        private static bool _isAnimationCancelling;

        private static void StartAnimation()
        {
            var animationIndex = 0;
            do
            {
                // save curson position
                var currentLineCursor = Console.CursorTop;

                if (animationIndex > 3)
                    animationIndex = 0;
                var animationSequence = AnimationList[animationIndex];
                animationIndex++;

                // output
                if (!string.IsNullOrWhiteSpace(_animationProgress))
                    __("{0} {1}", _animationProgress, animationSequence);
                else
                    __("{0}", animationSequence);

                if (_isAnimationCancelling) continue;

                // reset curson position
                Console.SetCursorPosition(0, currentLineCursor);

                // wait
                Thread.Sleep(250);

            } while (!_isAnimationCancelling);

            Console.WriteLine();
        }
    }
}

/*
    // check if waiting for input
    if (!process.HasExited)
        foreach (ProcessThread thread in process.Threads)
            if (thread.ThreadState == ThreadState.Wait &&
                thread.WaitReason == ThreadWaitReason.UserRequest)
            {
                var userResponse = Console.ReadLine();
                process.StandardInput.WriteLine(userResponse);
            }
*/
