using System.Diagnostics;

namespace ThreadRunner
{
    public class ThreadInfo
    {
        public string ThreadId { get; set; }
        public int RunTimeSeconds { get; set; }
        public string BlockingThread { get; set; }
    }

    public class ParallelThreadRunner
    {
        private static readonly object consoleLock = new object();
        private static Dictionary<string, Task> runningTasks = new Dictionary<string, Task>();
        private static Dictionary<string, TaskCompletionSource<bool>> taskCompletionSources = 
            new Dictionary<string, TaskCompletionSource<bool>>();

        static async Task Main(string[] args)
        {
            Console.WriteLine("Parallel Thread Runner");
            Console.WriteLine("======================");
            
            string csvFilePath = "threads.csv"; // Default filename
            
            if (args.Length > 0)
            {
                csvFilePath = args[0];
            }

            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"Error: CSV file '{csvFilePath}' not found.");
                return;
            }

            try
            {
                var threadInfos = ReadThreadsFromCsv(csvFilePath);
                
                if (threadInfos.Count == 0)
                {
                    Console.WriteLine("No thread information found in the CSV file.");
                    return;
                }

                var stopwatch = Stopwatch.StartNew();
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Starting parallel thread execution...");
                Console.WriteLine();

                // Initialize task completion sources for all threads
                foreach (var threadInfo in threadInfos)
                {
                    taskCompletionSources[threadInfo.ThreadId] = new TaskCompletionSource<bool>();
                }

                // Start all threads
                var tasks = threadInfos.Select(threadInfo => StartThreadAsync(threadInfo)).ToArray();
                
                // Wait for all threads to complete
                await Task.WhenAll(tasks);
                
                stopwatch.Stop();
                
                Console.WriteLine();
                Console.WriteLine($"Program Completed. Run time: {stopwatch.Elapsed.TotalSeconds:F3} seconds");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        static List<ThreadInfo> ReadThreadsFromCsv(string filePath)
        {
            var threadInfos = new List<ThreadInfo>();
            
            Console.WriteLine($"Reading from: {Path.GetFullPath(filePath)}");
            
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File does not exist at: {Path.GetFullPath(filePath)}");
                return threadInfos;
            }
            
            var lines = File.ReadAllLines(filePath);
            Console.WriteLine($"Read {lines.Length} line(s) from file");
            
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                Console.WriteLine($"Line {i + 1}: '{line}'");
                
                if (string.IsNullOrWhiteSpace(line) || line.StartsWith("#"))
                {
                    Console.WriteLine($"  -> Skipped (empty or comment)");
                    continue;
                }
                
                var parts = line.Split(',');
                Console.WriteLine($"  -> Split into {parts.Length} parts: [{string.Join("] [", parts)}]");
                
                if (parts.Length >= 2)
                {
                    try
                    {
                        var threadInfo = new ThreadInfo
                        {
                            ThreadId = parts[0].Trim(),
                            RunTimeSeconds = int.Parse(parts[1].Trim()),
                            BlockingThread = parts.Length > 2 ? parts[2].Trim() : string.Empty
                        };
                        
                        // If BlockingThread is empty string, set to null for consistency
                        if (string.IsNullOrWhiteSpace(threadInfo.BlockingThread))
                            threadInfo.BlockingThread = null;
                        
                        threadInfos.Add(threadInfo);
                        Console.WriteLine($"  -> Added thread: {threadInfo.ThreadId}, {threadInfo.RunTimeSeconds}s, blocking: {threadInfo.BlockingThread ?? "None"}");
                    }
                    catch (FormatException ex)
                    {
                        Console.WriteLine($"  -> Warning: Invalid format in line: {line} - {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine($"  -> Skipped: Not enough parts (need at least 2, got {parts.Length})");
                }
            }
            
            return threadInfos;
        }

        static async Task StartThreadAsync(ThreadInfo threadInfo)
        {
            try
            {
                // Wait for blocking thread if specified
                if (!string.IsNullOrEmpty(threadInfo.BlockingThread))
                {
                    if (taskCompletionSources.ContainsKey(threadInfo.BlockingThread))
                    {
                        LogMessage($"Thread {threadInfo.ThreadId} waiting for {threadInfo.BlockingThread} to complete...");
                        await taskCompletionSources[threadInfo.BlockingThread].Task;
                    }
                    else
                    {
                        LogMessage($"Warning: Blocking thread {threadInfo.BlockingThread} not found for {threadInfo.ThreadId}");
                    }
                }

                // Start the thread
                LogMessage($"Thread {threadInfo.ThreadId} Starting");
                
                // Simulate work with Thread.Sleep wrapped in Task.Run to avoid blocking the async context
                await Task.Run(() => Thread.Sleep(threadInfo.RunTimeSeconds * 1000));
                
                // Thread completed
                LogMessage($"Thread {threadInfo.ThreadId} Completed");
                
                // Signal completion to any waiting threads
                if (taskCompletionSources.ContainsKey(threadInfo.ThreadId))
                {
                    taskCompletionSources[threadInfo.ThreadId].SetResult(true);
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Thread {threadInfo.ThreadId} ERROR: {ex.Message}");
                
                // Signal completion even on error to prevent deadlocks
                if (taskCompletionSources.ContainsKey(threadInfo.ThreadId))
                {
                    taskCompletionSources[threadInfo.ThreadId].SetException(ex);
                }
            }
        }

        static void LogMessage(string message)
        {
            //Prevents multiple threads from writing to the console simultaneously
            lock (consoleLock)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
            }
        }
    }
}