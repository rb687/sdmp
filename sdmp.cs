using System;
using SIG.SDMP.Messaging.FileIO;
using SIG.SDMP.Messaging;
using SIG.SDMP.Messaging.Rv;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace sdmp_test_2012
{
    public class Program
    {
        static void Main(string[] args)
        {
            string dirPath = @"C:\Users\bhartia\Desktop\CAS_sample_files";

            string program_start_time = "04:15 AM";
            DateTime dt_program_start_time = Convert.ToDateTime(program_start_time);

            string timeframe_start_time = "11:29 AM";

            string timeframe_end_time = "4:30 PM";

            string[] dirFiles = Directory.GetFiles(dirPath); // Gets all the files in the provided directory and returns it in an array of string

            int program_started = 0;

            Console.WriteLine("Program start time set by user: " + program_start_time + "\n");

            TimeSpan time_wait = dt_program_start_time.Subtract(DateTime.Now);
            Task.Delay(time_wait.Duration());

            do
            {
                DateTime today_date = DateTime.Now;
                string time_program_started = today_date.ToShortTimeString();
                Console.WriteLine("Time program started (Wall clock): " + time_program_started);

                if (DateTime.Parse(time_program_started) >= DateTime.Parse(program_start_time))
                // current time > program start time 
                {
                    program_started++; // Keeping a check if the start time reached or not

                    CancellationTokenSource cts = new CancellationTokenSource();
                    ParallelOptions po = new ParallelOptions();
                    po.CancellationToken = cts.Token;

                    Task.Factory.StartNew(() =>
                    {
                        if (Console.ReadKey().KeyChar == 'c')
                        {
                            cts.Cancel();
                        }

                        Console.WriteLine("Press any key to exit");
                    });
                    try
                    {
                        Parallel.ForEach(dirFiles,
                                         (item) =>
                                         {
                                             Read_and_publish(item, timeframe_start_time, timeframe_end_time);
                                             Console.WriteLine("A new thread started with Thread ID");
                                             Console.WriteLine("{0}", Thread.CurrentThread.ManagedThreadId);
                                             po.CancellationToken.ThrowIfCancellationRequested();
                                         });
                    }
                    catch (OperationCanceledException e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
                else
                {
                    Task.Delay(1000);
                    // Makes the application sleep for 1 sec (value is in milliseconds)
                }
            } while (program_started == 0);

            Console.WriteLine("Job Done. Press any key to quit...");
            Console.ReadKey();
        }//main

        private static void Read_and_publish(string filename, string start_tstamp, string end_tstamp)
        {
            DateTime dt_start_tstamp = Convert.ToDateTime(start_tstamp); // Start time given by user 
            DateTime dt_end_tstamp = Convert.ToDateTime(end_tstamp); // End time given by user
            DateTime dt_virtual_clock = Convert.ToDateTime(start_tstamp);

            Console.WriteLine("Filename: " + filename);
            Console.WriteLine("Timeframe start time: " + start_tstamp);
            Console.WriteLine("Timeframe end time: " + end_tstamp);
            Console.WriteLine("Virtual Clock: " + dt_virtual_clock);

            StreamReader freader = new StreamReader(filename);
            string line = freader.ReadLine();

            using (ISyncMessageReader reader = MessageFileUtil.GetReaderFromFilePath(filename))
            {
                foreach (IMessageInfo messageInfo in reader.ReadMessages())
                {
                    var pdu = messageInfo.GetProtocolDataUnit();
                    if (pdu.header.receiver_timestampSpecified == true)
                    {
                        long tstamp = pdu.header.receiver_timestamp;
                        DateTime dt_msg_tstamp = SIG.SDMP.Common.DateTimeConvert.FromUnixTimestamp(tstamp); // Date and time the message was received 

                        string msg_time = dt_msg_tstamp.ToShortTimeString(); // Gets the time from the date 
                        DateTime dt_today_msg_tstamp = Convert.ToDateTime(msg_time); // msg time with today's date 

                        /*string start_time = dt_start_tstamp.ToShortTimeString(); // Gets the start time of timeframe 
                        string end_time = dt_end_tstamp.ToShortTimeString(); // Gets the end time of timeframe 
                        string virtual_clock = dt_virtual_clock.ToShortTimeString(); // Gets the virtual clock time 
                        */

                        TimeSpan time_wait = dt_today_msg_tstamp.Subtract(dt_virtual_clock);
                        int wait_in_secs = Math.Abs(((int)time_wait.TotalSeconds) * 1000);
                        Console.WriteLine("Current Virtual Clock time: " + dt_virtual_clock);
                        Console.WriteLine("Next msg at Virtual Clock: " + dt_virtual_clock.Add(time_wait));
                        DateTime today_date = DateTime.Now;
                        Console.WriteLine("Next msg at wall Clock: " + today_date.Add(time_wait));
                        //  System.Threading.Thread.Sleep(wait_in_secs);

                        Console.WriteLine("Virtual Clock: " + dt_virtual_clock);

                        dt_virtual_clock = dt_virtual_clock.Add(time_wait); // new virtual clock time updated

                        if ((DateTime.Compare(dt_today_msg_tstamp, dt_start_tstamp) >= 0) && (DateTime.Compare(dt_today_msg_tstamp, dt_end_tstamp) <= 0))
                        {
                            //string channelName, string environment, string source, string configFilePath);
                            SdmpBusApiRvWriter writer = new SdmpBusApiRvWriter("SDMP_TEST", "DEV", "2.2", @"C:\Users\bhartia\Desktop\CAS_send_config.xml");
                            writer.Write(messageInfo);
                            Console.WriteLine("Message written to RV from file: " + filename);
                        }
                        else
                        {
                            string msg = "Not available for time virtual clock";
                            Console.WriteLine(msg);
                        }

                        /*
                        DateTime? sentOrReceived = null;
                        if (pdu.header.receiver_timestampSpecified)
                            sentOrReceived = SIG.SDMP.Common.DateTimeConvert.FromUnixTimestamp(pdu.header.receiver_timestamp);
                        else if (pdu.header.sender_timestampSpecified)
                            sentOrReceived = SIG.SDMP.Common.DateTimeConvert.FromUnixTimestamp(pdu.header.sender_timestamp); 
                        */
                    }
                    else
                    {
                        Console.WriteLine("Time not true");
                    }


                } //for each IMessageInfo
            }//using  
        }
    }//class

}//namespace
