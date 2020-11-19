using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Npgsql;
using System.Threading;
using System.Net.NetworkInformation;
using System.Net;
using System.Management;
using System.Diagnostics;
//using System.Windows.Forms;

namespace PostgreSlave
{
    public partial class MainWindow : Window
    {
        public const string DefaultDBConnectionString = ";User Id=postgres;Password=postgres;Database=";
        public string MasterConnectionString = ";User Id=postgres;Password=postgres;Database=";
        public string SlaveConnectionString = ";User Id=postgres;Password=postgres;Database=";
        public int CountOfTest = 10;
        bool fileBackup = false;
        public bool createTestTableFlag = false;
        
        public MainWindow()
        {
            InitializeComponent();
            Button_Click.IsEnabled = false;
            Slave_Monitor.IsEnabled = false;
        }

        public void Reconnect(string ip)
        {
            string caption = "Primary IP is busy!!!";
            string msg = "Основной IP занят! \nПроверьте, что на мастере выключен интерфейс. \nПосле освобождения IP, этому компьютеру будет автоматически присвоен основной адрес и слейв будет перезапущен как мастер.";

            MessageBox.Show(msg, caption, MessageBoxButton.OK, MessageBoxImage.Warning);
            //Form form1 = new Form();
            //form1.Text = "Primary IP is busy!!!";//Основной IP занят! Проверьте, что на мастере выключен интерфейс.";
            //System.Windows.Forms.TextBox mse = new System.Windows.Forms.TextBox();
            //mse.Text = "Основной IP занят! Проверьте, что на мастере выключен интерфейс. После освобождения IP этому компьютеру автоматически присоится основной адрес и слев будет перезапущен как мастер.";            
            //form1.Show();            

            while (true)
            {
                try
                {
                    Ping pingSender = new Ping();
                    IPAddress address = IPAddress.Parse(ip);
                    PingReply reply = pingSender.Send(address);
                    if (reply.Status == IPStatus.Success) { }
                    else break;
                }
                catch (Exception a)
                {
                    //a.ToString();
                    MessageBox.Show($"Incorrect IP: {ip}. Error: {a}");
                    //System.Windows.Forms.MessageBox.Show("Incorrect IP: " + IP + " . Error: " + a);                    
                }
            }
            //form1.Close();
        }

        private void Button_Click_Test(object sender, RoutedEventArgs e)
        {
            CheckIPsAvalaible();
            Running_task.Text = "";
            Error_List.Text = "";
            Program_State.Text = "Testing...";
            if (!fileBackup)
            {
                if (MessageBox.Show("Do you want backup your work files?", "", MessageBoxButton.YesNo, MessageBoxImage.Question) == MessageBoxResult.Yes)
                {
                    BackupSlaveSetting();
                }
                fileBackup = true;
            }                                    
            MasterConnectionString = $"Server={Master_IP.Text}{MasterConnectionString}{Testing_DB.Text};";     
            SlaveConnectionString = $"Server={Slave_IP.Text}{SlaveConnectionString}{Testing_DB.Text};";
            if (!createTestTableFlag)
            {
                CreateTestTable();
            }
            Slave_State.Text = TestSlave() ? "Slave" : "DOWN!";

            if (TestDB(SlaveConnectionString, "insert into testofcluster values ('1')") == 1)
            {
                Slave_State.Text = "Slave";
            }
            else
            {
                Replication_Slave_Status.Text = "No replication!";
                Slave_State.Text = "Master";
            }

            if (TestMaster()) 
            {
                Master_State.Text = "Master";
                NpgsqlConnection connectionToDB = new NpgsqlConnection(MasterConnectionString);
                connectionToDB.Open();
                NpgsqlCommand commandToExecute = new NpgsqlCommand("select (active) from pg_replication_slots", connectionToDB);
                NpgsqlDataReader data = commandToExecute.ExecuteReader();
                while (data.Read())
                {
                    if (data[0].ToString() == "True")
                    {
                        Replication_Master_Status.Text = "Working";
                    }
                    else
                    {
                        Replication_Master_Status.Text = "Didn't work!";
                    }
                }
                connectionToDB.Close();
            }
            else
            {
                string errorMessage = "Test-table 'testofcluster' on master-host is not avalible. Do you want to try to create it?";
                if (MessageBox.Show(errorMessage, "Eror", MessageBoxButton.YesNo, MessageBoxImage.Question) == MessageBoxResult.Yes)
                {
                    CreateTestTable();
                }
                Replication_Master_Status.Text = "DOWN!";
            }
            if ((Slave_State.Text == "Slave") && (Master_State.Text == "Master") && (Replication_Master_Status.Text == "Working")) 
            {
                Slave_Monitor.IsEnabled = true;
            }
            Program_State.Text = $"Done. ({CountOfTest})";
            MasterConnectionString = DefaultDBConnectionString;
            SlaveConnectionString = DefaultDBConnectionString;            
        }

        private void Button_Click_Monitoring(object sender, RoutedEventArgs e)
        {
            MasterConnectionString = $"Server={Master_IP.Text}{MasterConnectionString}{Testing_DB.Text};";            
            Program_State.Text = "Monitoring is running...";
            Running_task.Text = "";
            Error_List.Text = "";
            MessageBox.Show("После нажатия кнопки \"ОК\" и в случае если мастер будет недоступен, слейв перейдёт в режим записи.", "Внимание!", MessageBoxButton.OK, MessageBoxImage.Warning);
            while (true)
            {
                if (!TestMaster() ) 
                    break;
            }
            BecameSlaveToMaster();
            Program_State.Text = "Monitor has stopped.";
            DateTime localDate = DateTime.Now;
            string time = localDate.ToString();
            MessageBox.Show($"Slave started as master in {time}");
            MasterConnectionString = DefaultDBConnectionString;            
        }

        public bool CheckIP(string ip)
        {
            bool res;
            try
            {
                Ping pingSender = new Ping();
                IPAddress address = IPAddress.Parse(ip);
                PingReply reply = pingSender.Send(address);
                if (reply.Status == IPStatus.Success)
                {
                    res = true;
                }
                else
                {
                    Running_task.Text += $"(NO {ip}) ";                    
                    res = false;
                }
            }
            catch (Exception a)
            {
                a.ToString();
                MessageBox.Show($"Incorrect IP: {ip}");
                res = false;
            }
            return res;
        }

        public bool BecameSlaveToMaster()
        {
            Running_task.Text = $"Checking primary IP({Primary_IP.Text})... ";
            //сделать циклическую проверку
            string IP = Primary_IP.Text;
            Task.Factory.StartNew(() => Reconnect(IP));
            while (CheckIP(Primary_IP.Text) == true) ;//System.Windows.MessageBox.Show("Primary IP is still busy! Check you network connection.");             
            System.IO.File.WriteAllText(@"C:\Program Files\PostgreSQL\9.4\data\startmaster", "Go!");
            Running_task.Text = "Became slave to master...";
            ChangeIPto(Primary_IP.Text, "255.255.255.0");
            Button_Click.IsEnabled = false;
            Slave_Monitor.IsEnabled = false;
            return true;
        }

        public static IPAddress GetDefaultGateway()
        {
            var interFace = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault();
            if (interFace == null) return null;
            var addressDG = interFace.GetIPProperties().GatewayAddresses.FirstOrDefault();
            return addressDG.Address;
        }

        public void SetGateway(string gateway)
        {
            ManagementClass objMC = new ManagementClass("Win32_NetworkAdapterConfiguration");
            ManagementObjectCollection objMOC = objMC.GetInstances();
            foreach (ManagementObject objMO in objMOC)
            {
                if ((bool)objMO["IPEnabled"])
                {
                    try
                    {
                        ManagementBaseObject setGateway;
                        ManagementBaseObject newGateway = objMO.GetMethodParameters("SetGateways");
                        newGateway["DefaultIPGateway"] = new string[] { gateway };
                        newGateway["GatewayCostMetric"] = new int[] { 1 };

                        setGateway = objMO.InvokeMethod("SetGateways", newGateway, null);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
            }
        }
        
        public void ChangeIPto(string ipAddress, string subnetMask)
        {
            IPAddress GTW = GetDefaultGateway();
            ManagementClass MC = new ManagementClass("Win32_NetworkAdapterConfiguration");
            ManagementObjectCollection MOC = MC.GetInstances();
            foreach (ManagementObject MO in MOC)
            {
                if ((bool)MO["IPEnabled"])
                {                    
                    try
                    {                        
                        ManagementBaseObject setIP;
                        ManagementBaseObject newIP = MO.GetMethodParameters("EnableStatic");
                        newIP["IPAddress"] = new string[] { ipAddress };
                        newIP["SubnetMask"] = new string[] { subnetMask };
                        setIP = MO.InvokeMethod("EnableStatic", newIP, null);                        
                        SetGateway(GTW.ToString());
                    }
                    catch (Exception a)
                    {
                        System.Windows.MessageBox.Show(a.ToString());                                          
                    }
                }
            }
        }

        public bool TestSlave()
        {
            bool res = false;
            if (TestDB(SlaveConnectionString, "select count(*) from testofcluster") == 0)
                 res = true;
            return res;
        }

        public bool TestMaster()
        {
            bool res = false;
            if (TestDB(MasterConnectionString, "insert into testofcluster values ('1')") == 0)
            {
                res = true;
            }

            return res;
        }

        public bool CreateTestTable()
        {
            bool res;
            if (TestDB(MasterConnectionString, "create table testofcluster (hint int)") == 0)
            {
                createTestTableFlag = true;
                res = true;
            }
            else
            {
                MessageBox.Show($"Can't create test-table 'testofcluster' in database '{Testing_DB.Text}'.");
                res = false;
            }
            return res;
        }

        public int TestDB(string connectionString, string executeCommand)
        {
            int output;
            NpgsqlConnection connectionToDB = null;
            try
            {
                CountOfTest++;
                connectionToDB = new NpgsqlConnection(connectionString);
                connectionToDB.Open();
                NpgsqlCommand commandToExecute = new NpgsqlCommand(executeCommand, connectionToDB);
                commandToExecute.ExecuteScalar();
                if (CountOfTest > 200000)
                {
                    TestDB(connectionString, "delete from testofcluster");
                    CountOfTest = 0;
                }
                
                output = 0;
            }
            catch (Exception ex)
            {
                if (ex.Message == "ОШИБКА: 25006: в транзакции в режиме \"только чтение\" нельзя выполнить INSERT")
                {
                    output = 1;
                }
                else if (ex.Message == "ОШИБКА: 42P07: отношение \"testofcluster\" уже существует")
                {
                    output = 0;
                }
                else
                {
                    Running_task.Text += "Database isn't available! ";
                    Error_List.Text = ex.Message;
                    output = 2;
                }
            }
            finally
            {
                connectionToDB.Close();
            }

            return output;
        }

        public bool BackupSlaveSetting()
        {
            string directoryPath = @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting";
            bool res;
            try
            {
                if (Directory.Exists(directoryPath))
                {
                    Error_List.Text = "Backup exists already (backupSlaveSetting). Delete it to proved new save.";
                    res = false;
                }
                else
                {
                    DirectoryInfo dir = Directory.CreateDirectory(directoryPath);            
                    File.Copy(@"C:\Program Files\PostgreSQL\9.4\data\recovery.conf", @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting\recovery.conf");
                    File.Copy(@"C:\Program Files\PostgreSQL\9.4\data\postgresql.conf", @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting\postgresql.conf");
                    File.Copy(@"C:\Program Files\PostgreSQL\9.4\data\postgresql.auto.conf", @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting\postgresql.auto.conf");
                    res = true;
                }
            }
            catch (Exception ex)
            {
                Error_List.Text = $"Backup is failed: {ex}";
                res =  false;
            }

            return res;
        }

        public void CheckIPsAvalaible()
        {
            CheckIP(Slave_IP.Text);
            CheckIP(Master_IP.Text);
            CheckIP(Primary_IP.Text);
        }
        private void IP_TextChanged(object sender, TextChangedEventArgs e)
        {
            Button_Click.IsEnabled = (Slave_IP.Text != "") && (Master_IP.Text != "") && (Primary_IP.Text != "") && (Testing_DB.Text != "");
        }
    }
}
