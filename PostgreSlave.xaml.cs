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
    enum NodeToTest
    {
        Master,
        Slave
    }

    public enum DBTestResult
    {
        Success,
        ReadOnly,
        NoDB
    }
    public partial class MainWindow : Window
    {
        const NodeToTest testMaster = NodeToTest.Master;
        const NodeToTest testSlave = NodeToTest.Slave;
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
            Slave_State.Text = TestNode(testSlave) ? "Slave" : "DOWN!";

            if (TestDB(SlaveConnectionString, "insert into testofcluster values ('1')") == DBTestResult.ReadOnly)
            {
                Slave_State.Text = "Slave";
            }
            else
            {
                Replication_Slave_Status.Text = "No replication!";
                Slave_State.Text = "Master";
            }

            if (TestNode(testMaster)) 
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

            Slave_Monitor.IsEnabled = (Slave_State.Text == "Slave") && (Master_State.Text == "Master") && (Replication_Master_Status.Text == "Working");

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
                if (!TestNode(testMaster)) 
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
            bool output;
            try
            {
                Ping pingSender = new Ping();
                IPAddress address = IPAddress.Parse(ip);
                PingReply reply = pingSender.Send(address);
                if (reply.Status == IPStatus.Success)
                {
                    output = true;
                }
                else
                {
                    Running_task.Text += $"(NO {ip}) ";                    
                    output = false;
                }
            }
            catch (Exception a)
            {
                a.ToString();
                MessageBox.Show($"Incorrect IP: {ip}");
                output = false;
            }
            return output;
        }

        public bool BecameSlaveToMaster()
        {
            Running_task.Text = $"Checking primary IP({Primary_IP.Text})... ";
            //сделать циклическую проверку
            string ip = Primary_IP.Text;
            Task.Factory.StartNew(() => Reconnect(ip));
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
            var @interface = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault();
            if (@interface == null) return null;
            var addressDG = @interface.GetIPProperties().GatewayAddresses.FirstOrDefault();
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

        private bool TestNode(NodeToTest nodeToTest)
        {
            bool output = false;
            switch (nodeToTest)
            {
                case testMaster:
                    if (TestDB(SlaveConnectionString, "select count(*) from testofcluster") == DBTestResult.Success)
                        output = true;
                    break;
                case testSlave:
                    if (TestDB(SlaveConnectionString, "select count(*) from testofcluster") == DBTestResult.Success)
                        output = true;
                    break;
            }

            return output;
        }
       
        public bool CreateTestTable()
        {
            bool output;
            if (TestDB(MasterConnectionString, "create table testofcluster (hint int)") == DBTestResult.Success)
            {
                createTestTableFlag = true;
                output = true;
            }
            else
            {
                MessageBox.Show($"Can't create test-table 'testofcluster' in database '{Testing_DB.Text}'.");
                output = false;
            }
            return output;
        }

        public DBTestResult TestDB(string connectionString, string executeCommand)
        {
            DBTestResult output;
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
                
                output = DBTestResult.Success;
            }
            catch (Exception ex)
            {
                if (ex.Message == "ОШИБКА: 25006: в транзакции в режиме \"только чтение\" нельзя выполнить INSERT")
                {
                    output = DBTestResult.ReadOnly;
                }
                else if (ex.Message == "ОШИБКА: 42P07: отношение \"testofcluster\" уже существует")
                {
                    output = DBTestResult.Success;
                }
                else
                {
                    Running_task.Text += "Database isn't available! ";
                    Error_List.Text = ex.Message;
                    output = DBTestResult.NoDB;
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
            bool output;
            try
            {
                if (Directory.Exists(directoryPath))
                {
                    Error_List.Text = "Backup exists already (backupSlaveSetting). Delete it to proved new save.";
                    output = false;
                }
                else
                {
                    DirectoryInfo dir = Directory.CreateDirectory(directoryPath);            
                    File.Copy(@"C:\Program Files\PostgreSQL\9.4\data\recovery.conf", @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting\recovery.conf");
                    File.Copy(@"C:\Program Files\PostgreSQL\9.4\data\postgresql.conf", @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting\postgresql.conf");
                    File.Copy(@"C:\Program Files\PostgreSQL\9.4\data\postgresql.auto.conf", @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting\postgresql.auto.conf");
                    output = true;
                }
            }
            catch (Exception ex)
            {
                Error_List.Text = $"Backup is failed: {ex}";
                output =  false;
            }

            return output;
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
