using Npgsql;
using System;
using System.IO;
using System.Linq;
using System.Management;
using System.Net;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
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
        const string psqlDataDir = @"C:\Program Files\PostgreSQL\9.4\data";
        const string slaveBackupDir = @"C:\Program Files\PostgreSQL\9.4\backupSlaveSetting";

        public string MasterConnectionString = ";User Id=postgres;Password=postgres;Database=";
        public string SlaveConnectionString = ";User Id=postgres;Password=postgres;Database=";
        public int CountOfTest = 10;
        public bool createTestTableFlag = false;

        public MainWindow()
        {
            InitializeComponent();
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

                    if (reply.Status != IPStatus.Success) break;
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
            if (CheckIP(Slave_IP.Text) && CheckIP(Master_IP.Text) && CheckIP(Primary_IP.Text))
            {
                Running_task.Text = "";
                Error_List.Text = "";
                Program_State.Text = "Testing...";

                if (MessageBox.Show("Do you want backup your work files?", "", MessageBoxButton.YesNo, MessageBoxImage.Question)
                    == MessageBoxResult.Yes)
                {
                    BackupSlaveSetting();
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
                    if (MessageBox.Show(errorMessage, "Error", MessageBoxButton.YesNo, MessageBoxImage.Question) == MessageBoxResult.Yes)
                    {
                        CreateTestTable();
                    }
                    Replication_Master_Status.Text = "DOWN!";
                }

                SlaveMonitorButton.IsEnabled = (Slave_State.Text == "Slave") && (Master_State.Text == "Master") && (Replication_Master_Status.Text == "Working");

                Program_State.Text = $"Done. ({CountOfTest})";
                MasterConnectionString = DefaultDBConnectionString;
                SlaveConnectionString = DefaultDBConnectionString;
            }
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
                if (!TestNode(testMaster)) break;
            }
            BecameSlaveToMaster();
            Program_State.Text = "Monitor has stopped.";
            MessageBox.Show($"Slave started as master in {DateTime.Now}");
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
            catch (Exception ex)
            {
                //a.ToString();
                MessageBox.Show($"Incorrect IP: {ip}");
                output = false;
            }
            return output;
        }

        public void BecameSlaveToMaster()
        {
            Running_task.Text = $"Checking primary IP({Primary_IP.Text})... ";
            //сделать циклическую проверку
            string ip = Primary_IP.Text;
            Task.Factory.StartNew(() => Reconnect(ip));
            while (CheckIP(Primary_IP.Text))
            {
                ;//System.Windows.MessageBox.Show("Primary IP is still busy! Check you network connection.");             
            }

            File.WriteAllText($@"{psqlDataDir}\startmaster", "Go!");
            Running_task.Text = "Became slave to master...";
            ChangeIPto(Primary_IP.Text, "255.255.255.0");
            TestButton.IsEnabled = false;
            SlaveMonitorButton.IsEnabled = false;
        }

        public static IPAddress GetDefaultGateway()
        {
            IPAddress output = null;
            var @interface = NetworkInterface.GetAllNetworkInterfaces().
                Where(n => n.OperationalStatus == OperationalStatus.Up).FirstOrDefault();
            
            if (@interface != null)
            {
                output = @interface.GetIPProperties().GatewayAddresses.FirstOrDefault()?.Address;
            }
            return output;
        }

        public void SetGateway(IPAddress gw)
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
                        newGateway["DefaultIPGateway"] = new IPAddress[] { gw };
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
            IPAddress gw = GetDefaultGateway();
            ManagementClass objMC = new ManagementClass("Win32_NetworkAdapterConfiguration");
            ManagementObjectCollection objMOC = objMC.GetInstances();
            foreach (ManagementObject objMO in objMOC)
            {
                if ((bool)objMO["IPEnabled"])
                {
                    try
                    {
                        ManagementBaseObject setIP;
                        ManagementBaseObject newIP = objMO.GetMethodParameters("EnableStatic");
                        newIP["IPAddress"] = new string[] { ipAddress };
                        newIP["SubnetMask"] = new string[] { subnetMask };
                        setIP = objMO.InvokeMethod("EnableStatic", newIP, null);
                        SetGateway(gw);
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

        public void CreateTestTable()
        {
            if (TestDB(MasterConnectionString, "create table testofcluster (hint int)") == DBTestResult.Success)
            {
                createTestTableFlag = true;
            }
            else
            {
                MessageBox.Show($"Can't create test-table 'testofcluster' in database '{Testing_DB.Text}'.");
            }
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
                //if (ex.Message == "ОШИБКА: 25006: в транзакции в режиме \"только чтение\" нельзя выполнить INSERT")
                if (ex.Message.Contains("25006")) //ReadOnly
                {
                    output = DBTestResult.ReadOnly;
                }
                //else if (ex.Message == "ОШИБКА: 42P07: отношение \"testofcluster\" уже существует")
                else if (ex.Message.Contains("42P07")) // Duplicate table
                {
                    output = DBTestResult.Success; //very strange
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

        public void BackupSlaveSetting()
        {
            try
            {
                if (Directory.Exists(slaveBackupDir))
                {
                    Error_List.Text = "Backup exists already (backupSlaveSetting). Delete it to provide new save.";
                }
                else
                {
                    DirectoryInfo dir = Directory.CreateDirectory(slaveBackupDir);
                    File.Copy($@"{psqlDataDir}\recovery.conf", $@"{slaveBackupDir}\recovery.conf");
                    File.Copy($@"{psqlDataDir}\postgresql.conf", $@"{slaveBackupDir}\postgresql.conf");
                    File.Copy($@"{psqlDataDir}\postgresql.auto.conf", $@"{slaveBackupDir}\postgresql.auto.conf");
                }
            }
            catch (Exception ex)
            {
                Error_List.Text = $"Backup is failed: {ex}";
            }
        }
        
        private void IP_TextChanged(object sender, TextChangedEventArgs e)
        {
            TestButton.IsEnabled = (Slave_IP.Text != "") && (Master_IP.Text != "") && (Primary_IP.Text != "") && (Testing_DB.Text != "");
        }
    }
}
