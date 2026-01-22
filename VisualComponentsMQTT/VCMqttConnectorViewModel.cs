using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.Composition;
using Caliburn.Micro;
using VisualComponents.UX.Shared;
using VisualComponents.Create3D;
using System.ComponentModel;
using System.Collections.ObjectModel;
using System.Windows.Controls;


// including the M2Mqtt Library
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using System.Windows;

namespace VisualComponentsMQTT
{
    /// <summary>
    /// MQTT-Connector for VC-Components. The Addon will be loaded as Plugin an uses a DockableScreen.
    /// </summary>
    [Export(typeof(IDockableScreen))]
    public class VCMqttConnectorViewModel : DockableScreen
    {
        [ImportingConstructor]
        public VCMqttConnectorViewModel([Import] IMessageService messageService, [Import] IApplication application)
        {
            this.DisplayName = "MQTT-Panel";
            this._messageService = messageService;
            this._application = application;
            _i40_ready_components = new ObservableCollection<TreeViewItem>();
            this.Initialize();
        }

        //TreeView Binding
        public ObservableCollection<TreeViewItem> _i40_ready_components;
        public ObservableCollection<TreeViewItem> I40ReadyComponents
        {
            get { return _i40_ready_components; }
        }

        // Application
        private IApplication _application;
        // Define a Message Service
        IMessageService _messageService;

        // MQTT-Client
        private MqttClient _mqttClient;
        // Container for the Client ID
        private string _clientId;
        // QOS of MQTT.
        private byte _qos = MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE;

        // Correlates OPCUA BrowseNames to corresponding component names
        public Dictionary<string, ISignal> topicToSignal { get; set; } = new Dictionary<string, ISignal>();
        public Dictionary<ISignal, string> signalToTopic { get; set; } = new Dictionary<ISignal, string>();

        public List<string> topicsToSubscribe = new List<string>();

        private string[] _validRpcExtensions = new string[] { "rpc_call_", "rpc_cancel_", "rpc_running_", "rpc_done_", "rpc_error_" };
        private string[] _validRpcExtensionsToTopic = new string[] { "input/call", "input/cancel", "status/running", "status/done", "status/error" };
        private string[] _validRpcOutputExtensions = new string[] { "rpc_running_", "rpc_done_", "rpc_error_" };

        public string Sequence { get; set; } = ""; //Sequence of Participant

        public List<BehaviorType> validSignals = new List<BehaviorType>();
        public BehaviorType[] validSignalsArray = new BehaviorType[] {
            BehaviorType.StringSignal,
            BehaviorType.BooleanSignal,
            BehaviorType.RealSignal,
            BehaviorType.IntegerSignal,
        };

        /// <summary>
        /// Helper Function, which will be used to Log the Messages.
        /// </summary>
        /// <param name="message">The message, which will be logged</param>
        /// <param name="force">Forces the Log.</param>
        private void _log(string message, bool force = false)
        {
            // Only if Logging is enabled => Show the Log
            if (enableLog || force)
            {
                _messageService.AppendMessage(message, MessageLevel.Warning);
            }
        }

        /// <summary>
        /// Helper to show a Messagebox with a caption
        /// </summary>
        /// <param name="message">The Content of the Message</param>
        /// <param name="caption">The Caption of the Messagebox</param>
        private void _showMessageBox(string message, string caption = "")
        {
            VcMessageBox.Show(message, caption);
        }

        public void Initialize()
        {
            _log("MQTT-Plugin loaded", true);

            // Subscribe to Adding or removing items as well as Loading
            _application.LayoutLoaded += _layoutLoaded;
            _application.World.ComponentAdded += _componentAdded;
            _application.World.ComponentRemoving += _componentRemoved;
            _application.Simulation.SimulationStarted += _simulationStarted;
            _application.Simulation.SimulationStopped += _simulationStopped;
            _application.Simulation.SimulationReset += _simulationReset;
            _application.Simulation.SpeedFactorChanged += _simulationSpeedChanged;

        }


        /// <summary>
        /// Helper to publish the simulation State.
        /// </summary>
        /// <param name="reseted"></param>
        private void _sendSimUpdate(bool reseted)
        {
            string topicPrefix = "";
            if (!string.IsNullOrWhiteSpace(Sequence)) { topicPrefix = Sequence + "/"; }
            else { }
        
            _publish(topicPrefix + "vc/status/initialState", reseted.ToString().ToLower());
            _publish(topicPrefix + "vc/status/running", _application.Simulation.IsRunning.ToString().ToLower());
            _publish(topicPrefix + "vc/status/running", _application.Simulation.IsRunning.ToString().ToLower());
        }
        private void _simulationSpeedChanged(object sender, SimulationSpeedFactorChangedEventArgs e)
        {
            string topicPrefix = "";
            if (!string.IsNullOrWhiteSpace(Sequence)) { topicPrefix = Sequence + "/"; }
            else { }

            _publish(topicPrefix + "vc/status/speed", _application.Simulation.SpeedFactor.ToString());
        }
        private void _simulationReset(object sender, EventArgs e)
        {
            _sendSimUpdate(true);
        }
        private void _simulationStopped(object sender, EventArgs e)
        {
            _sendSimUpdate(false);
        }
        private void _simulationStarted(object sender, EventArgs e)
        {
            _sendSimUpdate(false);
        }

        /// <summary>
        /// Helper Function, which will update the Subscriptions
        /// based on the present Compontens
        /// </summary>
        public void update()
        {
            _unsubscribe();
            _getSignals();
            _subscribe();
        }

        /// <summary>
        /// Function to Disconnect from MQTT.
        /// </summary>
        public void disconnect()
        {
            // Disconnect the old Broker, but only if it is connected
            if (_mqttClient != null && _mqttClient.IsConnected)
            {
                _mqttClient.Disconnect();
                _showMessageBox("Disconnected", "Info");
            }
        }

        public void updateTree()
        {
            update();
        }
        /// <summary>
        /// Function, that will connect the defined MQTT-Broker.
        /// </summary>
        public void connect()
        {
            _log("Connectiung " + Sequence + " to Broker", true);
            // Dispose the old Broker
            disconnect();

            // Define a new Broker
            _mqttClient = new MqttClient(brokerAddress);

            // register a callback-function (we have to implement, see below) which is called by the library when a message was received
            _mqttClient.MqttMsgPublishReceived += _msgReceived; ;

            // use a unique id as client id, each time we start the application
            _clientId = Guid.NewGuid().ToString();

            try
            {
                // Try to connect to our borker.
                _mqttClient.Connect(_clientId);
                // Logging the Connection.
                _showMessageBox("MQTT-Plugin connected to " + brokerAddress, "Info");
                // And force it an the output.
                _log("MQTT-Plugin connected to " + brokerAddress, true);

                // Manually Perform a Subscribing the Element
                update();
            }
            catch
            {
                // Something went wrong... we now inform the User.
                _showMessageBox("MQTT-Plugin failed to connect to " + brokerAddress, "Error");
                _log("MQTT-Plugin failed to connect to " + brokerAddress, true);
            }
        }

        /// <summary>
        /// collects all signal topics based on checkbox condition and returns list of checked topics for subscription
        /// </summary>
        public List<string> collectTopicsToSubscribe()
        {              
            List<string> checked_topicsToSubscribe = new List<string>();

            foreach (TreeViewItem mainComponents in _i40_ready_components)
            {
                ItemCollection itemCollectionOfMainComponents = mainComponents.Items;
                foreach(TreeViewItem signalType in itemCollectionOfMainComponents) 
                {
                    ItemCollection itemCollectionOfSignalType = signalType.Items;
                    foreach (TreeViewItem signal in itemCollectionOfSignalType)
                    {
                        try
                        {
                            StackPanel stackPanel = signal.Header as StackPanel;
                            CheckBox checkbox = stackPanel.Children[0] as CheckBox;
                            TreeViewItem treeViewItem = stackPanel.Children[1] as TreeViewItem;

                            if (checkbox.IsChecked == true) { checked_topicsToSubscribe.Add(treeViewItem.Header.ToString()); }
                            else 
                            {
                                //do nothing when treeviewitem is a signal but unchecked
                            }

                        }
                        catch
                        {
                            //do nothing when treeviewitem is not a signal
                        }
                    }
                }
            }
            return checked_topicsToSubscribe;
        }

        /// <summary>
        /// Function to subscribe to topics on mqtt. Therefore we just susbcribe the topics
        /// provided in the attirbute <see cref="topicToSignal"/>
        /// </summary>
        private void _subscribe()
        {
            string topicPrefix = "";
            if (!string.IsNullOrWhiteSpace(Sequence)) { topicPrefix = Sequence + "/"; }
            else { }

            // We will only go one, if our mqtt-client is connected.
            if (_mqttClient != null && _mqttClient.IsConnected)
            {
                string[] controlTopics = new string[] {
                   topicPrefix+"vc/control/reset",
                   topicPrefix+"vc/control/play",
                   topicPrefix+"vc/control/pause",
                   topicPrefix+"vc/control/speed"
                };

                topicsToSubscribe = collectTopicsToSubscribe();

                string[] _topicsToSubscribe = topicsToSubscribe.Concat(controlTopics).ToArray();

                // Create an array of the length of topics and fill it with the QOS
                byte[] qos = new byte[_topicsToSubscribe.Length];

                int idx = 0;
                // Now we iterate over every topic and log its name.
                foreach (string topic in _topicsToSubscribe)
                {
                    // Update the QOS
                    qos[idx] = _qos;

                    // Show the User which signals has been subscribed
                    _log("Subscribing to '" + topic + "'", true);

                    idx++;
                }

                if (_topicsToSubscribe.Length > 0)
                {
                    // Now we just subscribe to the relevant topics.
                    try
                    {
                        _mqttClient.Subscribe(_topicsToSubscribe, qos);
                    }
                    catch
                    {
                        _log("Not able to Subscribe!", true);
                    }
                }
            }
            else
            {
                // Create a Warning, that the client is not connected
                _log("MQTT-Client not connected. Is the Broker running?", true);
            }
        }


        /// <summary>
        /// Function, used to unsubscribe every topic.
        /// </summary>
        private void _unsubscribe()
        {
            if (_mqttClient != null && _mqttClient.IsConnected)
            {
                if (topicsToSubscribe.Count > 0)
                {
                    try
                    {
                        // Unsubscribe every Topic:
                        _mqttClient.Unsubscribe(topicsToSubscribe.ToArray());
                    }
                    catch
                    {
                        _log("Not able to Subscribe");
                    }
                }
            }
            else
            {
                // Create a Warning, that the client is not connected
                _log("MQTT-Client not connected. Is the Broker running?");
            }
        }

        private void _publish(string topic, string content)
        {
            if (_mqttClient != null && _mqttClient.IsConnected)
            {
                // publish a message with QoS 2
                _mqttClient.Publish(topic, Encoding.UTF8.GetBytes(content), _qos, true);
            }
            else
            {
                _log("Not Publishing! MQTT-Client not connected. Is the Broker running?", true);
            }
        }

        /// <summary>
        /// Handler to react on specific messages on mqtt.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">The Event of the Published Message.</param>
        private void _msgReceived(object sender, MqttMsgPublishEventArgs e)
        {
            // Extract the Content
            string contentOfMessage = Encoding.UTF8.GetString(e.Message);
            // Extract the Topic
            string topic = e.Topic;
            string topicPrefix = "";
            if (!string.IsNullOrWhiteSpace(Sequence)) { topicPrefix = Sequence + "/"; }
            else { }

            if (topicToSignal.ContainsKey(topic))
            {
                // Extract the Signal.
                ISignal signal = topicToSignal[topic];

                // Found the Value. Parse based on the Type.
                switch (signal.Type)
                {
                    case BehaviorType.BooleanSignal:
                        // We are excepting valid => JSON ==>
                        // we check for "true" and "false"
                        if (contentOfMessage == "true" || contentOfMessage == "1")
                        {
                            signal.Value = true;
                        }
                        else if (contentOfMessage == "false" || contentOfMessage == "0")
                        {
                            signal.Value = false;
                        }
                        else
                        {
                            _log("can not parse data on '" + topic + "'. Expected 'true' or 'false'");
                        }
                        break;
                    case BehaviorType.IntegerSignal:
                        try
                        {
                            // Parse to Integer
                            signal.Value = int.Parse(contentOfMessage);
                        }
                        catch
                        {
                            _log("can not parse data on '" + topic + "'. Expected a 16-Bit integer");
                        }
                        break;
                    case BehaviorType.RealSignal:
                        try
                        {
                            // Parse the Value to a double element
                            signal.Value = Double.Parse(contentOfMessage);
                        }
                        catch
                        {
                            _log("can not parse data on '" + topic + "'. Expected a Double");
                        }
                        break;
                    case BehaviorType.StringSignal:
                        try
                        {
                            // Our Addon is capable of working with rpcs.
                            // rpc and json signals are "string"-signals,
                            // that have to be named in the beginning 
                            // with "rpc_" or "json_"



                            // When we work with data, we have to differenciate
                            // between those.
                            if (signal.Name.StartsWith("rpc_") || signal.Name.EndsWith("AsJson"))
                            {
                                // Now we have to differ between "rpc" or "json" elements.
                                // They are handled, that they provide valid json only.
                                signal.Value = contentOfMessage;
                            }
                            else
                            {
                                // We are working with default string signal.
                                // Because we are excepting json => we have to
                                // remove the " chars.
                                signal.Value = contentOfMessage.Substring(1, contentOfMessage.Length - 2);
                            }
                        }
                        catch
                        {
                            _log("can not parse data on '" + topic + "'. Expected a Double");
                        }
                        break;
                    case BehaviorType.ComponentSignal:
                        break;
                    case BehaviorType.MatrixSignal:
                        break;
                }

                // Now we are more or less done.
                return;
            }

            
            if (topic == topicPrefix + "vc/control/reset")
            {
                _application.Simulation.Reset();
            }
            else if (topic == topicPrefix + "vc/control/play")
            {
                try
                {
                    // The user is able to provide the speed of the Simulation
                    _application.Simulation.Run(Double.Parse(contentOfMessage));
                }
                catch
                {
                    // Run with default Settings.
                    _application.Simulation.Run(1);
                }
            }
            else if (topic == topicPrefix + "vc/control/pause")
            {
                _application.Simulation.Pause();
            }
            else if (topic == topicPrefix + "vc/control/speed")
            {
                try
                {
                    // The user is able to provide the speed of the Simulation
                    _application.Simulation.SpeedFactor = Double.Parse(contentOfMessage);
                }
                catch
                {
                }
            }
            else { }

            // Log the Message
            _log("MQTT-Client received " + contentOfMessage + "on " + topic, false);
        }

        private void _componentRemoved(object sender, ComponentRemovingEventArgs e)
        {
            update();
        }

        private void _componentAdded(object sender, ComponentAddedEventArgs e)
        {
            update();
        }

        private void _layoutLoaded(object sender, LayoutLoadedEventArgs e)
        {
            update();
        }

        /// <summary>
        /// Function to generate the Topic Name.
        /// </summary>
        /// <param name="node"></param>
        /// <param name="signal"></param>
        /// <param name="handleAsInput"></param>
        /// <returns></returns>
        private string _getTopicForSignal(ISimNode node, ISignal signal, bool handleAsInput = false)
        {
            string topicPrefix = "";
            if (!string.IsNullOrWhiteSpace(Sequence)) { topicPrefix = Sequence + "/"; }
            else { }

            if (_isRpcSignal(signal))
            {
                return topicPrefix + "vc/rpc/" + _getGenericTopicHelper(node, _rpcTopicToName(signal.Name));
            }
            else if (handleAsInput)
            {
                return topicPrefix + "vc/signal/input/" + _getGenericTopicHelper(node, signal.Name);
            }
            return topicPrefix + "vc/signal/output/" + _getGenericTopicHelper(node, signal.Name);
        }

        /// <summary>
        /// Function, which will create valid mqtt-topics, based on the hierarchy of
        /// our model. Therfore, we recursivly generate the name by iterating over the
        /// parent node of an element.
        /// </summary>
        /// <param name="node">Simulation node to iterate over</param>
        /// <param name="name">Additional Strin that shoud be added</param>
        /// <returns></returns>
        private string _getGenericTopicHelper(ISimNode node, string name)
        {
            Dictionary<string, string> replacers = new Dictionary<string, string>
            {
                { " ",  "_"},
                { "#",  ""},
            };

            string _ret = "";

            if (node.Parent == null)
            {
                _ret = name;
            }
            else
            {
                _ret = _getGenericTopicHelper(node.Parent, node.Name + "/" + name);
            }

            // Perform the Replacers
            foreach (string key in replacers.Keys)
            {
                _ret = _ret.Replace(key, replacers[key]);
            }

            return _ret;
        }

        /// <summary>
        /// Function, which will return the corresponding RPC Name, based on the Signal.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private string _getRpcName(string name)
        {
            foreach (string rpcExension in _validRpcExtensions)
            {
                if (name.StartsWith(rpcExension))
                {
                    return name.Substring(name.Length);
                }

            }

            throw new Exception("Must take care of an RPC-Signal");
        }

        /// <summary>
        /// Function, gets the corresponding signal-name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private string _rpcTopicToName(string name)
        {
            int index = 0;
            foreach (string rpcExension in _validRpcExtensions)
            {
                if (name.StartsWith(rpcExension))
                {
                    return _validRpcExtensionsToTopic[index] + _getRpcName(name);
                }

                index++;

            }

            throw new Exception("Must take care of an RPC-Signal");
        }

        /// <summary>
        /// Helper Function which returns, whether a signal is related to 
        /// a RPC or just a default signal.
        /// </summary>
        /// <param name="signal">The Signal to Test.</param>
        /// <returns>True = RPC-Signal, False = Signal</returns>
        private bool _isRpcSignal(ISignal signal)
        {
            return signal.Name.StartsWith("rpc_");
        }

        private List<ISignal> _getSignals()
        {
            List<ISignal> initialNodes = new List<ISignal>();
            List<ISignal> validSignalTypes = new List<ISignal>();


            foreach (ISignal vcSignal in signalToTopic.Keys)
            {
                // Remove Listener
                vcSignal.SignalTrigger -= _signalChanged;
            }

            // Clear the Dicts.
            signalToTopic.Clear();
            topicToSignal.Clear();
            topicsToSubscribe.Clear();
            _i40_ready_components.Clear();

            // Iterate over the Components
            foreach (ISimComponent vcComponent in _application.World.Components)
            {
                
                TreeViewItem i40_component = new TreeViewItem() { Header = vcComponent.Name };
                TreeViewItem i40_component_inputSignals = new TreeViewItem() { Header = "Input Signals" };
                TreeViewItem i40_component_outputSignals = new TreeViewItem() { Header = "Output Signals" };
                TreeViewItem i40_component_rpcSignals = new TreeViewItem() { Header = "RPC" };
                i40_component.Items.Add(i40_component_inputSignals);
                i40_component.Items.Add(i40_component_outputSignals);
                _i40_ready_components.Add(i40_component);

                // Iterate over every Behavior and Filter the Relevant Signals
                foreach (IBehavior vcBehavior in vcComponent.Behaviors)
                {

                    // Test if the Behavior is a Valid Behavior:
                    if (validSignalsArray.Contains(vcBehavior.Type))
                    {
                        ISignal signal = (ISignal)vcBehavior;

                        string signalName = vcBehavior.Name;

                        // We check, if we want to subscribe to a rpc 
                        // or to a signal. We decide this using the rpc
                        // name. 
                        if (signalName.StartsWith("rpc_"))
                        {

                            // Generate the Topic as Input:
                            string signalTopic = _getTopicForSignal(vcBehavior.Node, signal);

                            // Store the Mapping
                            signalToTopic.Add(signal, signalTopic);
                            topicToSignal.Add(signalTopic, signal);

                            bool found = false;
                            foreach (string rpcExension in _validRpcExtensions)
                            {
                                if (signalName.StartsWith(rpcExension))
                                {
                                    found = true;
                                    break;
                                }

                            }
                            if (!found)
                            {
                                // Give the user a Hint, that he isnt following the name-schema.
                                _log("You should consider the RPC Name Schema. Please use the following elements: " + string.Join(",", _validRpcExtensions), true);
                            }


                            // Now we checkout if this signal is used as Output.
                            // If so, we subscribe the changes and forward them
                            // to MQTT.
                            found = false;
                            foreach (string rpcExension in _validRpcOutputExtensions)
                            {
                                if (signalName.StartsWith(rpcExension))
                                {
                                    // Outputs will be used to publish the 
                                    // Data on MQTT.
                                    signal.SignalTrigger += _signalChanged;

                                    found = true;
                                    break;
                                }
                            }

                            // The Signal should be handled as Input => Mark it
                            // to be subscribed.
                            if (!found)
                            {
                                topicsToSubscribe.Add(signalTopic);
                            }

                            TreeViewItem i40_component_inputSignalRPCTopic = new TreeViewItem() { Header = signalTopic };
                            i40_component_rpcSignals.Items.Add(i40_component_inputSignalRPCTopic);

                            _log("Added RPC-Signal -> " + signalTopic);
                        }
                        else
                        {
                            // We just handling a normal signals:

                            // 1. Handle the Input-Signals
                            // As Input Signal we do not need the Mapping between the signal and
                            // its name => because we only need this connection on signal changes
                            string InputSignalTopic = _getTopicForSignal(vcBehavior.Node, signal, true);
                            topicToSignal.Add(InputSignalTopic, signal);
                            topicsToSubscribe.Add(InputSignalTopic);
                            _log("Added Signal -> " + InputSignalTopic);

                            

                            // 2. Handle the Output-Signals.
                            // We need to link the signal with the topic, to use it in the trigger
                            string OutputSignalTopic = _getTopicForSignal(vcBehavior.Node, signal, false);
                            
                            signalToTopic.Add(signal, OutputSignalTopic);
                            topicToSignal.Add(OutputSignalTopic, signal);
                            // Adding a Trigger
                            signal.SignalTrigger += _signalChanged;

                            _log("Added Signal -> " + OutputSignalTopic);


                            ///adding input and output signals als child element to parent tree view item as stackpanel of checkbox and name, ToDo: maybe better in extra method
                            StackPanel stackPanelInput = new StackPanel();
                            stackPanelInput.Children.Add(new CheckBox() {IsChecked=true});
                            stackPanelInput.Children.Add(new TreeViewItem() { Header = InputSignalTopic });
                            stackPanelInput.Orientation = Orientation.Horizontal;
                            TreeViewItem stacked_component_treeViewItem_input = new TreeViewItem { Header = stackPanelInput, HorizontalAlignment = HorizontalAlignment.Stretch };

                            StackPanel stackPanelOutput = new StackPanel();
                            stackPanelOutput.Children.Add(new CheckBox() { IsChecked = true });
                            stackPanelOutput.Children.Add(new TreeViewItem() { Header = OutputSignalTopic });
                            stackPanelOutput.Orientation = Orientation.Horizontal;
                            TreeViewItem stacked_component_treeViewItem_output = new TreeViewItem { Header = stackPanelOutput, HorizontalAlignment = HorizontalAlignment.Stretch };                   

                            i40_component_inputSignals.Items.Add(stacked_component_treeViewItem_input);
                            i40_component_outputSignals.Items.Add(stacked_component_treeViewItem_output);
                            ////
                        }
                    }
                }
            }
            return initialNodes;
        }

        private void _signalChanged(object sender, SignalTriggerEventArgs e)
        {
            // Test if the Singal exists
            if (signalToTopic.ContainsKey(e.Signal))
            {
                // Extract the Signal and Topic.
                ISignal signal = e.Signal;
                string topic = signalToTopic[e.Signal];

                // Log the Message.
                _log(topic + "->" + signal.Value.ToString());

                string content = "";

                // Based on the Signal Type.
                // Decide how to parse the Type.
                switch (signal.Type)
                {
                    case BehaviorType.BooleanSignal:
                        content = signal.Value.ToString().ToLower();
                        break;
                    case BehaviorType.IntegerSignal:
                    case BehaviorType.RealSignal:
                        // Publish the Value:      
                        // We just convert it to a string,
                        content = signal.Value.ToString();
                        break;
                    case BehaviorType.StringSignal:
                        // Publish the Value:      
                        // We just convert it to a string,
                        if (_isRpcSignal(signal) || signal.Name.EndsWith("AsJson"))
                        {
                            content = signal.Value.ToString();
                        } else
                        {
                            content = '"' + signal.Value.ToString() + '"';
                        }
                        
                        break;
                    case BehaviorType.ComponentSignal:
                        // We will publish its Properties.
                        IComponent comp = (IComponent)signal.Value;
                        break;
                    case BehaviorType.MatrixSignal:
                        // Here we have to do proper convertion
                        break;
                }

                // Test if the Content has been adapted.
                // If so, publish it to the system.
                if (content != "")
                {
                    _publish(topic, content);
                }
            }

        }

        private string _brokerAddress = "127.0.0.1";
        public string brokerAddress
        {
            get { return _brokerAddress; }
            set
            {
                _brokerAddress = value;

                string[] invalidChars = new string[]
                {
                    " ",
                    ","
                };

                for (int idx = 0; idx < invalidChars.Length; idx++)
                {
                    if (value.Contains(invalidChars[idx]))
                    {
                        _log("Broker address contains invalid characters");
                        break;
                    }
                }

                NotifyOfPropertyChange(brokerAddress);
            }
        }


        private List<string> CollectTreeElements(ItemCollection treeItemCollection, List<string> checked_tree_signals)
        {
            foreach (TreeViewItem node in treeItemCollection)
            {
                _log("--------- 1) " + node.Header.ToString());
                if (node.Items.Count != 0) //if node has childs - open node and run over childs
                {
                    node.IsExpanded = true;
                    ItemCollection it = node.Items;
                    CollectTreeElements(it, checked_tree_signals);
                }
                else  // if node has no childs:
                {
                    try
                    {
                        StackPanel stackPanel = node.Header as StackPanel;
                        CheckBox checkbox = stackPanel.Children[0] as CheckBox;
                        TreeViewItem treeViewItem = stackPanel.Children[1] as TreeViewItem;

                        if (checkbox.IsChecked == true)
                        {
                            //if checkbox is checked, add to checked_tree_signals as string
                            checked_tree_signals.Add(treeViewItem.Header.ToString());
                        }
                        else
                        {
                            //if checkbox isn't checked, than do nothing
                        }

                    }
                    catch
                    {
                        //isn't a stackpanel, so no signal
                    }
                }
            }

            return checked_tree_signals;
        }
        public bool enableLog { get; set; }
    }
}
