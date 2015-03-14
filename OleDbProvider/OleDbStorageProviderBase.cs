
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Text;
using ScrewTurn.Wiki.PluginFramework;

namespace Ohm.ScrewTurn.Wiki.Provider {

	/// <summary>
	/// Implements a base class for OleDb Providers.
	/// </summary>
	[Serializable]
	public abstract class OleDbStorageProviderBase {

		protected string config;
		protected IHost host;

		public void Init(IHost host, string config) {
			this.host = host;
			this.config = ParseConfig(config);
			if(!ValidateConfig()) throw new InvalidConfigurationException("Unable to perform the Test Query, check the Configuration String and the Database.");
			if(!IsDatabaseUpToDate()) {
				if(!UpdateDatabase()) throw new Exception("Unable to update the Database Schema.");
			}
		}

		protected abstract bool ValidateConfig();

		protected abstract bool IsDatabaseUpToDate();

		protected abstract bool UpdateDatabase();

		/// <summary>
		/// Gets a new OleDb Connection.
		/// </summary>
		/// <returns>The OleDb Connection.</returns>
		protected OleDbConnection GetConnection() {
			return new OleDbConnection(config);
		}

		/// <summary>
		/// Gets a new OleDb Command, set with a new OleDb Connection.
		/// </summary>
		/// <returns>The OleDb Command.</returns>
		protected OleDbCommand GetCommand() {
			return GetConnection().CreateCommand();
		}

		/// <summary>
		/// Gets a new OleDbParameter.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		protected OleDbParameter GetParameter(string name, OleDbType type, object value) {
			return GetParameter(name, type, -1, value);
		}

		/// <summary>
		/// Gets a new OleDbParameter.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="size"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		protected OleDbParameter GetParameter(string name, OleDbType type, int size, object value) {
			OleDbParameter retVal;

			retVal = new OleDbParameter();
			retVal.ParameterName = name;
			retVal.OleDbType = type;
			if (size > 0) {
				retVal.Size = size;
			}
			retVal.Value = value;

			return retVal;
		}

		/// <summary>
		/// Begin a transaction for a OleDb Command.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		protected void BeginTransaction(OleDbCommand cmd) {
			try {
				cmd.Connection.Open();
				cmd.Transaction = cmd.Connection.BeginTransaction();
			} catch { }
		}

		/// <summary>
		/// Commit a transaction for a OleDb Command and closes it's Connection.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		protected void CommitTransaction(OleDbCommand cmd) {
			try {
				cmd.Transaction.Commit();
				cmd.Connection.Close();
			} catch { }
		}

		/// <summary>
		/// Abort a transaction for a OleDb Command and closes it's Connection.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		protected void RollBackTransaction(OleDbCommand cmd) {
			try {
				cmd.Transaction.Rollback();
				cmd.Connection.Close();
			} catch { }
		}

		/// <summary>
		/// Closes the Connection used by a OleDb Command.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		protected void Close(OleDbCommand cmd) {
			try {
				cmd.Connection.Close();
			}
			catch { }
		}

		/// <summary>
		/// Executes the method <b>ExecuteScalar</b> of a OleDb Command and returns the result.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		/// <returns>The result, or null.</returns>
		/// <remarks>The method automatically opens and then closes the connection.</remarks>
		protected object ExecuteScalar(OleDbCommand cmd) {
			object result = null;
			try {
#if DEBUG
				LogCommand("S", cmd);
#endif
				if (cmd.Transaction == null || cmd.Connection.State == ConnectionState.Closed) {
					cmd.Connection.Open();
				}
				result = cmd.ExecuteScalar();
			}
			catch(Exception ex) {
				result = null;
				host.LogEntry(ex.Message, LogEntryType.Error, this);
			}
			if (cmd.Transaction == null) {
				//Close only if no transaction is active.
				Close(cmd);
			}
			return result;
		}

		/// <summary>
		/// Executes the method <b>ExecuteReader</b> of a OleDb Command and returns the OleDb Data Reader.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		/// <returns>The OleDb Data Reader, or null.</returns>
		/// <remarks>The method automatically opens the connection but it <b>does not</b> closes it.</remarks>
		protected OleDbDataReader ExecuteReader(OleDbCommand cmd) {
			OleDbDataReader result = null;
			try {
#if DEBUG
				LogCommand("R", cmd);
#endif
				if (cmd.Transaction == null || cmd.Connection.State == ConnectionState.Closed) {
					cmd.Connection.Open();
				}
				result = cmd.ExecuteReader();
			}
			catch(Exception ex) {
				host.LogEntry(ex.Message, LogEntryType.Error, this);
				result = null;
			}
			return result;
		}

		/// <summary>
		/// Executes the method <b>ExecuteNonQuery</b> of a OleDb Command and returns the result.
		/// </summary>
		/// <param name="cmd">The OleDb Command.</param>
		/// <returns>The result, or -1.</returns>
		/// <remarks>The method automatically opens and then closes the connection.</remarks>
		protected int ExecuteNonQuery(OleDbCommand cmd) {
			int result = -1;
			try {
#if DEBUG
				LogCommand("N", cmd);
#endif
				if (cmd.Transaction == null || cmd.Connection.State == ConnectionState.Closed) {
					cmd.Connection.Open();
				}
				result = cmd.ExecuteNonQuery();
			}
			catch(Exception ex) {
				host.LogEntry(ex.Message, LogEntryType.Error, this);
				result = -1;
			}
			if (cmd.Transaction == null) {
				//Close only if no transaction is active.
				Close(cmd);
			}
			return result;
		}

		private void LogCommand(string method, OleDbCommand cmd) {
			/*
			host.LogEntry(string.Format("{0}; T:{1}; S:{2}; Q:{3}",
				method,
				(cmd.Transaction == null),
				cmd.Connection.State,
				cmd.CommandText.Substring(0, Math.Min(cmd.CommandText.Length, 192))), LogEntryType.General, this);
			 * */
		}

		private string ParseConfig(string config) {
		    string[] configParams;
		    configParams = config.Split(';'); //Split connection string's parameters
		    for (int i = 0; i < configParams.Length; i++) {
		        if (configParams[i].Trim().ToLower().StartsWith("data source")) {
					//Process "Data Source" parameter
		            string[] dataValue;
		            dataValue = configParams[i].Split('=');
					dataValue[1] = dataValue[1].TrimStart(); //Remove leading spaces
		            if (dataValue[1].StartsWith("/") || dataValue[1].StartsWith("\\")) {
		                //mdb path is relative to root
						//Remove leading [back]slashes and replace slashes with backslashes
						dataValue[1] = dataValue[1].TrimStart('\\', '/').Replace("/", "\\");

						configParams[i] = "Data Source=" +
							System.IO.Path.Combine(System.Web.HttpRuntime.AppDomainAppPath, dataValue[1]);
					}
		        }
		    }
#if DEBUG
			host.LogEntry(string.Join(";", configParams), LogEntryType.General, this);
#endif
			return string.Join(";", configParams);
		}

		public void Shutdown() { }

	}

}
