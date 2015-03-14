
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Text;
using ScrewTurn.Wiki.PluginFramework;
using System.Security.Cryptography;

namespace Ohm.ScrewTurn.Wiki.Provider {

	/// <summary>
	/// Implements a Users Storage Provider against OleDb storage.
	/// </summary>
	public class OleDbUsersStorageProvider : OleDbStorageProviderBase, IUsersStorageProvider {

		private ComponentInformation info = new ComponentInformation("OleDb Users Storage Provider " + CurrentVersion + CurrentRevision, "ScrewTurn Software", "http://www.screwturn.eu");
		private const string CurrentVersion = "1.0";
		private const string CurrentRevision = ".0";

		protected override bool ValidateConfig() {
			// Config must be a valid Connection String
			// Open a connection and perform a test query
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT count(*) FROM [User]";
			object c = null;
			try {
				c = ExecuteScalar(cmd);
			}
			catch { }
			if(c == null) {
				return GenerateDatabase();
			}
			else return true;
		}

		private bool GenerateDatabase() {
			int affected;

			OleDbCommand cmd = GetCommand();
			
			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "CREATE TABLE [UsersProviderVersion] ( " +
				"[Version] varchar(12) Constraint PrimaryKey primary key " +
				"); ";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "INSERT INTO [UsersProviderVersion] ([Version]) VALUES ('" + CurrentVersion + "'); ";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "CREATE TABLE [User] ( " +
				"[Username] nvarchar(128) Constraint PrimaryKey primary key, " +
				"[PasswordHash] varchar(128) NOT NULL, " +
				"[Email] varchar(128) NOT NULL, " +
				"[DateTime] datetime NOT NULL, " +
				"[Active] bit NOT NULL, " + // DEFAULT ((0))
				"[Admin] bit NOT NULL " + // DEFAULT ((0))
				");";
			affected += ExecuteNonQuery(cmd);

			if (affected > 0) {
				CommitTransaction(cmd);
			} else {
				RollBackTransaction(cmd);
			}

			return affected > 0;
		}

		protected override bool IsDatabaseUpToDate() {
			// Try to retrieve the version number
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [UsersProviderVersion]";
			string ver = null;
			try {
				ver = (string)ExecuteScalar(cmd);
			}
			catch {	}
			if(ver == null) {
				// Database has no Version, create table Version (v1.0) and Update Database
				cmd = GetCommand();
				cmd.CommandText = "CREATE TABLE [UsersProviderVersion] ([Version] varchar(12) Constraint PrimaryKey primary key); ";
				ExecuteNonQuery(cmd);
				cmd.CommandText = "INSERT INTO [UsersProviderVersion] ([Version]) VALUES ('1.0'); ";
				ExecuteNonQuery(cmd);
				return false;
			}
			else if(ver.Equals(CurrentVersion)) return true;
			else return false;
		}

		protected override bool UpdateDatabase() {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [UsersProviderVersion]";
			string ver = (string)ExecuteScalar(cmd);
			return UpdateDatabaseInternal(ver, CurrentVersion);
		}

		private bool UpdateDatabaseInternal(string fromVersion, string toVersion) {
			// No updates needed for now
			switch(fromVersion) {
				case "1.0":
					switch(toVersion) {
						case "1.1":
							return UpdateFrom10To11();
					}
					break;
			}
			return false;
		}

		private bool UpdateFrom10To11() {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [UsersProviderVersion] SET [Version] = '1.1' WHERE 1 = 1";
			return ExecuteNonQuery(cmd) == 1;
		}

		public ComponentInformation Information {
			get { return info; }
		}

		public bool ReadOnly {
			get { return false; }
		}

		public bool TestAccount(UserInfo user, string password) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [User] WHERE [Username] = @Username AND [PasswordHash] = @PasswordHash";
			cmd.Parameters.Add(new OleDbParameter("Username", user.Username));
			cmd.Parameters.Add(new OleDbParameter("PasswordHash", ComputeHash(password)));
			return ExecuteScalar(cmd) != null;
		}

		public UserInfo[] AllUsers {
			get {
				OleDbCommand cmd = GetCommand();
				cmd.CommandText = "SELECT * FROM [User]";
				OleDbDataReader reader = ExecuteReader(cmd);
				List<UserInfo> result = new List<UserInfo>();
				while(reader != null && reader.Read()) {
					result.Add(new UserInfo(reader.GetString(0), reader.GetString(2), reader.GetBoolean(4), reader.GetDateTime(3), reader.GetBoolean(5), this));
				}
				Close(cmd);
				return result.ToArray();
			}
		}

		public UserInfo AddUser(string username, string password, string email, bool active, DateTime dateTime, bool admin) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "INSERT INTO [User] ([Username], [PasswordHash], [Email], [DateTime], [Active], [Admin]) VALUES (@Username, @PasswordHash, @Email, @DateTime, @Active, @Admin)";
			cmd.Parameters.Add(new OleDbParameter("Username", username));
			cmd.Parameters.Add(new OleDbParameter("PasswordHash", ComputeHash(password)));
			cmd.Parameters.Add(new OleDbParameter("Email", email));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, dateTime));
			cmd.Parameters.Add(new OleDbParameter("Active", active));
			cmd.Parameters.Add(new OleDbParameter("Admin", admin));
			if(ExecuteNonQuery(cmd) == 1) {
				return new UserInfo(username, email, active, dateTime, admin, this);
			}
			else return null;
		}

		public UserInfo SetUserActivationStatus(UserInfo user, bool active) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [User] SET [Active] = @Active WHERE [Username] = @Username";
			cmd.Parameters.Add(new OleDbParameter("Active", active));
			cmd.Parameters.Add(new OleDbParameter("Username", user.Username));
			if(ExecuteNonQuery(cmd) == 1) {
				return new UserInfo(user.Username, user.Email, active, user.DateTime, user.Admin, this);
			}
			else return null;
		}

		public UserInfo SetUserAdministrationStatus(UserInfo user, bool admin) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [User] SET [Admin] = @Admin WHERE [Username] = @Username";
			cmd.Parameters.Add(new OleDbParameter("Admin", admin));
			cmd.Parameters.Add(new OleDbParameter("Username", user.Username));
			if(ExecuteNonQuery(cmd) == 1) {
				return new UserInfo(user.Username, user.Email, user.Active, user.DateTime, admin, this);
			}
			else return null;
		}

		public bool RemoveUser(UserInfo user) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "DELETE FROM [User] WHERE [Username] = @Username";
			cmd.Parameters.Add(new OleDbParameter("Username", user.Username));
			return ExecuteNonQuery(cmd) == 1;
		}

		public UserInfo ChangeEmail(UserInfo user, string newEmail) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [User] SET [Email] = @Email WHERE [Username] = @Username";
			cmd.Parameters.Add(new OleDbParameter("Email", newEmail));
			cmd.Parameters.Add(new OleDbParameter("Username", user.Username));
			if(ExecuteNonQuery(cmd) == 1) {
				return new UserInfo(user.Username, newEmail, user.Active, user.DateTime, user.Admin, this);
			}
			else return null;
		}

		public UserInfo ChangePassword(UserInfo user, string newPassword) {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [User] SET [PasswordHash] = @PasswordHash WHERE [Username] = @Username";
			cmd.Parameters.Add(new OleDbParameter("PasswordHash", ComputeHash(newPassword)));
			cmd.Parameters.Add(new OleDbParameter("Username", user.Username));
			if(ExecuteNonQuery(cmd) == 1) {
				return new UserInfo(user.Username, user.Email, user.Active, user.DateTime, user.Admin, this);
			}
			else return null;
		}

		/// <summary>
		/// Computes the Hash code of a string.
		/// </summary>
		/// <param name="input">The string.</param>
		/// <returns>The Hash code.</returns>
		private byte[] ComputeHashBytes(string input) {
			SHA1 sha1 = SHA1CryptoServiceProvider.Create();
			return sha1.ComputeHash(Encoding.ASCII.GetBytes(input));
		}

		/// <summary>
		/// Computes the Hash code of a string and converts it into a Hex string.
		/// </summary>
		/// <param name="input">The string.</param>
		/// <returns>The Hash code, converted into a Hex string.</returns>
		private string ComputeHash(string input) {
			byte[] bytes = ComputeHashBytes(input);
			string result = "";
			for(int i = 0; i < bytes.Length; i++) {
				result += string.Format("{0:X2}", bytes[i]);
			}
			return result;
		}

	}

}
