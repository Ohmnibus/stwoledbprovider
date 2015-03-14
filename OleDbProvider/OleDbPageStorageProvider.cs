
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Text;
using ScrewTurn.Wiki.PluginFramework;

namespace Ohm.ScrewTurn.Wiki.Provider {

	/// <summary>
	/// Implements a Pages Storage Provider against SQL Server.
	/// </summary>
	public class OleDbPagesStorageProvider : OleDbStorageProviderBase, IPagesStorageProvider {

		private ComponentInformation info = new ComponentInformation("OleDb Pages Storage Provider " + CurrentVersion + CurrentRevision, "Ohmnibus", "http://www.ohmnibus.net");
		private const string CurrentVersion = "1.0";
		private const string CurrentRevision = ".0";

		protected override bool ValidateConfig() {
			// Config must be a valid Connection String
			// Open a connection and perform a test query
#if DEBUG
			host.LogEntry("ValidateConfig", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT count(*) FROM [Page]";
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

#if DEBUG
			host.LogEntry("GenerateDatabase", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "CREATE TABLE [PagesProviderVersion] ( " +
				"[Version] varchar(12) Constraint PrimaryKey PRIMARY KEY " +
				"); ";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "INSERT INTO [PagesProviderVersion] ([Version]) VALUES ('" + CurrentVersion + "');";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [Page] ( " +
					"[Name] nvarchar(128) Constraint PrimaryKey primary key, " +
					"[Status] char(1) not null, " + // default ('N') - (P)ublic, N(ormal), (L)ocked
					"[CreationDateTime] datetime not null " +
					"); ";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [PageContent] ( " +
					"[Page] nvarchar(128) Constraint PageContentPage references [Page]([Name]) on update cascade on delete cascade, " +
					"[Revision] int not null, " + //default ((-1)) - "-1" for Current Revision
					"[Title] nvarchar(255) not null, " +
					"[DateTime] datetime not null, " +
					"[Username] nvarchar(64) not null, " +
					"[Content] ntext not null, " +
					"[Comment] nvarchar(128) not null, " +
					"Constraint PrimaryKey primary key ([Page], [Revision]) " +
					");";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [Category] ( " +
					"[Name] nvarchar(128) Constraint PrimaryKey primary key " +
					");";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [CategoryBinding] ( " +
					"[Category] nvarchar(128) Constraint CategoryBindingCategory references [Category]([Name]) on update cascade on delete cascade, " +
					"[Page] nvarchar(128) Constraint CategoryBindingPage references [Page]([Name]) on update cascade on delete cascade, " +
					"Constraint PrimaryKey primary key ([Category], [Page]) " +
					");";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [Message] ( " +
					"[ID] AutoIncrement Constraint PrimaryKey primary key, " +
					"[Page] nvarchar(128) references [Page]([Name]) on update cascade on delete cascade, " +
					"[Parent] int not null, " + //-1 for no parent
					"[Username] nvarchar(64) not null, " +
					"[DateTime] datetime not null, " +
					"[Subject] nvarchar(128) not null, " +
					"[Body] ntext not null " +
					");";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [Snippet] ( " +
					"[Name] nvarchar(128) Constraint PrimaryKey primary key, " +
					"[Content] ntext not null " +
					");";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [NavigationPath] ( " +
					"[Name] nvarchar(128) not null Constraint PrimaryKey primary key " +
					");";
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "create table [NavigationPathBinding] ( " +
					"[NavigationPath] nvarchar(128) not null references [NavigationPath]([Name]) on delete cascade, " +
					"[Page] nvarchar(128) not null references [Page]([Name]) on update cascade on delete cascade, " +
					"[Number] int not null, " +
					"Constraint PrimaryKey primary key ([NavigationPath], [Page], [Number]) " +
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
#if DEBUG
			host.LogEntry("IsDatabaseUpToDate", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [PagesProviderVersion]";
			string ver = null;
			try {
				ver = (string)ExecuteScalar(cmd);
			}
			catch { }
			if(ver == null) {
				// Database has no Version, create table Version (v1.0) and Update Database
				cmd = GetCommand();
				cmd.CommandText = "CREATE TABLE [PagesProviderVersion] ([Version] varchar(12) Constraint PrimaryKey PRIMARY KEY); ";
				ExecuteNonQuery(cmd);
				cmd.CommandText = "INSERT INTO [PagesProviderVersion] ([Version]) VALUES ('1.0');";
				ExecuteNonQuery(cmd);
				return false;
			}
			else if(ver.Equals(CurrentVersion)) return true;
			else return false;
		}

		protected override bool UpdateDatabase() {
#if DEBUG
			host.LogEntry("UpdateDatabase", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [PagesProviderVersion]";
			string ver = (string)ExecuteScalar(cmd);
			return UpdateDatabaseInternal(ver, CurrentVersion);
		}

		private bool UpdateDatabaseInternal(string fromVersion, string toVersion) {
#if DEBUG
			host.LogEntry("UpdateDatabaseInternal", LogEntryType.General, this);
#endif
			switch (fromVersion) {
				case "1.0":
					switch(toVersion) {
						case "1.1":
							return UpdateFrom10To11();
						case "1.2":
							return UpdateFrom10To12();
						case "1.3":
							return UpdateFrom10To13();
					}
					break;
				case "1.1":
					switch(toVersion) {
						case "1.2":
							return UpdateFrom11To12();
						case "1.3":
							return UpdateFrom11To13();
					}
					break;
				case "1.2":
					switch(toVersion) {
						case "1.3":
							return UpdateFrom12To13();
					}
					break;
			}
			return false;
		}

		private bool UpdateFrom10To11() {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [PagesProviderVersion] SET [Version] = '1.1' WHERE 1 = 1";
			return ExecuteNonQuery(cmd) == 1;
		}

		private bool UpdateFrom10To12() {
			bool temp = UpdateFrom10To11();
			if(temp) {
				temp = UpdateFrom11To12();
			}
			return temp;
		}

		private bool UpdateFrom10To13() {
			bool temp = UpdateFrom10To12();
			if(temp) {
				temp = UpdateFrom12To13();
			}
			return temp;
		}

		private bool UpdateFrom11To12() {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [PagesProviderVersion] SET [Version] = '1.2' WHERE 1 = 1";
			return ExecuteNonQuery(cmd) == 1;
		}

		private bool UpdateFrom11To13() {
			bool temp = UpdateFrom11To12();
			if(temp) {
				temp = UpdateFrom12To13();
			}
			return temp;
		}

		private bool UpdateFrom12To13() {
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [PagesProviderVersion] SET [Version] = '1.3' WHERE 1 = 1";
			return ExecuteNonQuery(cmd) == 1;
		}

		public ComponentInformation Information {
			get { return info; }
		}

		public bool ReadOnly {
			get { return false; }
		}

		public PageInfo[] AllPages {
			get {
#if DEBUG
				host.LogEntry("AllPages", LogEntryType.General, this);
#endif
				OleDbCommand cmd = GetCommand();
				cmd.CommandText = "SELECT * FROM [Page]";
				OleDbDataReader reader = ExecuteReader(cmd);
				List<PageInfo> result = new List<PageInfo>();
				while(reader != null && reader.Read()) {
					PageStatus s = PageStatus.Normal;
					switch(reader.GetString(1).ToUpperInvariant()) {
						case "N":
							s = PageStatus.Normal;
							break;
						case "L":
							s = PageStatus.Locked;
							break;
						case "P":
							s = PageStatus.Public;
							break;
						default:
							throw new Exception("Invalid Page Status.");
					}
					result.Add(new PageInfo(reader.GetString(0), this, s, reader.GetDateTime(2)));
				}
				Close(cmd);
				return result.ToArray();
			}
		}

		public CategoryInfo[] AllCategories {
			get {
#if DEBUG
				host.LogEntry("AllCategories", LogEntryType.General, this);
#endif
				OleDbCommand cmd = GetCommand();
				cmd.CommandText = "SELECT * FROM [Category]";
				OleDbDataReader reader = ExecuteReader(cmd);
				List<CategoryInfo> result = new List<CategoryInfo>();
				while(reader != null && reader.Read()) {
					CategoryInfo ci = new CategoryInfo(reader.GetString(0), this);
					ci.Pages = GetPagesPerCategory(ci.Name);
					result.Add(ci);
				}
				Close(cmd);
				return result.ToArray();
			}
		}

		private string[] GetPagesPerCategory(string category) {
#if DEBUG
			host.LogEntry("GetPagesPerCategory", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT [Page] FROM [CategoryBinding] WHERE [Category] = @Category";
			cmd.Parameters.Add(new OleDbParameter("Category", category));
			OleDbDataReader reader = ExecuteReader(cmd);
			List<string> result = new List<string>();
			while(reader != null && reader.Read()) {
				result.Add(reader.GetString(0));
			}
			Close(cmd);
			return result.ToArray();
		}

		public CategoryInfo AddCategory(string name) {
#if DEBUG
			host.LogEntry("AddCategory", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "INSERT INTO [Category] ([Name]) VALUES (@Category)";
			cmd.Parameters.Add(new OleDbParameter("Category", name));
			if(ExecuteNonQuery(cmd) == 1) {
				CategoryInfo ci = new CategoryInfo(name, this);
				ci.Pages = new string[0];
				return ci;
			}
			else return null;
		}

		public CategoryInfo RenameCategory(CategoryInfo category, string newName) {
#if DEBUG
			host.LogEntry("RenameCategory", LogEntryType.General, this);
#endif
			// No need to update table CategoryBinding because there is ON UPDADE/DELETE CASCADE
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [Category] SET [Name] = @NewCategory WHERE [Name] = @Category";
			cmd.Parameters.Add(new OleDbParameter("NewCategory", newName));
			cmd.Parameters.Add(new OleDbParameter("Category", category.Name));
			if(ExecuteNonQuery(cmd) == 1) {
				CategoryInfo ci = new CategoryInfo(newName, this);
				ci.Pages = category.Pages;
				return ci;
			}
			else return null;
		}

		public bool RemoveCategory(CategoryInfo category) {
#if DEBUG
			host.LogEntry("RemoveCategory", LogEntryType.General, this);
#endif
			// No need to update table CategoryBinding because there is ON UPDADE/DELETE CASCADE
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "DELETE FROM [Category] WHERE [Name] = @Category";
			cmd.Parameters.Add(new OleDbParameter("Category", category.Name));
			return ExecuteNonQuery(cmd) == 1;
		}

		public CategoryInfo MergeCategories(CategoryInfo source, CategoryInfo destination) {
#if DEBUG
			host.LogEntry("MergeCategories", LogEntryType.General, this);
#endif
			// Delete pages that are common to both the categories in [CategoryBinding]
			// Rename source to destination in [CategoryBinding]
			// Delete destination in [Category]
			int affected;
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);
			
			cmd.CommandText = "DELETE FROM [CategoryBinding] WHERE [Category] = @Cat1 AND [Page] IN (SELECT [Page] FROM [CategoryBinding] WHERE [Category] = @Cat2);";
			cmd.Parameters.Add(new OleDbParameter("Cat1", source.Name));
			cmd.Parameters.Add(new OleDbParameter("Cat2", destination.Name));
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "UPDATE [CategoryBinding] SET [Category] = @Cat2 WHERE [Category] = @Cat1;";
			cmd.Parameters.Clear();
			cmd.Parameters.Add(new OleDbParameter("Cat2", destination.Name));
			cmd.Parameters.Add(new OleDbParameter("Cat1", source.Name));
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "DELETE FROM [Category] WHERE [Name] = @Cat1";
			cmd.Parameters.Clear();
			cmd.Parameters.Add(new OleDbParameter("Cat1", source.Name));
			affected += ExecuteNonQuery(cmd);

			CommitTransaction(cmd);

			if (affected > 0) {
				List<string> p = new List<string>();
				p.AddRange(source.Pages);
				p.AddRange(destination.Pages);
				CategoryInfo ci = new CategoryInfo(destination.Name, this);
				ci.Pages = p.ToArray();
				return ci;
			}
			else return null;
		}

		public PageContent GetContent(PageInfo page) {
#if DEBUG
			host.LogEntry("GetContent", LogEntryType.General, this);
#endif
			return GetBackupContent(page, -1);
		}

		public List<int> GetBackups(PageInfo page) {
#if DEBUG
			host.LogEntry("GetBackups", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT [Revision] FROM [PageContent] WHERE [Page] = @Page AND NOT [Revision] = -1 ORDER BY [Revision]";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			OleDbDataReader reader = ExecuteReader(cmd);
			List<int> result = new List<int>();
			while(reader != null && reader.Read()) {
				result.Add(reader.GetInt32(0));
			}
			Close(cmd);
			return result;
		}

		public PageContent GetBackupContent(PageInfo page, int revision) {
#if DEBUG
			host.LogEntry("GetBackupContent", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [PageContent] WHERE [Page] = @Page AND [Revision] = @Revision";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("Revision", revision));
			OleDbDataReader reader = ExecuteReader(cmd);
			PageContent content = null;
			if(reader != null && reader.Read()) {
				host.LogEntry("GetBackupContent#GetString(2):" + reader.GetString(2), LogEntryType.General, this);
				content = new PageContent(page, reader.GetString(2), reader.GetString(4), reader.GetDateTime(3), reader.GetString(6), reader.GetString(5));
			}
			Close(cmd);
			return content;
		}

		public bool SetBackupContent(PageContent content, int revision) {
#if DEBUG
			host.LogEntry("SetBackupContent", LogEntryType.General, this);
#endif
			OleDbCommand cmd;
			if(GetBackupContent(content.PageInfo, revision) == null) {
				cmd = GetCommand();
				// Insert a fake revision and update it
				cmd.CommandText = "INSERT INTO [PageContent] ([Page], [Revision], [Title], [DateTime], [Username], [Content], [Comment]) VALUES (@Page, @Revision, '-', @DateTime, '-', '-', '-')";
				cmd.Parameters.Add(new OleDbParameter("Page", content.PageInfo.Name));
				cmd.Parameters.Add(new OleDbParameter("Revision", revision));
				cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, DateTime.Now));
				if(ExecuteNonQuery(cmd) != 1) return false;
			}
			cmd = GetCommand();
			host.LogEntry("SetBackupContent#Title:" + content.Title + "#" + revision.ToString(), LogEntryType.General, this);
			cmd.CommandText = "UPDATE [PageContent] SET [Title] = @Title, [DateTime] = @DateTime, [Username] = @Username, [Comment] = @Comment, [Content] = @Content WHERE [Page] = @Page AND [Revision] = @Revision";
			cmd.Parameters.Add(new OleDbParameter("Title", content.Title));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, content.LastModified));
			cmd.Parameters.Add(new OleDbParameter("Username", content.User));
			cmd.Parameters.Add(new OleDbParameter("Comment", content.Comment));
			cmd.Parameters.Add(new OleDbParameter("Content", content.Content));
			cmd.Parameters.Add(new OleDbParameter("Page", content.PageInfo.Name));
			cmd.Parameters.Add(new OleDbParameter("Revision", revision));

			return ExecuteNonQuery(cmd) == 1;
		}

		public bool Backup(PageInfo page) {
#if DEBUG
			host.LogEntry("Backup", LogEntryType.General, this);
#endif
			List<int> b = GetBackups(page);
			int revision = b.Count > 0 ? b[b.Count - 1] + 1 : 0;
			PageContent content = GetContent(page);
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "INSERT INTO [PageContent] ([Page], [Revision], [Title], [DateTime], [Username], [Content], [Comment]) VALUES (@Page, @Revision, @Title, @DateTime, @Username, @Content, @Comment)";
			host.LogEntry("Backup#Title:" + content.Title + "#" + revision.ToString(), LogEntryType.General, this);
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("Revision", revision));
			cmd.Parameters.Add(new OleDbParameter("Title", content.Title));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, content.LastModified));
			cmd.Parameters.Add(new OleDbParameter("Username", content.User));
			cmd.Parameters.Add(new OleDbParameter("Content", content.Content));
			cmd.Parameters.Add(new OleDbParameter("Comment", content.Comment));
			return ExecuteNonQuery(cmd) == 1;
		}

		public PageInfo AddPage(string name, DateTime creationDateTime) {
#if DEBUG
			host.LogEntry("AddPage", LogEntryType.General, this);
#endif
			int affected;
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "INSERT INTO [Page] ([Name], [Status], [CreationDateTime]) VALUES (@Name, 'N', @DateTime);";
			cmd.Parameters.Add(new OleDbParameter("Name", name));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, creationDateTime));
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "INSERT INTO [PageContent] ([Page],[Revision], [Title], [DateTime], [Username], [Content], [Comment]) VALUES (@Name, -1, '-', @DateTime, '-', '-', '-')";
			affected += ExecuteNonQuery(cmd);

			CommitTransaction(cmd);

			if (affected == 2) {
				return new PageInfo(name, this, PageStatus.Normal, creationDateTime);
			}
			else return null;
		}

		public PageInfo RenamePage(PageInfo page, string newName) {
#if DEBUG
			host.LogEntry("RenamePage", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [Page] SET [Name] = @NewPage WHERE [Name] = @Page";
			cmd.Parameters.Add(new OleDbParameter("NewPage", newName));
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			if(ExecuteNonQuery(cmd) == 1) {
				return new PageInfo(newName, this, page.Status, page.CreationDateTime);
			}
			else return null;
		}

		public PageInfo SetStatus(PageInfo page, PageStatus status) {
#if DEBUG
			host.LogEntry("SetStatus", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [Page] SET [Status] = @Status WHERE [Name] = @Page";
			string s = "N";
			switch(status) {
				case PageStatus.Normal:
					s = "N";
					break;
				case PageStatus.Locked:
					s = "L";
					break;
				case PageStatus.Public:
					s = "P";
					break;
			}
			cmd.Parameters.Add(new OleDbParameter("Status", s));
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			if(ExecuteNonQuery(cmd) == 1) {
				return new PageInfo(page.Name, this, status, page.CreationDateTime);
			}
			else return null;
		}

		public bool ModifyPage(PageInfo page, string title, string username, DateTime dateTime, string comment, string content, bool backup) {
#if DEBUG
			host.LogEntry("ModifyPage", LogEntryType.General, this);
#endif
			if (backup) {
				if(!Backup(page)) return false;
			}

			PageContent c = new PageContent(page, title, username, dateTime, comment, content);
			return SetBackupContent(c, -1);
		}

		public bool RollbackPage(PageInfo page, int revision) {
#if DEBUG
			host.LogEntry("RollbackPage", LogEntryType.General, this);
#endif
			// Delete newer backups, update current revision
			int affected;
			PageContent content = GetBackupContent(page, revision);
			if (content == null)
				return false;
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "UPDATE [PageContent] SET [Title] = @Title, [DateTime] = @DateTime, [Username] = @Username, [Content] = @Content, [Comment] = @Comment WHERE [Page] = @Page AND [Revision] = -1;";
			cmd.Parameters.Add(new OleDbParameter("Title", content.Title));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, content.LastModified));
			cmd.Parameters.Add(new OleDbParameter("Username", content.User));
			cmd.Parameters.Add(new OleDbParameter("Content", content.Content));
			cmd.Parameters.Add(new OleDbParameter("Comment", content.Comment));
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "DELETE FROM [PageContent] WHERE [Page] = @Page AND NOT [Revision] = -1 AND [Revision] >= @Revision";
			cmd.Parameters.Clear();
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("Revision", revision));
			affected += ExecuteNonQuery(cmd);

			CommitTransaction(cmd);

			return affected > 1;
		}

		public bool DeleteBackups(PageInfo page, int revision) {
#if DEBUG
			host.LogEntry("DeleteBackups", LogEntryType.General, this);
#endif
			// Delete older backups, re-number remaining backups
			int affected;
			if (revision == -1) {
				List<int> backups = GetBackups(page);
				if(backups.Count > 0) revision = backups[backups.Count - 1];
			}
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "DELETE FROM [PageContent] WHERE [Page] = @Page AND NOT [Revision] = -1 AND [Revision] <= @Revision;";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("Revision", revision));
			affected += ExecuteNonQuery(cmd);

			cmd.CommandText = "UPDATE [PageContent] SET [Revision] = [Revision] - @Revision - 1 WHERE [Page] = @Page AND NOT [Revision] = -1";
			cmd.Parameters.Clear();
			cmd.Parameters.Add(new OleDbParameter("Revision", revision));
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			affected += ExecuteNonQuery(cmd);

			CommitTransaction(cmd);

			return affected > 0;
		}

		public bool RemovePage(PageInfo page) {
#if DEBUG
			host.LogEntry("RemovePage", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "DELETE FROM [Page] WHERE [Name] = @Page";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			return ExecuteNonQuery(cmd) == 1;
		}

		public bool Rebind(PageInfo page, string[] categories) {
#if DEBUG
			host.LogEntry("Rebind", LogEntryType.General, this);
#endif
			// Delete old bindings, add new bindings
			int affected;
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "DELETE FROM [CategoryBinding] WHERE [Page] = @Page; ";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			affected += ExecuteNonQuery(cmd);
			for(int i = 0; i < categories.Length; i++) {
				cmd.CommandText = "INSERT INTO [CategoryBinding] ([Category], [Page]) VALUES (@Cat, @Page); ";
				cmd.Parameters.Clear();
				cmd.Parameters.Add(new OleDbParameter("Cat", categories[i]));
				cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
				affected += ExecuteNonQuery(cmd);
			}
			
			CommitTransaction(cmd);

			return true;
		}

		public Message[] GetMessages(PageInfo page) {
#if DEBUG
			host.LogEntry("GetMessages", LogEntryType.General, this);
#endif
			// This method is implemented in a very raw way
			return GetReplies(page, -1).ToArray();
		}

		private List<Message> GetReplies(PageInfo page, int parent) {
#if DEBUG
			host.LogEntry("GetReplies", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT * FROM [Message] WHERE [Page] = @Page AND [Parent] = @Parent";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("Parent", parent));
			OleDbDataReader reader = ExecuteReader(cmd);
			List<Message> result = new List<Message>();
			while(reader != null && reader.Read()) {
				Message msg = new Message(reader.GetInt32(0), reader.GetString(3), reader.GetString(5), reader.GetDateTime(4), reader.GetString(6));
				// Too many connections with recursion?
				msg.Replies = GetReplies(page, msg.ID);
				result.Add(msg);
			}
			Close(cmd);
			return result;
		}

		public int GetMessageCount(PageInfo page) {
#if DEBUG
			host.LogEntry("GetMessageCount", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT COUNT(*) FROM [Message] WHERE [Page] = @Page";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			return (int)ExecuteScalar(cmd);
		}

		public bool AddMessage(PageInfo page, string username, string subject, DateTime dateTime, string body, int parent) {
#if DEBUG
			host.LogEntry("AddMessage", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "INSERT INTO [Message] ([Page], [Parent], [Username], [DateTime], [Subject], [Body]) VALUES (@Page, @Parent, @Username, @DateTime, @Subject, @Body)";
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("Parent", parent));
			cmd.Parameters.Add(new OleDbParameter("Username", username));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, dateTime));
			cmd.Parameters.Add(new OleDbParameter("Subject", subject));
			cmd.Parameters.Add(new OleDbParameter("Body", body));
			return ExecuteNonQuery(cmd) == 1;
		}

		public bool RemoveMessage(PageInfo page, int id, bool removeReplies) {
#if DEBUG
			host.LogEntry("RemoveMessage", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			if(removeReplies) {
				// Recursively remove all the replies
				cmd.CommandText = "SELECT [ID] FROM [Message] WHERE [Page] = @Page AND [Parent] = @ID";
				cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
				cmd.Parameters.Add(new OleDbParameter("ID", id));
				OleDbDataReader reader = ExecuteReader(cmd);
				while(reader != null && reader.Read()) {
					RemoveMessage(page, reader.GetInt32(0), true);
				}
				Close(cmd);
				// Delete the message
				cmd = GetCommand();
				cmd.CommandText = "DELETE FROM [Message] WHERE [Page] = @Page AND [ID] = @ID";
				cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
				cmd.Parameters.Add(new OleDbParameter("ID", id));
				return ExecuteNonQuery(cmd) == 1;
			}
			else {
				// Find parent
				cmd.CommandText = "SELECT [Parent] FROM [Message] WHERE [Page] = @Page AND [ID] = @ID";
				cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
				cmd.Parameters.Add(new OleDbParameter("ID", id));
				int pid = (int)ExecuteScalar(cmd); // Can be -1
				// Set new parent, delete message
				int affected;
				cmd = GetCommand();

				affected = 0;
				BeginTransaction(cmd);

				cmd.CommandText = "UPDATE [Message] SET [Parent] = @Parent WHERE [Page] = @Page AND [Parent] = @ID;";
				cmd.Parameters.Add(new OleDbParameter("Parent", pid));
				cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
				cmd.Parameters.Add(new OleDbParameter("ID", id));
				affected += ExecuteNonQuery(cmd);
				
				cmd.CommandText = "DELETE FROM [Message] WHERE [Page] = @Page AND [ID] = @ID";
				cmd.Parameters.Clear();
				cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
				cmd.Parameters.Add(new OleDbParameter("ID", id));
				affected += ExecuteNonQuery(cmd);
				
				CommitTransaction(cmd);

				return affected > 0;
			}
		}

		public bool ModifyMessage(PageInfo page, int id, string username, string subject, DateTime dateTime, string body) {
#if DEBUG
			host.LogEntry("ModifyMessage", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [Message] SET [Username] = @Username, [Subject] = @Subject, [DateTime] = @DateTime, [Body] = @Body WHERE [Page] = @Page AND [ID] = @ID";
			cmd.Parameters.Add(new OleDbParameter("Username", username));
			cmd.Parameters.Add(new OleDbParameter("Subject", subject));
			cmd.Parameters.Add(GetParameter("DateTime", OleDbType.Date, dateTime));
			cmd.Parameters.Add(new OleDbParameter("Body", body));
			cmd.Parameters.Add(new OleDbParameter("Page", page.Name));
			cmd.Parameters.Add(new OleDbParameter("ID", id));
			return ExecuteNonQuery(cmd) == 1;
		}

		public NavigationPath[] AllNavigationPaths {
			get {
#if DEBUG
				host.LogEntry("AllNavigationPaths", LogEntryType.General, this);
#endif
				OleDbCommand cmd = GetCommand();
				cmd.CommandText = "SELECT * FROM [NavigationPath]";
				OleDbDataReader reader = ExecuteReader(cmd);
				List<NavigationPath> result = new List<NavigationPath>();
				while(reader != null && reader.Read()) {
					NavigationPath n = new NavigationPath(reader.GetString(0), this);
					n.Pages.AddRange(GetPages(n.Name));
					result.Add(n);
				}
				Close(cmd);
				return result.ToArray();
			}
		}

		private List<string> GetPages(string path) {
#if DEBUG
			host.LogEntry("GetPages", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "SELECT [Page] FROM [NavigationPathBinding] WHERE [NavigationPath] = @Path ORDER BY [Number]";
			cmd.Parameters.Add(new OleDbParameter("Path", path));
			OleDbDataReader reader = ExecuteReader(cmd);
			List<string> result = new List<string>();
			while(reader != null && reader.Read()) {
				result.Add(reader.GetString(0));
			}
			Close(cmd);
			return result;
		}

		public NavigationPath AddNavigationPath(string name, PageInfo[] pages) {
#if DEBUG
			host.LogEntry("AddNavigationPath", LogEntryType.General, this);
#endif
			int affected;
			OleDbCommand cmd = GetCommand();

			affected = 0;
			BeginTransaction(cmd);

			cmd.CommandText = "INSERT INTO [NavigationPath] ([Name]) VALUES (@Path); ";
			cmd.Parameters.Add(new OleDbParameter("Path", name));
			affected += ExecuteNonQuery(cmd);
			string[] pgs = new string[pages.Length];
			for(int i = 0; i < pages.Length; i++) {
				cmd.CommandText = "INSERT INTO [NavigationPathBinding] ([NavigationPath], [Page], [Number]) VALUES (@Path, @Page, @Number); ";
				cmd.Parameters.Clear();
				cmd.Parameters.Add(new OleDbParameter("Path", name));
				cmd.Parameters.Add(new OleDbParameter("Page", pages[i].Name));
				cmd.Parameters.Add(new OleDbParameter("Number", i));
				affected += ExecuteNonQuery(cmd);
				pgs[i] = pages[i].Name;
			}
			
			CommitTransaction(cmd);

			if(affected == pages.Length + 1) {
				NavigationPath p = new NavigationPath(name, this);
				p.Pages.AddRange(pgs);
				return p;
			}
			else return null;
		}

		public NavigationPath ModifyNavigationPath(string name, PageInfo[] pages) {
#if DEBUG
			host.LogEntry("ModifyNavigationPath", LogEntryType.General, this);
#endif
			// Shortcut
			if(!RemoveNavigationPath(name)) return null;
			return AddNavigationPath(name, pages);
		}

		public bool RemoveNavigationPath(string name) {
#if DEBUG
			host.LogEntry("RemoveNavigationPath", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "DELETE FROM [NavigationPath] WHERE [Name] = @Path";
			cmd.Parameters.Add(new OleDbParameter("Path", name));
			return ExecuteNonQuery(cmd) == 1;
		}

		public Snippet[] AllSnippets {
			get {
#if DEBUG
				host.LogEntry("AllSnippets", LogEntryType.General, this);
#endif
				OleDbCommand cmd = GetCommand();
				cmd.CommandText = "SELECT * FROM [Snippet]";
				OleDbDataReader reader = ExecuteReader(cmd);
				List<Snippet> result = new List<Snippet>();
				while(reader != null && reader.Read()) {
					result.Add(new Snippet(reader.GetString(0), reader.GetString(1), this));
				}
				Close(cmd);
				return result.ToArray();
			}
		}

		public Snippet AddSnippet(string name, string content) {
#if DEBUG
			host.LogEntry("AddSnippet", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "INSERT INTO [Snippet] ([Name], [Content]) VALUES (@Name, @Content)";
			cmd.Parameters.Add(new OleDbParameter("Name", name));
			cmd.Parameters.Add(new OleDbParameter("Content", content));
			if(ExecuteNonQuery(cmd) == 1) {
				return new Snippet(name, content, this);
			}
			else return null;
		}

		public Snippet ModifySnippet(string name, string content) {
#if DEBUG
			host.LogEntry("ModifySnippet", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "UPDATE [Snippet] SET [Content] = @Content WHERE [Name] = @Name";
			cmd.Parameters.Add(new OleDbParameter("Content", content));
			cmd.Parameters.Add(new OleDbParameter("Name", name));
			if(ExecuteNonQuery(cmd) == 1) {
				return new Snippet(name, content, this);
			}
			else return null;
		}

		public bool RemoveSnippet(string name) {
#if DEBUG
			host.LogEntry("RemoveSnippet", LogEntryType.General, this);
#endif
			OleDbCommand cmd = GetCommand();
			cmd.CommandText = "DELETE FROM [Snippet] WHERE [Name] = @Name";
			cmd.Parameters.Add(new OleDbParameter("Name", name));
			return ExecuteNonQuery(cmd) == 1;
		}
	}

}
