using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using System.IO;

/// <summary>
/// Apple EPF Reader
/// </summary>
public class AppleEPFReader : IDataReader, IEnumerable<string[]> {

	#region Constants
	/// <summary>
	/// Field Separator
	/// </summary>
	private static readonly char FIELD_SEPARATOR = (char)1;

	/// <summary>
	/// Record Separator
	/// </summary>
	private static readonly char RECORD_SEPARATOR = (char)2;

	/// <summary>
	/// Comment Initializer
	/// </summary>
	private static readonly char COMMENT_INITIALIZER = '#';

	/// <summary>
	/// Row name separator
	/// </summary>
	private static readonly char COMMENT_ROW_NAME_SEPARATOR = ':';
	#endregion

	/// <summary>
	/// DataReader status
	/// </summary>
	private bool _IsClosed = true;

	/// <summary>
	/// Table Schema
	/// </summary>
	private DataTable _SCHEMA;

	/// <summary>
	/// DB Column Types
	/// </summary>
	private string[] _TYPES;

	/// <summary>
	/// Foreign Keys
	/// </summary>
	private string[] _KEYS;

	/// <summary>
	/// Table Column Names
	/// </summary>
	private string[] _COLUMNS;

	/// <summary>
	/// Table Column Name Index
	/// </summary>
	private Dictionary<string, int> _COLUMN_INDEX;

	/// <summary>
	/// Stream reader
	/// </summary>
	private StreamReader _FILE_READER;

	/// <summary>
	/// Current row data
	/// </summary>
	private string[] _CURRENT_ROW;

	/// <summary>
	/// Gets or sets the name.
	/// </summary>
	/// <value>The name.</value>
	public string Name { get; private set; }

	/// <summary>
	/// Gets the columns.
	/// </summary>
	/// <value>The columns.</value>
	public string[] Columns {
		get { 
			return _COLUMNS; 
		}
	}        

	/// <summary>
	/// Gets or sets the export mode.
	/// </summary>
	/// <value>The export mode.</value>
	public AppleEPFExportMode ExportMode { get; private set; }

	#region Constructor
	/// <summary>
	/// Initializes a new instance of the <see cref="AppleEPFReader"/> class.
	/// </summary>
	/// <param name="filePath">Name of the file.</param>
	public AppleEPFReader(string filePath) {
		//Open file
		_FILE_READER = new StreamReader(filePath);

		//File name
		this.Name = Path.GetFileName(filePath);

		//Read scehma data
		//Read column names
		_COLUMNS = _FILE_READER.ReadLine()
							.Trim(COMMENT_INITIALIZER)
							.Split(FIELD_SEPARATOR);
		_COLUMNS[_COLUMNS.Length - 1] = _COLUMNS[_COLUMNS.Length - 1].Trim(RECORD_SEPARATOR);
		//Convert to a Dictionary with the column name as the key and index as the value
		_COLUMN_INDEX = _COLUMNS.ToDictionary(s => s, s => Array.IndexOf(_COLUMNS, s));

		//Read keys
		_KEYS = _FILE_READER.ReadLine()
							.Split(COMMENT_ROW_NAME_SEPARATOR)[1]
							.Split(FIELD_SEPARATOR);
		_KEYS[_KEYS.Length - 1] = _KEYS[_KEYS.Length - 1].Trim(RECORD_SEPARATOR);
		
		//Read column data types
		_TYPES = _FILE_READER.ReadLine()
							.Split(COMMENT_ROW_NAME_SEPARATOR)[1]
							.Split(FIELD_SEPARATOR);
		_TYPES[_TYPES.Length - 1] = _TYPES[_TYPES.Length - 1].Trim(RECORD_SEPARATOR);
		
		//Read export method full/incremental
		string exportMode = _FILE_READER.ReadLine()
							.Split(COMMENT_ROW_NAME_SEPARATOR)[1]
							.Trim(COMMENT_INITIALIZER, FIELD_SEPARATOR, RECORD_SEPARATOR);
		this.ExportMode = (AppleEPFExportMode)Enum.Parse(typeof(AppleEPFExportMode), exportMode, true);

		//create schema table
		_SCHEMA = new DataTable();
		_SCHEMA.Columns.Add("ColumnName");
		_SCHEMA.Columns.Add("DataType");
		_SCHEMA.Columns.Add("IsKey");
		for (int i = 0; i < _COLUMNS.Length; i++) {
			DataRow row = _SCHEMA.NewRow();
			row["ColumnName"] = _COLUMNS[i];
			row["DataType"] = _TYPES[i];
			row["IsKey"] = _KEYS.Contains(_COLUMNS[i]);
			_SCHEMA.Rows.Add(row);
		}

		this._IsClosed = false;
	}
	#endregion

	#region DataReader Implementation
	/// <summary>
	/// Closes the <see cref="T:System.Data.IDataReader"/> Object.
	/// </summary>
	public void Close() {
		if (null != _FILE_READER) {
			_FILE_READER.Close();
			this._IsClosed = true;
		}
	}

	/// <summary>
	/// Gets a value indicating the depth of nesting for the current row.
	/// </summary>
	/// <value></value>
	/// <returns>The level of nesting.</returns>
	public int Depth {
		get { return 0; }
	}

	/// <summary>
	/// Returns a <see cref="T:System.Data.DataTable"/> that describes the column metadata of the <see cref="T:System.Data.IDataReader"/>.
	/// </summary>
	/// <returns>
	/// A <see cref="T:System.Data.DataTable"/> that describes the column metadata.
	/// </returns>
	/// <exception cref="T:System.InvalidOperationException">The <see cref="T:System.Data.IDataReader"/> is closed. </exception>
	public DataTable GetSchemaTable() {
		return _SCHEMA;
	}
	
	/// <summary>
	/// Gets a value indicating whether the data reader is closed.
	/// </summary>
	/// <value></value>
	/// <returns>true if the data reader is closed; otherwise, false.</returns>
	public bool IsClosed {
		get {
			return _IsClosed;
		}
	}

	/// <summary>
	/// Advances the data reader to the next result, when reading the results of batch SQL statements.
	/// </summary>
	/// <returns>
	/// true if there are more rows; otherwise, false.
	/// </returns>
	public bool NextResult() {
		return false;
	}

	/// <summary>
	/// Advances the <see cref="T:System.Data.IDataReader"/> to the next record.
	/// </summary>
	/// <returns>
	/// true if there are more rows; otherwise, false.
	/// </returns>
	public bool Read() {
		if (null == _FILE_READER)
			return false;

		if (null != _FILE_READER && _FILE_READER.EndOfStream)
			return false;

		string data;
		do {
			data = FetchRecord(_FILE_READER, RECORD_SEPARATOR);
		}
		while (!_FILE_READER.EndOfStream && data[0] == COMMENT_INITIALIZER);

		_CURRENT_ROW = (!_FILE_READER.EndOfStream) ? data.ToString().Split(FIELD_SEPARATOR) : null;

		return null != _CURRENT_ROW;
	}

	/// <summary>
	/// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
	/// </summary>
	/// <value></value>
	/// <returns>The number of rows changed, inserted, or deleted; 0 if no rows were affected or the statement failed; and -1 for SELECT statements.</returns>
	public int RecordsAffected {
		get { 
			return 0; 
		}
	}

	/// <summary>
	/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
	/// </summary>
	public void Dispose() {
		if (null != _FILE_READER)
			_FILE_READER.Dispose();
		
		if (null != _SCHEMA)
			_SCHEMA.Dispose();

		_IsClosed = true;
	}

	/// <summary>
	/// Gets the number of columns in the current row.
	/// </summary>
	/// <value></value>
	/// <returns>When not positioned in a valid recordset, 0; otherwise, the number of columns in the current record. The default is -1.</returns>
	public int FieldCount {
		get {
			return this._COLUMNS.Length;
		}
	}

	/// <summary>
	/// Gets the value of the specified column as a Boolean.
	/// </summary>
	/// <param name="i">The zero-based column ordinal.</param>
	/// <returns>The value of the column.</returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public bool GetBoolean(int i) {
		return bool.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the 8-bit unsigned integer value of the specified column.
	/// </summary>
	/// <param name="i">The zero-based column ordinal.</param>
	/// <returns>
	/// The 8-bit unsigned integer value of the specified column.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public byte GetByte(int i) {
		return Convert.ToByte(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Reads a stream of bytes from the specified column offset into the buffer as an array, starting at the given buffer offset.
	/// </summary>
	/// <param name="i">The zero-based column ordinal.</param>
	/// <param name="fieldOffset">The index within the field from which to start the read operation.</param>
	/// <param name="buffer">The buffer into which to read the stream of bytes.</param>
	/// <param name="bufferoffset">The index for <paramref name="buffer"/> to start the read operation.</param>
	/// <param name="length">The number of bytes to read.</param>
	/// <returns>The actual number of bytes read.</returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
		throw new NotImplementedException();
	}

	/// <summary>
	/// Gets the character value of the specified column.
	/// </summary>
	/// <param name="i">The zero-based column ordinal.</param>
	/// <returns>
	/// The character value of the specified column.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public char GetChar(int i) {
		return Convert.ToChar(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Reads a stream of characters from the specified column offset into the buffer as an array, starting at the given buffer offset.
	/// </summary>
	/// <param name="i">The zero-based column ordinal.</param>
	/// <param name="fieldoffset">The index within the row from which to start the read operation.</param>
	/// <param name="buffer">The buffer into which to read the stream of bytes.</param>
	/// <param name="bufferoffset">The index for <paramref name="buffer"/> to start the read operation.</param>
	/// <param name="length">The number of bytes to read.</param>
	/// <returns>The actual number of characters read.</returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
		throw new NotImplementedException();
	}

	/// <summary>
	/// Returns an <see cref="T:System.Data.IDataReader"/> for the specified column ordinal.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// An <see cref="T:System.Data.IDataReader"/>.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public IDataReader GetData(int i) {
		return this;
	}

	/// <summary>
	/// Gets the data type information for the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The data type information for the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public string GetDataTypeName(int i) {
		return _TYPES[i];
	}

	/// <summary>
	/// Gets the date and time data value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The date and time data value of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public DateTime GetDateTime(int i) {
		return DateTime.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the fixed-position numeric value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The fixed-position numeric value of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public decimal GetDecimal(int i) {
		return decimal.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the double-precision floating point number of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The double-precision floating point number of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public double GetDouble(int i) {
		return double.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the <see cref="T:System.Type"/> information corresponding to the type of <see cref="T:System.Object"/> that would be returned from <see cref="M:System.Data.IDataRecord.GetValue(System.Int32)"/>.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The <see cref="T:System.Type"/> information corresponding to the type of <see cref="T:System.Object"/> that would be returned from <see cref="M:System.Data.IDataRecord.GetValue(System.Int32)"/>.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	Type IDataRecord.GetFieldType(int i) {
		throw new NotImplementedException();
	}

	/// <summary>
	/// Gets the single-precision floating point number of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The single-precision floating point number of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public float GetFloat(int i) {
		return float.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Returns the GUID value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>The GUID value of the specified field.</returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public Guid GetGuid(int i) {
		return Guid.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the 16-bit signed integer value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The 16-bit signed integer value of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public short GetInt16(int i) {
		return short.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the 32-bit signed integer value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The 32-bit signed integer value of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public int GetInt32(int i) {
		return int.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the 64-bit signed integer value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The 64-bit signed integer value of the specified field.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public long GetInt64(int i) {
		return long.Parse(_CURRENT_ROW[i]);
	}

	/// <summary>
	/// Gets the name for the field to find.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The name of the field or the empty string (""), if there is no value to return.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public string GetName(int i) {
		return _COLUMNS[i];
	}

	/// <summary>
	/// Return the index of the named field.
	/// </summary>
	/// <param name="name">The name of the field to find.</param>
	/// <returns>The index of the named field.</returns>
	public int GetOrdinal(string name) {
		return _COLUMN_INDEX[name];
	}

	/// <summary>
	/// Gets the string value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>The string value of the specified field.</returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public string GetString(int i) {
		return _CURRENT_ROW[i];
	}

	/// <summary>
	/// Return the value of the specified field.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// The <see cref="T:System.Object"/> which will contain the field value upon return.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public object GetValue(int i) {
		return _CURRENT_ROW[i];
	}

	/// <summary>
	/// Populates an array of objects with the column values of the current record.
	/// </summary>
	/// <param name="values">An array of <see cref="T:System.Object"/> to copy the attribute fields into.</param>
	/// <returns>
	/// The number of instances of <see cref="T:System.Object"/> in the array.
	/// </returns>
	public int GetValues(object[] values) {
		Array.Copy(_CURRENT_ROW, values, values.Length);
		return values.Length;
	}

	/// <summary>
	/// Return whether the specified field is set to null.
	/// </summary>
	/// <param name="i">The index of the field to find.</param>
	/// <returns>
	/// true if the specified field is set to null; otherwise, false.
	/// </returns>
	/// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
	public bool IsDBNull(int i) {
		return null == _CURRENT_ROW[i];
	}

	/// <summary>
	/// Gets the <see cref="System.Object"/> with the specified name.
	/// </summary>
	/// <value></value>
	public object this[string name] {
		get {
			return this[_COLUMN_INDEX[name]];
		}
	}

	/// <summary>
	/// Gets the <see cref="System.Object"/> with the specified i.
	/// </summary>
	/// <value></value>
	public object this[int i] {
		get {
			return _CURRENT_ROW[i];
		}
	}
	#endregion

	#region IEnumerable Implementation
	/// <summary>
	/// Gets the enumerator.
	/// </summary>
	/// <returns></returns>
	public IEnumerator<string[]> GetEnumerator() {
		while (!this.IsClosed && this.Read()) {
			string[] values = new string[this.FieldCount];
			this.GetValues(values);
			yield return values;
		}
	}

	/// <summary>
	/// Returns an enumerator that iterates through a collection.
	/// </summary>
	/// <returns>
	/// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
	/// </returns>
	IEnumerator IEnumerable.GetEnumerator() {
		return GetEnumerator();
	}
	#endregion

	#region Private Methods

	/// <summary>
	/// Fetches the record.
	/// </summary>
	/// <param name="strRd">The STR rd.</param>
	/// <param name="recordSeparator">The record separator.</param>
	/// <returns></returns>
	private string FetchRecord(StreamReader strRd, char recordSeparator) {
		StringBuilder data = new StringBuilder();
		do {
			data.Clear();
			data.AppendLine(strRd.ReadLine());
		} while (!strRd.EndOfStream && recordSeparator != data[data.Length - 1]);
		Trim(data, recordSeparator);
		return data.ToString();
	}

	/// <summary>
	/// Trims the specified StringBuilder.
	/// </summary>
	/// <param name="sb">The sb.</param>
	private void Trim(StringBuilder sb, char trimChar) {
		while (trimChar == sb[sb.Length - 1]) 
			sb.Length -= 1;
	}


	#endregion
}
