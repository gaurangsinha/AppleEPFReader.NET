AppleEPFReader.NET
==================

A simple class which reads the Apple EPF files and exposes it as a **DataReader** and **Enumerable**.

Implements [`IDataReader`](http://msdn.microsoft.com/en-us/library/system.data.idatareader.aspx) and [`IEnumerable`](http://msdn.microsoft.com/en-us/library/9eekhta0.aspx) completely.

##Usage##
Include `AppleEPFReader.cs` in your project.

*OR*

Build the library and include that in your project.

```csharp
using (var reader = new AppleEPFReader("PATH_TO_FILE")) { //Create new instance, open EPF file and initialize reader
  while (reader.Read()) {                                 //Read next record
    Console.WriteLine((string)reader["FIELD_NAME"]);      //Fetch 'FIELD_NAME' column and print to console
  }
}
```

##Build a library (dll)##
1. Open the visual studio command prompt.
2. Navigate to the directory where `AppleEPFReader.cs` is stored.
3. Compile using the following command `csc /target:library AppleEPFReader.cs`
4. You will find a `AppleEPFReader.dll` created in the directory which can now be included in your project.

##Apple Enterprise Partner Feed##
Read more about Apple's EPF at 
http://www.apple.com/itunes/affiliates/resources/documentation/itunes-enterprise-partner-feed.html
