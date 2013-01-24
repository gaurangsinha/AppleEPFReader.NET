AppleEPFReader.NET
==================

A simple class which reads the Apple EPF files and exposes it as a **DataReader** and **Enumerable**.

Implements `IDataReader` and `IEnumerable` completely.

##Usage##
Include `AppleEPFReader.cs` in your project

```csharp
using (var reader = new AppleEPFReader("PATH_TO_FILE")) {
  while (reader.Read()) {
    Console.WriteLine((string)reader["FIELD_NAME"]);
  }
}

```

##Apple Enterprise Partner Feed##
Read more about Apple's EPF at 
http://www.apple.com/itunes/affiliates/resources/documentation/itunes-enterprise-partner-feed.html
