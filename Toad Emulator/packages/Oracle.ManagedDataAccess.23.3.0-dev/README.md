<center><img src="https://www.oracle.com/a/ocom/img/dotnet.svg" alt="Oracle .NET logo" height="200" width="200"/></center>

# Oracle.ManagedDataAccess 23.3.0-dev README
Release Notes for Oracle Data Provider for .NET, Managed Driver NuGet Package

October 2023

Managed Oracle Data Provider for .NET (ODP.NET) features optimized ADO.NET data access to the Oracle database for .NET Framework and is 100% managed code. ODP.NET allows developers to take advantage of advanced Oracle database functionality, including Real Application Clusters, self-tuning statement cache, Application Continuity, JSON Relational Duality, and Fast Connection Failover. 

This document provides information that supplements the [Oracle Data Provider for .NET (ODP.NET) documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/odpnt/index.html).

## Oracle .NET Links
* [Oracle .NET Home Page](https://www.oracle.com/database/technologies/appdev/dotnet.html)
* [GitHub - Sample Code](https://github.com/oracle/dotnet-db-samples)
* [ODP.NET Discussion Forum](https://forums.oracle.com/ords/apexds/domain/dev-community/category/odp-dot-net)
* [YouTube](https://www.youtube.com/user/OracleDOTNETTeam)
* [X (Twitter)](https://twitter.com/oracledotnet)
* [Email Newsletter Sign Up](https://go.oracle.com/LP=28277?elqCampaignId=124071&nsl=onetdev)

## Bug Fixes since Oracle.ManagedDataAccess NuGet Package 23.2.0-dev
Bug 35778668 - PIPELINING HAS THREAD BLOCKING IN A RECV FOR A CONNECTION
Bug 35711258 - AQ ORACLEAQAGENT CONSTRUCTOR WITH NULL NAME PARAMETER TRIGGERS ERROR 
Bug 35698455 - ASYNC: ORA-50028 INVALID PARAMETER BINDING WHEN USING ARRAY BINDING 
Bug 35666844 - ORA-50000: CONNECTION REQUEST TIMED OUT USING ASYNC AND CONNECTION POOLING 

## Installation Changes
The following app/web.config entries are added when installing the managed ODP.NET NuGet package to your application:

1) Configuration Section Handler

A configuration section handler entry is added to the app/web.config to enable applications to add an <oracle.manageddataaccess.client> 
section for ODP.NET, Managed Driver-specific configuration.

Note: For a web app, if the same config section handler for "oracle.manageddataaccess.client" also exists in machine.config but the "Version" attribute values are different, an error message "There is a duplicate 'oracle.manageddataaccess.client' section defined." may be observed at runtime. To resolve the error, remove the "oracle.manageddataaccess.client" config section handler entry in the machine.config. If other applications on the machine depend on this machine.config entry, move the config section handler entry to each application's web.config file.

2) DbProviderFactories

The DbProviderFactories entry is added for applications that use DbProviderFactories and DbProviderFactory classes. Any DbProviderFactories entry for "Oracle.ManagedDataAccess.Client" in the machine.config will be ignored. 

3) Dependent Assembly

The dependent assembly entry is created to ignore policy DLLs for Oracle.ManagedDataAccess.dll. It directs the app to always use the Oracle.ManagedDataAccess.dll version that is specified by the "newVersion" attribute in the "bindingRedirect" element. The "newVersion" attribute corresponds to the Oracle.ManagedDataAccess.dll version which came with the NuGet package.

4) Data Sources

The data sources entry is added to provide a template on how a data source can be configured in the app/web.config. 
Simply rename the sample data source to an alias of your choosing; modify the PROTOCOL, HOST, PORT, SERVICE_NAME as required; 
and un-comment the "dataSource" element. Once that is done, the alias can be used as the "data source" attribute in 
your ODP.NET connection string.


 Copyright (c) 2023, Oracle and/or its affiliates.
