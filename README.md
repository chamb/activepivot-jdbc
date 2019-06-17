# sandbox-jdbc
ActivePivot sample project to demonstrate loading data from a relational database using JDBC.

To start the application, launch `com.activeviam.sandbox.ActivePivotServer` or alternatively, build the project with maven into a .war file, and deploy the .war file in a servlet container such as Apache Tomcat.

The project is autocontained and creates a transient h2 database when the application starts, loaded with sample data (a portfolio management data model with trades, products and risks). The initialization of database is done in `com.activeviam.sandbox.cfg.DataLoadingConfig#createDatabase()`. When using an existing database, this preparation step can be removed.

The data is loaded from the database into ActivePivot using the ActiveViam JDBC Source. The JDBC Source a Spring bean configured by `com.activeviam.sandbox.cfg.DataLoadingConfig#jdbcSource()` and the ActivePivot loading is implemented in `com.activeviam.sandbox.cfg.DataLoadingConfig#loadData()`.
