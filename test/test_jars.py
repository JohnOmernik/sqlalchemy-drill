# This is a simple test to see if the JDBC driver is available for use
# Run "resolve.sh" prior to running this to install all of the required
# drill dependencies.
import jpype
import jpype.imports

jpype.startJVM('-ea', classpath='lib/*')

# First lets see if we have everything in the class path
print(jpype.java.lang.System.getProperty("java.class.path"))

# Second lets see if the driver is loadable
from org.apache.drill.jdbc import Driver

# All good to go
print("GOOD")
